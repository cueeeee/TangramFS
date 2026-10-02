#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include "utlist.h"
#include "tangramfs-ucx-delegator.h"
#include "tangramfs-ucx-taskmgr.h"

#define NUM_THREADS         2
#define NUM_OUTGOING_RPC    4

// BUGFIX (Bug 2): upper bound on how long a delegator waits for ANOTHER
// delegator during split-lock negotiation (and on how long it keeps retrying
// a send to it). Talking to the global server keeps waiting indefinitely --
// the server is assumed to always be up -- but a peer delegator can
// legitimately be shutting down or already gone.
#define DELEGATOR_NEGOTIATION_TIMEOUT_MS 3000

// BUGFIX (Bug 2): outgoing RPCs are matched to their replies through
// seq_id = (generation << SEQ_SLOT_BITS) | slot. The peer echoes seq_id back
// unchanged, so after a timeout the slot can be reused immediately: a late
// reply to the abandoned request carries an old generation and is dropped,
// instead of being delivered to whichever call owns the slot now.
#define SEQ_SLOT_BITS       8
#define SEQ_SLOT_MASK       ((1ULL << SEQ_SLOT_BITS) - 1)

typedef struct rpc_respond {
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
    volatile bool   in_use;
    volatile bool   flag;
    uint64_t        gen;
    void**          ptr;
} rpc_respond_t;
static rpc_respond_t* g_responds;


static tfs_info_t*           g_tfs_info;
static taskmgr_t             g_taskmgr;

// BUGFIX (Bug 1 / shutdown hang): stopping is now split into separate steps.
//   g_stop_requested  -- set when the node-local client sends STOP
//   g_accepting       -- whether incoming requests are still queued for the workers
//   g_progress_running-- keeps the progress thread alive
// Originally a single flag, set by STOP, ended the progress loop right away.
// A worker that was still handling a peer's split-lock request (and waiting
// for the server's reply) could then never receive that reply, so it blocked
// forever, taskmgr_finalize() blocked forever joining it, and the peer that
// sent the split request waited forever too. The progress thread now keeps
// running until every worker has finished, and is joined before teardown.
static volatile bool         g_stop_requested   = false;
static volatile bool         g_progress_running = true;
static bool                  g_accepting        = true;
static pthread_mutex_t       g_accept_mutex     = PTHREAD_MUTEX_INITIALIZER;
static pthread_t             g_progress_thread;
static ucs_async_context_t*  g_delegator_async;


// intra-node communication with node-local clients, use share memory
static tangram_uct_context_t g_delegator_intra_context;

// inter-node communication with server and other delegators, use network
static tangram_uct_context_t g_delegator_inter_context;

static uct_ep_h g_ep_server;


void* (*delegator_am_handler)(uint8_t, tangram_uct_addr_t* client, void* data, uint8_t* respond_id, size_t *respond_len);

// All request listeners go through here. Once shutdown has begun the task
// manager is about to be torn down, so new requests are dropped instead of
// being appended to it (appending after taskmgr_finalize() would be a
// use-after-free). A peer whose split request is dropped this way times out
// and re-asks the server, which by then has already been told that we no
// longer own any locks (see tangram_delegator_flush_to_server()).
static void enqueue_task(uint8_t id, void* buf, size_t buf_len, int tid) {
    pthread_mutex_lock(&g_accept_mutex);
    if(g_accepting)
        taskmgr_append_task_to_worker(&g_taskmgr, id, buf, buf_len, tid);
    pthread_mutex_unlock(&g_accept_mutex);
}

static ucs_status_t am_acquire_lock_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    enqueue_task(AM_ID_ACQUIRE_LOCK_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_release_lock_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    enqueue_task(AM_ID_RELEASE_LOCK_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_release_lock_file_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    enqueue_task(AM_ID_RELEASE_LOCK_FILE_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_release_lock_client_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    enqueue_task(AM_ID_RELEASE_LOCK_CLIENT_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_split_lock_request_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    enqueue_task(AM_ID_SPLIT_LOCK_REQUEST, buf, buf_len, 1);
    return UCS_OK;
}
static ucs_status_t am_respond_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    uint64_t seq_id;
    void*    ptr = NULL;
    unpack_rpc_buffer(buf, buf_len, &seq_id, TANGRAM_UCT_ADDR_IGNORE, &ptr);

    uint64_t slot = seq_id & SEQ_SLOT_MASK;
    uint64_t gen  = seq_id >> SEQ_SLOT_BITS;
    if(slot >= NUM_OUTGOING_RPC) {
        free(ptr);
        return UCS_OK;
    }

    pthread_mutex_lock(&g_responds[slot].mutex);
    if(g_responds[slot].in_use && g_responds[slot].gen == gen && !g_responds[slot].flag) {
        g_responds[slot].flag   = true;
        *(g_responds[slot].ptr) = ptr;
        pthread_cond_signal(&g_responds[slot].cond);
    } else {
        // Late reply to a request whose caller already gave up: nobody owns it.
        free(ptr);
    }
    pthread_mutex_unlock(&g_responds[slot].mutex);

    return UCS_OK;
}

static ucs_status_t am_stop_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    g_stop_requested = true;
    return UCS_OK;
}


// Handle RPC tasks
// Most tasks are from node-local clients
// Currently, only one task (SPLIT_LOCK_REQUEST) is
// requested from remote delegators.
void delegator_handle_task(task_t* task) {
    if(task->id == AM_ID_SPLIT_LOCK_REQUEST) {
        // BUGFIX (Bug 3): the requester is another node's delegator, which may
        // have exited by the time we reply. Original code asserted on a failed
        // connect/send here, aborting THIS node because of a problem on the
        // OTHER one. Now a reply that cannot be delivered is just dropped.
        tangram_uct_context_t* context = &g_delegator_inter_context;
        task->respond = (*delegator_am_handler)(task->id, &task->client, task->data, &task->id, &task->respond_len);

        uct_ep_h ep;
        pthread_mutex_lock(&context->mutex);
        ucs_status_t status = uct_ep_create_connect_try(context->iface, &task->client, &ep);
        pthread_mutex_unlock(&context->mutex);
        if(status == UCS_OK) {
            do_uct_am_short_lock_try(&context->mutex, ep, task->id, task->seq_id, &context->self_addr,
                                     task->respond, task->respond_len, DELEGATOR_NEGOTIATION_TIMEOUT_MS);
            pthread_mutex_lock(&context->mutex);
            uct_ep_destroy(ep);
            pthread_mutex_unlock(&context->mutex);
        }
        free(task->respond);
        task->respond = NULL;
        return;
    }

    tangram_uct_context_t* context = &g_delegator_intra_context;

    pthread_mutex_lock(&context->mutex);
    uct_ep_h ep;
    uct_ep_create_connect(context->iface, &task->client, &ep);
    pthread_mutex_unlock(&context->mutex);

    task->respond = (*delegator_am_handler)(task->id, &task->client, task->data, &task->id, &task->respond_len);

    do_uct_am_short_lock(&context->mutex, ep, task->id, task->seq_id, &context->self_addr, task->respond, task->respond_len);

    pthread_mutex_lock(&context->mutex);
    uct_ep_destroy(ep);
    pthread_mutex_unlock(&context->mutex);
}

// BUGFIX (Bug 2): timeout_ms <= 0 keeps the original behavior (send, then
// wait for the reply indefinitely; used for the server). timeout_ms > 0 bounds
// both the send and the wait (used for peer delegators). Returns whether a
// reply was received. On false, *respond_ptr is not written.
static bool delegator_sendrecv_core(uint8_t id, tangram_uct_context_t* context, uct_ep_h ep, void* data, size_t length, void** respond_ptr, int timeout_ms) {
    int      slot   = 0;
    uint64_t seq_id = 0;
    if(respond_ptr != NULL) {
        int i = 0;
        while(true) {
            pthread_mutex_lock(&g_responds[i].mutex);
            if(g_responds[i].in_use) {
                pthread_mutex_unlock(&g_responds[i].mutex);
                i = (i + 1) % NUM_OUTGOING_RPC;
            } else {
                slot = i;
                g_responds[i].in_use = true;
                g_responds[i].flag   = false;
                g_responds[i].gen++;
                g_responds[i].ptr    = respond_ptr;
                seq_id = (g_responds[i].gen << SEQ_SLOT_BITS) | (uint64_t)i;
                pthread_mutex_unlock(&g_responds[i].mutex);
                break;
            }
        }
    }

    if(timeout_ms <= 0) {
        do_uct_am_short_lock(&context->mutex, ep, id, seq_id, &context->self_addr, data, length);
    } else {
        ucs_status_t status = do_uct_am_short_lock_try(&context->mutex, ep, id, seq_id, &context->self_addr, data, length, timeout_ms);
        if(status != UCS_OK) {
            if(respond_ptr != NULL) {
                pthread_mutex_lock(&g_responds[slot].mutex);
                g_responds[slot].in_use = false;
                pthread_mutex_unlock(&g_responds[slot].mutex);
            }
            return false;
        }
    }

    if(respond_ptr == NULL)
        return true;

    bool got_reply = true;
    pthread_mutex_lock(&g_responds[slot].mutex);
    if(timeout_ms <= 0) {
        while(!g_responds[slot].flag)
            pthread_cond_wait(&g_responds[slot].cond, &g_responds[slot].mutex);
    } else {
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec  += timeout_ms / 1000;
        deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if(deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec  += 1;
            deadline.tv_nsec -= 1000000000L;
        }
        while(!g_responds[slot].flag) {
            if(pthread_cond_timedwait(&g_responds[slot].cond, &g_responds[slot].mutex, &deadline) == ETIMEDOUT) {
                got_reply = g_responds[slot].flag;
                break;
            }
        }
    }
    // Release the slot right away. A reply that shows up later for this
    // request carries the old generation and is dropped by am_respond_listener().
    g_responds[slot].in_use = false;
    pthread_mutex_unlock(&g_responds[slot].mutex);
    return got_reply;
}

void tangram_ucx_delegator_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr) {
    delegator_sendrecv_core(id, &g_delegator_inter_context, g_ep_server, data, length, respond_ptr, 0);
}

bool tangram_ucx_delegator_sendrecv_delegator(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    // BUGFIX (Bug 3): the peer may already be gone ("Connection refused").
    // Report failure to the caller instead of asserting.
    uct_ep_h ep;
    pthread_mutex_lock(&g_delegator_inter_context.mutex);
    ucs_status_t status = uct_ep_create_connect_try(g_delegator_inter_context.iface, dest, &ep);
    pthread_mutex_unlock(&g_delegator_inter_context.mutex);
    if(status != UCS_OK)
        return false;

    bool got_reply = delegator_sendrecv_core(id, &g_delegator_inter_context, ep, data, length, respond_ptr, DELEGATOR_NEGOTIATION_TIMEOUT_MS);

    pthread_mutex_lock(&g_delegator_inter_context.mutex);
    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_delegator_inter_context.mutex);
    return got_reply;
}

void tangram_ucx_delegator_init(tfs_info_t *tfs_info) {
    g_tfs_info = tfs_info;

    g_responds = malloc(sizeof(rpc_respond_t) * NUM_OUTGOING_RPC);
    for(int i = 0; i < NUM_OUTGOING_RPC; i++) {
        g_responds[i].in_use = false;
        g_responds[i].flag   = false;
        g_responds[i].gen    = 0;
        g_responds[i].ptr    = NULL;
        pthread_mutex_init(&g_responds[i].mutex, NULL);
        pthread_cond_init(&g_responds[i].cond, NULL);
    }


    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_delegator_async);

    tangram_uct_context_init(g_delegator_async, tfs_info, true, &g_delegator_intra_context);
    tangram_uct_context_init(g_delegator_async, tfs_info, false, &g_delegator_inter_context);

    uct_ep_create_connect(g_delegator_inter_context.iface, &g_delegator_inter_context.server_addr, &g_ep_server);

    // From node-local clients, use intra_context
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_ACQUIRE_LOCK_REQUEST, am_acquire_lock_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_RELEASE_LOCK_REQUEST, am_release_lock_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_RELEASE_LOCK_FILE_REQUEST, am_release_lock_file_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_RELEASE_LOCK_CLIENT_REQUEST, am_release_lock_client_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_STOP_REQUEST, am_stop_listener, NULL, 0);

    // From other delegators, use inter_context
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_SPLIT_LOCK_REQUEST, am_split_lock_request_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_SPLIT_LOCK_RESPOND, am_respond_listener, NULL, 0);

    // From server, respond to our acquire_lock and release_lock request
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_ACQUIRE_LOCK_RESPOND, am_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_RELEASE_LOCK_RESPOND, am_respond_listener, NULL, 0);
    // BUGFIX (Bug 4): reply to tangram_delegator_flush_to_server()
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_RELEASE_LOCK_CLIENT_RESPOND, am_respond_listener, NULL, 0);

    taskmgr_init(&g_taskmgr, NUM_THREADS, delegator_handle_task);
}

void tangram_ucx_delegator_register_rpc(void* (*user_handler)(uint8_t, tangram_uct_addr_t*, void*, uint8_t*, size_t*)) {
    delegator_am_handler = user_handler;
}

void* delegator_progress_loop(void* arg) {
    while(g_progress_running) {
        pthread_mutex_lock(&g_delegator_intra_context.mutex);
        uct_worker_progress(g_delegator_intra_context.worker);
        pthread_mutex_unlock(&g_delegator_intra_context.mutex);

        pthread_mutex_lock(&g_delegator_inter_context.mutex);
        uct_worker_progress(g_delegator_inter_context.worker);
        pthread_mutex_unlock(&g_delegator_inter_context.mutex);
    }

    return NULL;
}

void tangram_ucx_delegator_start() {
    // BUGFIX (Bug 1): keep the handle so stop() can join the thread
    // before destroying the UCX resources it is using.
    pthread_create(&g_progress_thread, NULL, delegator_progress_loop, NULL);
}

void tangram_ucx_delegator_stop() {

    // 1. Wait for the node-local client's STOP request.
    while(!g_stop_requested)
        usleep(1000);

    // 2. Stop queueing new requests.
    pthread_mutex_lock(&g_accept_mutex);
    g_accepting = false;
    pthread_mutex_unlock(&g_accept_mutex);

    // 3. Let the workers finish what they are doing. The progress thread is
    //    still running, so replies they are waiting for can still arrive.
    taskmgr_finalize(&g_taskmgr);

    // 4. Only now stop the progress thread, and wait for it to exit.
    g_progress_running = false;
    pthread_join(g_progress_thread, NULL);

    for(int i = 0; i < NUM_OUTGOING_RPC; i++) {
        pthread_cond_destroy(&g_responds[i].cond);
        pthread_mutex_destroy(&g_responds[i].mutex);
    }
    free(g_responds);


    uct_ep_destroy(g_ep_server);
    tangram_uct_context_destroy(&g_delegator_intra_context);
    tangram_uct_context_destroy(&g_delegator_inter_context);
    ucs_async_context_destroy(g_delegator_async);
}

tangram_uct_addr_t* tangram_ucx_delegator_intra_addr() {
    return &g_delegator_intra_context.self_addr;
}
tangram_uct_addr_t* tangram_ucx_delegator_inter_addr() {
    return &g_delegator_inter_context.self_addr;
}
