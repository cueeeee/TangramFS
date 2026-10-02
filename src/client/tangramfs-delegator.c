#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <mpi.h>
#include "tangramfs-delegator.h"
#include "tangramfs-ucx-delegator.h"
#include "tangramfs-lock-manager.h"

static lock_table_t *g_lt;

/**
 * Return a respond, can be NULL
 */
void* delegator_rpc_handler(uint8_t id, tangram_uct_addr_t* client, void* data, uint8_t* respond_id, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    char hostname[128];
    gethostname(hostname, 128);

    if(id == AM_ID_ACQUIRE_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_assert(in->num_intervals == 1);
        //tangram_debug("[tangramfs delegator %s] acquire lock start, filename: %s, ask [%ld-%ld]\n", hostname, in->filename, in->intervals[0].offset/LOCK_BLOCK_SIZE, (in->intervals[0].offset+in->intervals[0].count-1)/LOCK_BLOCK_SIZE);
        tangram_lockmgr_delegator_acquire_lock(&g_lt, client, in->filename, in->intervals[0].offset, in->intervals[0].count, in->intervals[0].type);
        tangram_debug("[tangramfs delegator %s] acquire lock done, filename: %s, ask [%ld-%ld]\n", hostname, in->filename, in->intervals[0].offset/LOCK_BLOCK_SIZE, (in->intervals[0].offset+in->intervals[0].count-1)/LOCK_BLOCK_SIZE);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_ACQUIRE_LOCK_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_assert(in->num_intervals == 1);
        tangram_debug("[tangramfs delegator] release lock, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        tangram_lockmgr_server_release_lock(g_lt, client, in->filename, in->intervals[0].offset, in->intervals[0].count);
        //tangram_debug("[tangramfs] release lock success, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_FILE_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_lockmgr_server_release_lock_file(g_lt, client, in->filename);
        tangram_debug("[tangramfs delegator] release lock file: %s\n", in->filename);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_FILE_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_CLIENT_REQUEST) {
        tangram_debug("[tangramfs delegator %s] release lock client.\n", hostname);
        tangram_lockmgr_server_release_lock_client(g_lt, client);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_CLIENT_RESPOND;
    } else if(id == AM_ID_SPLIT_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_assert(in->num_intervals == 1);
        tangram_debug("[tangramfs delegator %s] split lock start, filename: %s, offset:%lu, count: %lu\n", hostname, in->filename, in->intervals[0].offset, in->intervals[0].count);
        tangram_lockmgr_delegator_split_lock(&g_lt, in->filename, in->intervals[0].offset, in->intervals[0].count, in->intervals[0].type);
        tangram_debug("[tangramfs delegator %s] split lock done, filename: %s, offset:%lu, count: %lu\n", hostname, in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_SPLIT_LOCK_RESPOND;
    }

    return respond;
}

void tangram_delegator_start(tfs_info_t* tfs_info) {
    tangram_lockmgr_init(&g_lt);
    tangram_ucx_delegator_init(tfs_info);
    tangram_ucx_delegator_register_rpc(delegator_rpc_handler);

    // Enter the progress loop and exit when the
    // stop command is received
    tangram_ucx_delegator_start();
}

// BUGFIX (Bug 4 -- root cause of the 2-node failures): a delegator keeps
// (caches) the lock tokens the server granted it even after its local clients
// release them; it only gives a range up when another delegator asks it to
// split. Nothing ever handed those tokens back when the delegator exited, so
// the server kept naming a delegator that no longer existed as the owner, and
// every node that later needed one of those ranges was sent to negotiate with
// it. Depending on timing that ended in an assertion abort ("Connection
// refused"), in the exiting node hanging on shutdown, or in the server's lock
// worker spinning on a reply that could never be delivered, which froze lock
// service for every node.
//
// Called while the delegator is still fully running (before STOP): asks the
// server to drop every lock owned by this delegator. After this, the server
// never sends anyone to us again.
void tangram_delegator_flush_to_server() {
    void* ack = NULL;
    tangram_ucx_delegator_sendrecv_server(AM_ID_RELEASE_LOCK_CLIENT_REQUEST, NULL, 0, &ack);
    free(ack);
}

void tangram_delegator_stop() {
    tangram_ucx_delegator_stop();
    tangram_lockmgr_finalize(&g_lt);
}
