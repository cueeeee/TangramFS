#ifndef _TANGRAMFS_DELEGATOR_H_
#define _TANGRAMFS_DELEGATOR_H_

#include "tangramfs.h"

/**
 * Delegator is used for locking-based implementation.
 * Rank 0 of each node serves as the node-local server
 * and processes all requests from the same node.
 */

void tangram_delegator_start(tfs_info_t* tfs_info);
void tangram_delegator_stop();
// Return all locks this delegator owns to the global server. Call before stop.
void tangram_delegator_flush_to_server();

#endif
