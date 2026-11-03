/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef SLAB_READ_H
#define SLAB_READ_H

/* Slab reads off the main thread (slab_read.c), dispatched by the commands in t_slab.c. */

#include "sds.h"

#include <stdbool.h>
#include <stddef.h>

struct client;
struct slabColl;

/* Whether a read of 'nkeys' keys (SIZE_MAX: a whole-collection read) goes to a query
 * thread. If not, the command answers it inline, with the same reply. */
bool slabReadAsync(struct client *c, size_t nkeys);
void slabReadDispatchMget(struct client *c, struct slabColl *coll, int first, int count);
void slabReadDispatchScan(struct client *c, struct slabColl *coll, sds start, sds prefix, long count, bool novalues);
void slabReadDispatchCount(struct client *c, struct slabColl *coll);
void slabReadBeforeSleep(void);
void slabReadClientGone(struct client *c);
sds slabReadInfo(sds info, bool debug);
void slabReadSetHold(bool hold);
void slabReadKillThreads(void);

#endif /* SLAB_READ_H */
