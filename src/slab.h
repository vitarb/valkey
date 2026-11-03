/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef SLAB_H
#define SLAB_H

/* The slab engine: a collection of many small entries under one key.
 *
 * Writes go to an active memtable (a VersionedLog whose versions are command sequence
 * numbers). Full memtables freeze; maintenance flushes frozen memtables into a tree of
 * leaves, each an immutable prefix-compressed base block plus an append-only delta log,
 * and compacts leaves whose deltas grew. A View publishes one consistent state: the
 * active and frozen memtables, the tree and flushed_through, the newest memtable id the
 * tree contains. A snapshot is a View plus the active memtable's bound; every read sees
 * exactly the writes at or below its bounds.
 *
 * Threads: only the main thread writes, freezes, installs maintenance results and
 * takes references to the published View. Readers on other threads use snapshots the
 * main thread handed them. */

#ifndef __cplusplus
#include <stdatomic.h>
#endif
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct slabColl slabColl;
typedef struct slabView slabView;
typedef struct slabIter slabIter;

/* A collection's settings. Users set the options (SLCREATE / SLALTER), which are
 * persisted; the engine parameters stay at their defaults outside unit tests. */
typedef struct slabConfig {
    /* Options. */
    uint32_t block_bytes;
    uint64_t flush_entries;     /* 0 = off */
    uint64_t flush_interval_ms; /* 0 = off */
    bool auto_compact;
    /* Engine parameters. */
    uint32_t key_restart;
    uint32_t value_restart;
    uint32_t compact_ratio_bp; /* basis points of the base */
} slabConfig;

#define SLAB_DEFAULT_BLOCK_BYTES 65536
#define SLAB_DEFAULT_RESTART 16
#define SLAB_DEFAULT_FLUSH_ENTRIES 10000
#define SLAB_DEFAULT_COMPACT_RATIO_BP 3750
#define SLAB_MAX_RATIO_BP 100000000
#define SLAB_MAX_FLUSH_INTERVAL_MS 3600000

/* Backpressure: a client write is refused with -SLABBUSY once a collection's pending
 * writes would pass SLAB_MAX_PENDING_BYTES, or while it has SLAB_MAX_FROZEN_MEMTABLES
 * frozen memtables. Pending bytes past the cap also freeze the active memtable. */
#define SLAB_MAX_PENDING_BYTES (256ULL * 1024 * 1024)
#define SLAB_MAX_FROZEN_MEMTABLES 16
/* The limits in force: the defaults above, unless a test changed them (DEBUG SLAB
 * MAX-PENDING-BYTES, MAX-FROZEN-MEMTABLES). */
extern uint64_t slab_max_pending_bytes;
extern uint32_t slab_max_frozen_memtables;

void slabConfigDefaults(slabConfig *cfg);
/* Checks every option for range; returns NULL or a static error message. */
const char *slabConfigValidate(const slabConfig *cfg);

/* Options as persisted (RDB): numbered (id, value) pairs. slabConfigToPairs fills at
 * most SLAB_CONFIG_PAIRS of each; slabConfigSetPair returns false for an unknown id. */
#define SLAB_CONFIG_PAIRS 4
int slabConfigToPairs(const slabConfig *cfg, uint64_t *ids, uint64_t *vals);
bool slabConfigSetPair(slabConfig *cfg, uint64_t id, uint64_t val);

/* Maintenance counters of one collection, added by the thread that owns the event. */
typedef struct slabStats {
    _Atomic(uint64_t) flushes;
    _Atomic(uint64_t) compactions;
    _Atomic(uint64_t) defragged_leaves;
    _Atomic(uint64_t) user_bytes_written;
    _Atomic(uint64_t) maint_delta_record_bytes;
    _Atomic(uint64_t) maint_base_block_bytes;
    _Atomic(uint64_t) maint_rewrap_bytes;
    _Atomic(uint64_t) maint_index_bytes;
} slabStats;

/* ---------------------------------------------------------------------------------------
 * Collections
 * ------------------------------------------------------------------------------------- */

slabColl *slabCollCreate(const slabConfig *cfg);
/* The key holding the collection was freed (any thread, including lazyfree): stops
 * maintenance and drops the key's reference. */
void slabCollDetach(slabColl *c);
void slabCollRetain(slabColl *c);
void slabCollRelease(slabColl *c);
/* Whether the key holding the collection was freed (any thread). */
bool slabCollDetached(const slabColl *c);
/* Freezes the active memtables that have reached FLUSH_INTERVAL_MS (main thread, once
 * per event-loop turn; one load while no collection is armed). */
void slabIdleFlush(void);
/* COPY: an O(1) copy sharing the source's tree and frozen memtables. Main thread. */
slabColl *slabCollCopy(slabColl *src);
const slabConfig *slabCollConfig(const slabColl *c);
/* SLALTER. Main thread. */
void slabCollAlter(slabColl *c, const slabConfig *cfg);
/* Allocations a free touches, for lazyfree's effort estimate. */
size_t slabCollFreeEffort(const slabColl *c);
/* Exact allocator bytes of the published state (MEMORY USAGE). Main thread. */
size_t slabCollMemUsage(const slabColl *c);
/* O(1) estimate of live entries (SLCOUNT without EXACT). */
uint64_t slabCollCountEstimate(const slabColl *c);

/* ---------------------------------------------------------------------------------------
 * Writes (main thread)
 * ------------------------------------------------------------------------------------- */

typedef enum slabOrigin {
    SLAB_ORIGIN_CLIENT,
    SLAB_ORIGIN_REPLICATED, /* primary link, slot import */
    SLAB_ORIGIN_LOADING,    /* AOF replay */
} slabOrigin;

#define SLAB_ADMIT_OK 0
#define SLAB_ADMIT_BUSY_BYTES 1
#define SLAB_ADMIT_BUSY_LAYERS 2
#define SLAB_ADMIT_TOO_LARGE 3 /* the batch alone exceeds the cap: no retry can pass */

/* Backpressure for a batch of n records of 'bytes' key and value bytes. Only client
 * writes are refused; for other origins the result says whether a client write would
 * have been (the caller counts the bypass). A refusal on bytes that would pass once
 * the active memtable drained freezes it, so admission stays live. */
int slabAdmit(slabColl *c, slabOrigin origin, size_t n, size_t bytes);
/* Applies one write command: n records, all puts or all tombstones, as one batch with
 * one sequence number. Then freezes the active memtable if a trigger holds. */
void slabApply(slabColl *c, size_t n, const unsigned char *const *keys, const size_t *klens, const unsigned char *const *vals, const size_t *vlens, bool tombstones);

/* ---------------------------------------------------------------------------------------
 * Reads
 * ------------------------------------------------------------------------------------- */

typedef struct slabSnapshot {
    slabColl *coll;
    slabView *view;
    uint64_t bound; /* the active memtable's published_seq when taken */
    bool owned;     /* holds references to the View and the collection */
} slabSnapshot;

/* Takes a snapshot of the published state. Main thread. An owned snapshot may be
 * handed to another thread and must be released (on any thread); an unowned one is
 * valid until the main thread returns to the event loop. */
void slabSnapshotTake(slabColl *c, slabSnapshot *s, bool owned);
void slabSnapshotRelease(slabSnapshot *s);
/* The collection options as of the snapshot. */
const slabConfig *slabSnapshotConfig(const slabSnapshot *s);

/* Point lookup. Returns true and sets the value if the key is live. */
bool slabGet(const slabSnapshot *s, const unsigned char *key, size_t klen, const unsigned char **val, size_t *vlen);
/* Batch lookup. vals[i] is NULL for a missing key. Values point into the snapshot and
 * stay valid while it is held. Any thread. */
void slabMultiGet(const slabSnapshot *s, size_t n, const unsigned char *const *keys, const size_t *klens, const unsigned char **vals, size_t *vlens);

/* The number of live entries, by iterating them all (SLCOUNT EXACT). Any thread. */
uint64_t slabCountExact(const slabSnapshot *s);

/* Ordered iteration over the live entries of a snapshot, from 'start' (inclusive).
 * 'fork_safe' avoids publishing or waiting for shared state (fork children, forkless
 * saves). */
slabIter *slabIterCreate(const slabSnapshot *s, const unsigned char *start, size_t start_len, bool fork_safe);
bool slabIterNext(slabIter *it, const unsigned char **key, size_t *klen, const unsigned char **val, size_t *vlen);
/* Stop at the first key boundary after this many records were visited (0 = none). */
void slabIterSetBudget(slabIter *it, uint64_t visits);
/* True when the last slabIterNext stopped on the budget with records left; the
 * resume point is then the last visited key (slabIterLastKey). */
bool slabIterStoppedOnBudget(const slabIter *it);
void slabIterLastKey(const slabIter *it, const unsigned char **key, size_t *klen);
void slabIterFree(slabIter *it);

/* ---------------------------------------------------------------------------------------
 * Maintenance
 * ------------------------------------------------------------------------------------- */

/* Freezes a non-empty active memtable. Returns the entries frozen. Main thread. */
uint64_t slabFreeze(slabColl *c);
/* SLFLUSH: freezes and returns the entries handed to the flush; *target receives the
 * memtable id that must be installed for WAIT. */
uint64_t slabRequestFlush(slabColl *c, uint64_t *target);
bool slabFlushDone(const slabColl *c, uint64_t target);
/* SLCOMPACT: leaves scheduled now (FULL picks them as its pass runs); *target for WAIT. */
uint64_t slabRequestCompaction(slabColl *c, bool full, uint64_t *target);
/* Active defrag (main thread): checks up to SLAB_DEFRAG_SCAN_LEAVES base blocks with
 * 'should_move' (the allocator's hint, which only the main thread may query), from where
 * the last call stopped, and has maintenance rebuild those leaves in new allocations.
 * Blocks readers share are never moved in place. */
#define SLAB_DEFRAG_SCAN_LEAVES 1024
void slabRequestDefrag(slabColl *c, int (*should_move)(void *ptr));
bool slabCompactionDone(const slabColl *c, uint64_t target);

/* Maintenance threads (slab_maint.c). slabMaintConfigure sets how many threads the
 * first unit starts; they wake the main thread through a pipe, whose handler and
 * beforeSleep call slabMaintDrain() to apply the finished units' results. With zero
 * threads units run inline. */
void slabMaintConfigure(int threads);
size_t slabMaintDrain(void);
int slabMaintThreads(void);
void slabMaintKillThreads(void);
/* Setup and teardown of a slab thread (maintenance or query), as bio.c does. */
void slabThreadSetup(const char *name, const char *cpulist);
void slabThreadKill(pthread_t tid, const char *name);
void slabMaintStats(uint64_t *units, uint64_t *installs, uint64_t *fork_waits, uint64_t *fork_timeouts, uint64_t *reclaimed);
uint64_t slabReclaimQueueLen(void);
void slabMaintSetPaused(bool paused);
void slabMaintSetSectionHold(bool hold);
int slabMaintSectionsHeld(void);

/* Fork handling (main thread, around fork()). */
bool slabForkPrepare(void);
void slabForkDone(bool child, bool prepared);
bool slabForkImageVerified(void);
void slabSetChildActive(bool active);

/* ---------------------------------------------------------------------------------------
 * Persistence
 * ------------------------------------------------------------------------------------- */

/* Export of one snapshot: the leaves in order, each with its lower bound, base block
 * and visible delta (merged into one persisted segment, or NULL), then the pending
 * entries of every memtable merged newest-wins in key order. */
typedef struct slabExport slabExport;
slabExport *slabExportBegin(const slabSnapshot *s);
uint64_t slabExportLeafCount(const slabExport *e);
/* Leaf i: lower bound, base block bytes, and a zmalloc'd delta segment (or NULL). */
void slabExportLeaf(slabExport *e, uint64_t i, const unsigned char **lower, size_t *lower_len, const unsigned char **base, size_t *base_len, unsigned char **delta, size_t *delta_len);
/* Pending entries: a fork-safe iterator over memtables only; val NULL = tombstone. */
uint64_t slabExportPendingCount(slabExport *e);
bool slabExportPendingNext(slabExport *e, const unsigned char **key, size_t *klen, const unsigned char **val, size_t *vlen);
void slabExportEnd(slabExport *e);

typedef struct slabImport slabImport;
slabImport *slabImportBegin(const slabConfig *cfg);
/* Takes ownership of the buffers on success; returns NULL or an error message. */
const char *slabImportLeaf(slabImport *im, const unsigned char *lower, size_t lower_len, unsigned char *base, size_t base_len, unsigned char *delta, size_t delta_len);
const char *slabImportPending(slabImport *im, const unsigned char *key, size_t klen, const unsigned char *val, size_t vlen, bool tombstone);
slabColl *slabImportFinish(slabImport *im, const char **err);
void slabImportAbort(slabImport *im);

/* ---------------------------------------------------------------------------------------
 * Introspection
 * ------------------------------------------------------------------------------------- */

typedef struct slabInfo {
    uint64_t entries;
    uint64_t blocks;
    uint64_t delta_blocks;
    uint64_t delta_entries;
    uint64_t delta_puts;
    uint64_t delta_tombstones;
    uint64_t delta_bytes;
    uint64_t base_bytes;
    uint64_t snapshot_bytes;
    uint64_t data_bytes;
    uint64_t pending_writes;
    uint64_t pending_writes_draining;
    uint64_t pending_bytes;
    uint64_t memtable_puts;
    uint64_t memtable_tombstones;
    uint64_t frozen_memtables;
    uint64_t flush_inflight;
    uint64_t compaction_requested;
    uint64_t compaction_inflight;
    uint64_t background_queue_depth;
} slabInfo;

void slabCollInfo(const slabColl *c, slabInfo *out);
const slabStats *slabCollStats(const slabColl *c);
/* Checks the tree's structure on a snapshot: frozen memtable ids, page sizes, leaf
 * bounds increasing from the empty key, and every base and visible delta key inside its
 * leaf. Blocks are validated when built or loaded, and the incremental totals are not
 * recounted (in-place flush appends move the live counters past a View's). Returns NULL
 * or an error (zmalloc'd). Any thread. */
char *slabVerify(const slabSnapshot *s);

/* Forkless save pin (bgiteration, main thread). */
void slabPinForIteration(slabColl *c);
void slabUnpinForIteration(slabColl *c);
bool slabPinnedSnapshot(slabColl *c, slabSnapshot *s);

#endif /* SLAB_H */
