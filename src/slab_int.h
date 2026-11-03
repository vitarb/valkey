/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef SLAB_INT_H
#define SLAB_INT_H

/* Internals of the slab engine, shared by slab.c, slab_tree.c and slab_maint.c.
 * C only. The invariant and the threading rule are in slab.h. */

#include "slab.h"
#include "slab_block.h"
#include "slab_fence.h"
#include "slab_vlog.h"

#include <stdatomic.h>
#include <pthread.h>

/* ---------------------------------------------------------------------------------------
 * Reference counts
 *
 * An increment is relaxed: the caller already holds a reference (for a View, the
 * published one, held by the main thread), so the object cannot be freed meanwhile. A
 * decrement is a release, and the thread that drops the last reference issues an
 * acquire fence before freeing, so every access by every former holder happens-before
 * the free.
 * ------------------------------------------------------------------------------------- */

static inline void slabRefIncr(_Atomic uint32_t *r) {
    atomic_fetch_add_explicit(r, 1, memory_order_relaxed);
}

static inline int slabRefDecr(_Atomic uint32_t *r) {
    if (atomic_fetch_sub_explicit(r, 1, memory_order_release) == 1) {
        atomic_thread_fence(memory_order_acquire);
        return 1;
    }
    return 0;
}

/* ---------------------------------------------------------------------------------------
 * Tuning constants
 * ------------------------------------------------------------------------------------- */

#define SLAB_PAGE_MAX 512
#define SLAB_PAGE_MIN (SLAB_PAGE_MAX / 4)
#define SLAB_MIN_MEMTABLE_SLOTS 64
#define SLAB_MAX_FROZEN_SOFT 4
#define SLAB_LAG_FLUSH_ENTRIES (32 * 1024)
/* Hot keys: a memtable also freezes at this many records (versions) per FLUSH_ENTRIES,
 * so overwriting a few keys cannot keep every version until the pending-bytes cap. */
#define SLAB_HOT_RECORDS_FACTOR 4
#define SLAB_FLUSH_UNIT_MAX_MEMTABLES 8
#define SLAB_FLUSH_UNIT_MAX_BYTES (128ULL * 1024 * 1024)
#define SLAB_COMPACT_UNIT_MAX_LEAVES 64
#define SLAB_COMPACT_UNIT_MAX_BYTES (16ULL * 1024 * 1024)
/* Data-parallel flushes: the fewest records per chunk. */
#define SLAB_PAR_MIN_CHUNK 2048
/* Records a flush appends between two chances for a pending fork to proceed. */
#define SLAB_FORK_YIELD_RECORDS 64
/* Contiguous candidate leaves a non-FULL compaction packs as one run, at most. */
#define SLAB_COMPACT_RUN_SPLIT 8
#define SLAB_HARD_CAP_BASE_FACTOR 4
#define SLAB_HARD_CAP_FLOOR (64ULL * 1024 * 1024)
#define SLAB_HARD_CAP_SLACK (1024 * 1024)
#define SLAB_DENSE_RECORD_OVERHEAD 48
#define SLAB_WASTE_FACTOR 3
#define SLAB_MAX_OUTSTANDING_INSTALLS 2
#define SLAB_RATIO_BP 10000
#define SLAB_SCAN_FREEZE_MIN_ENTRIES 1024

/* ---------------------------------------------------------------------------------------
 * Memtables
 * ------------------------------------------------------------------------------------- */

/* A frozen memtable's newest version per key, in key order. */
typedef struct slabRun {
    size_t n;
    const slabVlogRec *recs[];
} slabRun;

typedef struct slabMemtable {
    _Atomic uint32_t refcount;
    uint64_t id;
    slabVlog *log; /* NULL for the shared empty memtable */
    /* Highest sequence number visible to readers. Commit stores it with release;
     * a snapshot loads it with acquire. */
    _Atomic uint64_t published_seq;
    uint64_t trig_entries; /* 0 = off */
    uint64_t trig_records; /* 0 = off */
    long long activated_ms;
    uint64_t charged_bytes; /* what a freeze added to the collection's frozen_bytes */
    /* Built once, by CAS, only on a frozen memtable (no writer). */
    _Atomic(slabRun *) run;
} slabMemtable;

/* ---------------------------------------------------------------------------------------
 * Tree
 * ------------------------------------------------------------------------------------- */

/* A leaf's delta: 'own' is the append-only log the builder creates on its first flush
 * into the leaf (published with release, read with acquire); 'older' an immutable
 * segment from RDB or a copy. Shared by the leaf copies of copy-on-write pages. */
typedef struct slabLeafDelta {
    _Atomic uint32_t refcount;
    _Atomic(slabVlog *) own;
    slabDeltaSeg *older;
    uint64_t owner; /* collection whose builder may append to 'own' */
} slabLeafDelta;

typedef struct slabLeaf {
    slabBase *base;
    slabLeafDelta *delta;
    /* Hint that the delta may hold records, so readers of a leaf without one never
     * touch its delta (a cache miss per key otherwise). Set by the flush before its
     * first append into the leaf, relaxed: a reader needs those records only after the
     * install, which happens-after this store. A page copy carries the value along;
     * a reader of an older copy cannot need records appended after the copy. */
    _Atomic uint8_t has_delta;
} slabLeaf;

typedef struct slabBaseInfo {
    uint64_t leaves;
    uint64_t base_entries;
    uint64_t base_bytes;  /* encoded */
    uint64_t alloc_bytes; /* blocks, structs, fence indexes */
} slabBaseInfo;

typedef struct slabPage {
    _Atomic uint32_t refcount;
    uint32_t n;
    slabFence *fence; /* lower bounds of the leaves */
    slabBaseInfo info;
    size_t alloc_bytes;
    slabLeaf leaves[];
} slabPage;

typedef struct slabTree {
    _Atomic uint32_t refcount;
    uint32_t npages;
    uint64_t nleaves;
    uint64_t owner;
    slabFence *fence; /* lower bounds of the pages */
    slabBaseInfo info;
    uint64_t *first; /* global position of each page's first leaf */
    size_t alloc_bytes;
    slabPage *pages[];
} slabTree;

typedef struct slabDeltaInfo {
    uint64_t delta_leaves;
    uint64_t entries;
    uint64_t puts;
    uint64_t tombstones;
    uint64_t payload_bytes;
    uint64_t bytes;
} slabDeltaInfo;

/* ---------------------------------------------------------------------------------------
 * View and collection
 * ------------------------------------------------------------------------------------- */

typedef struct slabView {
    _Atomic uint32_t refcount;
    slabMemtable *active;
    slabTree *tree;
    uint64_t flushed_through;
    uint64_t generation;
    slabDeltaInfo delta; /* totals for 'tree' at 'flushed_through' */
    slabConfig config;
    uint32_t nfrozen;
    slabMemtable *frozen[]; /* newest first */
} slabView;

typedef enum {
    SLAB_INSTALL_FLUSH,
    SLAB_INSTALL_COMPACTION,
    SLAB_INSTALL_REWRAP
} slabInstallKind;

/* A maintenance result for the main thread to install (slabInstallApply). */
typedef struct slabInstall {
    struct slabInstall *next;
    slabInstallKind kind;
    struct slabTree *base;     /* the tree the unit started from (identity checked) */
    uint64_t base_ft;          /* the builder's flushed_through when the unit started */
    struct slabTree *new_tree; /* NULL: keep the published tree */
    uint64_t consumed_through;
    uint32_t consumed;
    slabDeltaInfo delta;
    /* Explicit compaction requests (SLCOMPACT sequence numbers) this install completes:
     * a FULL pass, and a non-FULL scan whose candidates are all compacted. */
    uint64_t done_full_seq, done_scan_seq;
} slabInstall;

/* Maintenance state of one collection, owned by the unit running on it. */
typedef struct slabMaint {
    slabTree *builder_tree; /* the tree after the last successful unit, installed or not */
    uint64_t builder_ft;
    slabDeltaInfo builder_delta;
    bool foreign;         /* a copy that must re-wrap shared deltas first */
    bool full_compact;    /* a FULL pass is running */
    bool scan_leaves;     /* evaluate every leaf's triggers (after load or copy) */
    uint64_t *candidates; /* lower bounds (4-byte length + bytes) of candidate leaves */
    size_t ncandidates, candidates_cap;
    /* The FULL pass resumes at the leaf holding this key (a key, not a position: the
     * pass's own rewrites shift positions). */
    unsigned char *compact_cursor;
    size_t compact_cursor_len;
    uint64_t full_seq;   /* the SLCOMPACT FULL request the running pass completes */
    uint64_t scan_seq;   /* the SLCOMPACT request its explicit candidates complete */
    bool explicit_cands; /* candidates are checked with the explicit triggers */
    /* Defrag pass: the bases to move (sorted), taken from the request; the position
     * reached in the builder tree. */
    void **defrag_bases;
    size_t ndefrag_bases;
    uint64_t defrag_cursor;
    int flush_streak;
} slabMaint;

struct slabColl {
    _Atomic uint32_t refcount;
    /* Weak references (the idle-flush heap's), plus one held by all strong ones together:
     * the last strong release frees the contents, the last weak one the struct. */
    _Atomic uint32_t weak;
    uint64_t id;
    slabView *view; /* the published View: read and replaced by the main thread only */

    /* Writer state (main thread). */
    uint64_t next_seq;
    uint64_t next_memtable_id;
    uint32_t frozen_count;
    uint64_t frozen_bytes;
    uint64_t last_frozen_entries; /* sizes the next memtable */
    long long idle_due_ms;        /* FLUSH_INTERVAL_MS: when the idle heap checks it (0 = not armed) */

    /* Maintenance. */
    _Atomic int detached;
    _Atomic uint32_t installs_outstanding;
    _Atomic uint64_t req_full_seq; /* SLCOMPACT FULL requested (its sequence number) */
    _Atomic uint64_t req_scan_seq; /* SLCOMPACT requested: evaluate every leaf */
    _Atomic int compact_pending;   /* the builder has candidates or a FULL pass left */
    uint64_t defrag_scan;          /* main thread: next leaf slabRequestDefrag checks */
    slabMaint maint;
    /* Scheduling (slab_maint.c), under the scheduler lock. */
    int sched;
    bool sched_rerun;
    struct slabView *maint_view; /* the mailbox: the View handed to maintenance */
    pthread_mutex_t install_lock;
    slabInstall *installs_head, *installs_tail;
    void **defrag_req; /* under install_lock: bases active defrag asked to move */
    size_t ndefrag_req, defrag_req_cap;

    /* Main-thread maintenance bookkeeping for INFO and WAIT. */
    uint64_t compact_requested; /* SLCOMPACT requests (sequence numbers) */
    uint64_t compact_done;      /* the highest request whose work is installed */
    _Atomic int flush_inflight;
    _Atomic int compaction_inflight;

    /* Counters. */
    slabStats stats;

    /* Forkless save pin: taken and dropped on the main thread. */
    slabView *iter_pin;
    uint64_t iter_pin_bound;
    uint32_t iter_pin_count;
};

/* ---------------------------------------------------------------------------------------
 * Internal functions
 * ------------------------------------------------------------------------------------- */

/* Memtables (slab.c). */
slabMemtable *slabMemtableCreate(uint64_t id, const slabConfig *cfg, uint64_t prev_entries, bool lag);
void slabMemtableRetain(slabMemtable *m);
void slabMemtableRelease(slabMemtable *m);
uint64_t slabMemtableBytes(const slabMemtable *m);
const slabRun *slabMemtableRun(slabMemtable *m, bool publish);
slabRun *slabRunBuild(const slabVlog *log, uint64_t bound);
void slabRunFree(slabRun *r);

/* Views (slab.c). */
slabView *slabViewCreate(uint32_t nfrozen);
void slabViewRetain(slabView *v);
void slabViewRelease(slabView *v);
void slabViewFree(slabView *v);

/* Trees (slab_tree.c). */
slabTree *slabTreeEmpty(uint64_t owner);
void slabTreeRetain(slabTree *t);
void slabTreeRelease(slabTree *t);
static inline slabLeaf *slabTreeLeaf(const slabTree *t, uint64_t pos, uint32_t *page, uint32_t *local);
uint64_t slabTreeRoute(const slabTree *t, const unsigned char *key, size_t klen);
void slabTreeLower(const slabTree *t, uint64_t pos, const unsigned char **key, size_t *klen);
slabTree *slabTreeFromLeaves(uint64_t owner, size_t n, slabLeaf *leaves, unsigned char **lowers, size_t *lower_lens);
slabTree *slabTreeReplace(const slabTree *t, uint64_t from, uint64_t to, size_t n, slabLeaf *leaves, unsigned char **lowers, size_t *lower_lens);
void slabLeafDeltaRetain(slabLeafDelta *d);
void slabLeafDeltaRelease(slabLeafDelta *d);
void slabBaseRetain(slabBase *b);
void slabBaseRelease(slabBase *b);
void slabDeltaSegRelease(slabDeltaSeg *s);
void slabLeafDeltaCounters(const slabLeafDelta *d, slabDeltaInfo *out);

/* Reclamation of Views whose last reference drops (slab_maint.c). */
void slabReclaimView(slabView *v);
void slabMaintNotify(slabColl *c);

/* The fork gate (slab_maint.c): every in-place publication a fork child
 * can read runs inside a section. */
void slabSectionEnter(void);
void slabSectionLeave(void);
void slabSectionYield(void);

/* Data-parallel unit work (slab_maint.c): runs chunk(ctx, i) for every i < nchunks on the
 * calling maintenance thread and idle ones; returns when all are done. */
void slabParRun(void (*chunk)(void *ctx, size_t idx), void *ctx, size_t nchunks);

static inline slabLeaf *slabTreeLeaf(const slabTree *t, uint64_t pos, uint32_t *page, uint32_t *local) {
    /* Pages hold SLAB_PAGE_MIN..SLAB_PAGE_MAX leaves, so a binary search over the
     * page starts finds the page. */
    uint32_t lo = 0, hi = t->npages;
    while (hi - lo > 1) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (t->first[mid] <= pos)
            lo = mid;
        else
            hi = mid;
    }
    *page = lo;
    *local = (uint32_t)(pos - t->first[lo]);
    return &t->pages[lo]->leaves[*local];
}

#endif /* SLAB_INT_H */
