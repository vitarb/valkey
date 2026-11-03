/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Slab engine core: collections, memtables, Views, the write path and the read path.
 * Tree construction and maintenance units are in slab_tree.c. */

#include "slab_int.h"
#include "slab_hash.h"
#include "serverassert.h"
#include "zmalloc.h"

#include <stdlib.h>
#include <string.h>

uint64_t slab_max_pending_bytes = SLAB_MAX_PENDING_BYTES;
uint32_t slab_max_frozen_memtables = SLAB_MAX_FROZEN_MEMTABLES;

static _Atomic uint64_t next_coll_id = 1;

/* ---------------------------------------------------------------------------------------
 * Config
 * ------------------------------------------------------------------------------------- */

void slabConfigDefaults(slabConfig *cfg) {
    memset(cfg, 0, sizeof(*cfg));
    cfg->block_bytes = SLAB_DEFAULT_BLOCK_BYTES;
    cfg->key_restart = SLAB_DEFAULT_RESTART;
    cfg->value_restart = SLAB_DEFAULT_RESTART;
    cfg->flush_entries = SLAB_DEFAULT_FLUSH_ENTRIES;
    cfg->compact_ratio_bp = SLAB_DEFAULT_COMPACT_RATIO_BP;
    cfg->auto_compact = true;
}

const char *slabConfigValidate(const slabConfig *cfg) {
    if (cfg->block_bytes < 1024 || cfg->block_bytes > 64 * 1024 * 1024) return "BLOCK_BYTES must be between 1024 and 67108864";
    if (cfg->key_restart < 1 || cfg->key_restart > SLAB_MAX_RESTART) return "KEY_RESTART must be between 1 and 1024";
    if (cfg->value_restart < 1 || cfg->value_restart > SLAB_MAX_RESTART) return "VALUE_RESTART must be between 1 and 1024";
    if (cfg->flush_entries > (1ULL << 30)) return "FLUSH_ENTRIES must be at most 1073741824";
    if (cfg->compact_ratio_bp < 1 || cfg->compact_ratio_bp > SLAB_MAX_RATIO_BP) return "COMPACT_RATIO must be between 0.0001 and 10000";
    return NULL;
}

/* Persisted option ids; never reused. Engine parameters are not persisted. */
enum {
    SLAB_OPT_BLOCK_BYTES = 1,
    SLAB_OPT_FLUSH_ENTRIES = 2,
    SLAB_OPT_AUTO_COMPACT = 3,
};

int slabConfigToPairs(const slabConfig *cfg, uint64_t *ids, uint64_t *vals) {
    int n = 0;
#define PAIR(id, v) (ids[n] = (id), vals[n] = (uint64_t)(v), n++)
    PAIR(SLAB_OPT_BLOCK_BYTES, cfg->block_bytes);
    PAIR(SLAB_OPT_FLUSH_ENTRIES, cfg->flush_entries);
    PAIR(SLAB_OPT_AUTO_COMPACT, cfg->auto_compact);
#undef PAIR
    return n;
}

bool slabConfigSetPair(slabConfig *cfg, uint64_t id, uint64_t val) {
    switch (id) {
    case SLAB_OPT_BLOCK_BYTES: cfg->block_bytes = val > UINT32_MAX ? UINT32_MAX : (uint32_t)val; break;
    case SLAB_OPT_FLUSH_ENTRIES: cfg->flush_entries = val; break;
    case SLAB_OPT_AUTO_COMPACT: cfg->auto_compact = val != 0; break;
    default: return false;
    }
    return true;
}

/* ---------------------------------------------------------------------------------------
 * Memtables
 * ------------------------------------------------------------------------------------- */

/* The shared, empty active memtable of every idle collection: never freed, never
 * counted, never written. */
static slabMemtable slab_empty_memtable;

static uint64_t nextPow2(uint64_t v) {
    uint64_t c = 1;
    while (c < v) c <<= 1;
    return c;
}

/* A memtable's index capacity: next_pow2(min(2 x trigger, 2 x the previous memtable's
 * entries)), at least SLAB_MIN_MEMTABLE_SLOTS. A cold collection's first memtable thus
 * starts small and grows by doublings, while a memtable after a size-triggered freeze
 * is sized for its trigger and never grows on the main thread in steady state. */
static uint64_t memtableSlots(uint64_t trig_entries, uint64_t prev_entries) {
    uint64_t want = 2 * prev_entries;
    if (trig_entries && 2 * trig_entries < want) want = 2 * trig_entries;
    want = nextPow2(want);
    return want < SLAB_MIN_MEMTABLE_SLOTS ? SLAB_MIN_MEMTABLE_SLOTS : want;
}

/* The capacity of a memtable sized for its trigger (freeze-before-crossing applies only
 * at it). */
static uint64_t memtableTargetSlots(uint64_t trig_entries) {
    uint64_t want = nextPow2(2 * trig_entries);
    return want < SLAB_MIN_MEMTABLE_SLOTS ? SLAB_MIN_MEMTABLE_SLOTS : want;
}

slabMemtable *slabMemtableCreate(uint64_t id, const slabConfig *cfg, uint64_t prev_entries, bool lag) {
    slabMemtable *m = zcalloc(sizeof(*m));
    atomic_init(&m->refcount, 1);
    m->id = id;
    m->trig_entries = cfg->flush_entries;
    if (lag) {
        /* Maintenance lags: buffer more per memtable before the frozen-layer cap. A
         * disabled trigger stays disabled. */
        if (m->trig_entries && m->trig_entries < SLAB_LAG_FLUSH_ENTRIES) m->trig_entries = SLAB_LAG_FLUSH_ENTRIES;
    }
    m->trig_records = m->trig_entries ? m->trig_entries * SLAB_HOT_RECORDS_FACTOR : 0;
    m->log = slabVlogCreate(lag ? memtableTargetSlots(m->trig_entries) : memtableSlots(m->trig_entries, prev_entries), 0);
    atomic_init(&m->run, NULL);
    return m;
}

void slabMemtableRetain(slabMemtable *m) {
    if (m != &slab_empty_memtable) slabRefIncr(&m->refcount);
}

void slabMemtableRelease(slabMemtable *m) {
    if (m == &slab_empty_memtable || !slabRefDecr(&m->refcount)) return;
    slabRun *r = atomic_load_explicit(&m->run, memory_order_relaxed);
    slabRunFree(r);
    slabVlogFree(m->log);
    zfree(m);
}

uint64_t slabMemtableBytes(const slabMemtable *m) {
    if (!m->log) return 0;
    uint64_t run_bytes = 0;
    slabRun *r = atomic_load_explicit(&((slabMemtable *)m)->run, memory_order_acquire);
    if (r) run_bytes = zmalloc_size(r);
    return slabVlogBytes(m->log) + zmalloc_size((void *)m) + run_bytes;
}

static uint64_t memtableEntries(const slabMemtable *m) {
    return m->log ? slabVlogEntries(m->log) : 0;
}

static int runCmp(const void *a, const void *b) {
    const slabVlogRec *x = *(const slabVlogRec *const *)a, *y = *(const slabVlogRec *const *)b;
    return slabKeyCmp(x->data, x->klen, y->data, y->klen);
}

slabRun *slabRunBuild(const slabVlog *log, uint64_t bound) {
    uint64_t n = log ? slabVlogEntries(log) : 0;
    slabRun *r = zmalloc(sizeof(*r) + n * sizeof(r->recs[0]));
    r->n = 0;
    if (log) {
        slabVlogIter it;
        slabVlogIterInit(&it, log, bound);
        const slabVlogRec *rec;
        while ((rec = slabVlogIterNext(&it))) {
            /* The entry count is the writer's; a log being written (an active memtable
             * read under a bound) may have more slots than counted. */
            if (r->n == n) {
                n = n ? n * 2 : 16;
                r = zrealloc(r, sizeof(*r) + n * sizeof(r->recs[0]));
            }
            r->recs[r->n++] = rec;
        }
    }
    qsort(r->recs, r->n, sizeof(r->recs[0]), runCmp);
    return r;
}

void slabRunFree(slabRun *r) {
    zfree(r);
}

/* The sorted run of a frozen memtable. With 'publish', the run is built once and
 * shared through a CAS (release, paired with the acquire load here); without it (fork
 * children, forkless saves) an unpublished run is built privately and the caller
 * frees it: such paths must never wait on, or publish, shared state. */
const slabRun *slabMemtableRun(slabMemtable *m, bool publish) {
    slabRun *r = atomic_load_explicit(&m->run, memory_order_acquire);
    if (r) return r;
    slabRun *built = slabRunBuild(m->log, UINT64_MAX);
    if (!publish) return built;
    slabRun *expected = NULL;
    slabSectionEnter();
    bool won = atomic_compare_exchange_strong_explicit(&m->run, &expected, built, memory_order_acq_rel, memory_order_acquire);
    slabSectionLeave();
    if (won) return built;
    slabRunFree(built);
    return expected;
}

/* ---------------------------------------------------------------------------------------
 * Views
 * ------------------------------------------------------------------------------------- */

slabView *slabViewCreate(uint32_t nfrozen) {
    slabView *v = zcalloc(sizeof(*v) + nfrozen * sizeof(slabMemtable *));
    atomic_init(&v->refcount, 1);
    v->nfrozen = nfrozen;
    return v;
}

void slabViewRetain(slabView *v) {
    slabRefIncr(&v->refcount);
}

void slabViewFree(slabView *v) {
    slabMemtableRelease(v->active);
    for (uint32_t i = 0; i < v->nfrozen; i++) slabMemtableRelease(v->frozen[i]);
    slabTreeRelease(v->tree);
    zfree(v);
}

void slabViewRelease(slabView *v) {
    if (slabRefDecr(&v->refcount)) slabReclaimView(v);
}

/* A new View like 'cur' with room for nfrozen frozen memtables; the caller fills the
 * frozen list and takes references for whatever it keeps from 'cur'. */
static slabView *viewDerive(const slabView *cur, uint32_t nfrozen) {
    slabView *v = slabViewCreate(nfrozen);
    v->active = cur->active;
    v->tree = cur->tree;
    v->flushed_through = cur->flushed_through;
    v->generation = cur->generation + 1;
    v->delta = cur->delta;
    v->config = cur->config;
    return v;
}

/* Publishes a new View (main thread). The replaced View loses the collection's
 * reference; snapshots that still hold it keep it alive. */
static void publishView(slabColl *c, slabView *v) {
    slabView *old = c->view;
    c->view = v;
    slabViewRelease(old);
}

/* ---------------------------------------------------------------------------------------
 * Collections
 * ------------------------------------------------------------------------------------- */

static slabColl *collAlloc(const slabConfig *cfg) {
    slabColl *c = zcalloc(sizeof(*c));
    atomic_init(&c->refcount, 1);
    c->id = atomic_fetch_add_explicit(&next_coll_id, 1, memory_order_relaxed);
    c->next_seq = 1;
    c->next_memtable_id = 1;
    pthread_mutex_init(&c->install_lock, NULL);
    slabView *v = slabViewCreate(0);
    v->active = &slab_empty_memtable;
    v->tree = slabTreeEmpty(c->id);
    v->generation = 1;
    v->config = *cfg;
    c->view = v;
    c->maint.builder_tree = v->tree;
    slabTreeRetain(v->tree);
    return c;
}

slabColl *slabCollCreate(const slabConfig *cfg) {
    return collAlloc(cfg);
}

void slabCollRetain(slabColl *c) {
    slabRefIncr(&c->refcount);
}

void slabMaintRelease(slabMaint *m);

void slabCollRelease(slabColl *c) {
    if (!slabRefDecr(&c->refcount)) return;
    if (c->maint_view) slabViewRelease(c->maint_view);
    slabViewRelease(c->view);
    slabMaintRelease(&c->maint);
    zfree(c->defrag_req);
    pthread_mutex_destroy(&c->install_lock);
    zfree(c);
}

void slabMaintDropInstalls(slabColl *c);

void slabCollDetach(slabColl *c) {
    atomic_store_explicit(&c->detached, 1, memory_order_release);
    slabMaintDropInstalls(c);
    slabCollRelease(c);
}

bool slabCollDetached(const slabColl *c) {
    return atomic_load_explicit(&((slabColl *)c)->detached, memory_order_acquire);
}

const slabConfig *slabCollConfig(const slabColl *c) {
    return &c->view->config;
}

const slabStats *slabCollStats(const slabColl *c) {
    return &c->stats;
}

void slabMaintNotify(slabColl *c);

size_t slabCollFreeEffort(const slabColl *c) {
    const slabView *v = c->view;
    return 1 + v->nfrozen + v->tree->nleaves * 2 + v->tree->npages;
}

size_t slabCollMemUsage(const slabColl *c) {
    const slabView *v = c->view;
    size_t bytes = zmalloc_size((void *)c) + zmalloc_size((void *)v);
    bytes += slabMemtableBytes(v->active);
    for (uint32_t i = 0; i < v->nfrozen; i++) bytes += slabMemtableBytes(v->frozen[i]);
    bytes += v->tree->alloc_bytes + v->tree->info.alloc_bytes + v->delta.bytes;
    return bytes;
}

uint64_t slabCollCountEstimate(const slabColl *c) {
    const slabView *v = c->view;
    /* Base entries plus delta and memtable puts, minus their tombstones: overwrites
     * count twice and deletes of absent keys subtract, so this is an estimate. */
    int64_t n = (int64_t)v->tree->info.base_entries + (int64_t)v->delta.puts - (int64_t)v->delta.tombstones;
    for (uint32_t i = 0; i <= v->nfrozen; i++) {
        const slabMemtable *m = i == v->nfrozen ? v->active : v->frozen[i];
        if (!m->log) continue;
        n += (int64_t)slabVlogPuts(m->log) - (int64_t)slabVlogTombstones(m->log);
    }
    return n > 0 ? (uint64_t)n : 0;
}

/* ---------------------------------------------------------------------------------------
 * Snapshots
 * ------------------------------------------------------------------------------------- */

void slabSnapshotTake(slabColl *c, slabSnapshot *s, bool owned) {
    slabView *v = c->view;
    s->coll = c;
    s->view = v;
    /* Acquire: pairs with the release store of the last committed batch. */
    s->bound = atomic_load_explicit(&v->active->published_seq, memory_order_acquire);
    s->owned = owned;
    if (owned) {
        slabViewRetain(v);
        slabCollRetain(c);
    }
}

const slabConfig *slabSnapshotConfig(const slabSnapshot *s) {
    return &s->view->config;
}

void slabSnapshotRelease(slabSnapshot *s) {
    if (!s->owned) return;
    slabViewRelease(s->view);
    slabCollRelease(s->coll);
    s->owned = false;
}

void slabPinForIteration(slabColl *c) {
    if (c->iter_pin_count++ == 0) {
        slabSnapshot s;
        slabSnapshotTake(c, &s, false);
        slabViewRetain(s.view);
        c->iter_pin = s.view;
        c->iter_pin_bound = s.bound;
    }
}

void slabUnpinForIteration(slabColl *c) {
    assert(c->iter_pin_count > 0);
    if (--c->iter_pin_count == 0) {
        slabViewRelease(c->iter_pin);
        c->iter_pin = NULL;
    }
}

bool slabPinnedSnapshot(slabColl *c, slabSnapshot *s) {
    if (!c->iter_pin) return false;
    s->coll = c;
    s->view = c->iter_pin;
    s->bound = c->iter_pin_bound;
    s->owned = false;
    return true;
}

/* ---------------------------------------------------------------------------------------
 * Writes
 * ------------------------------------------------------------------------------------- */

uint64_t slabFreeze(slabColl *c) {
    slabView *cur = c->view;
    slabMemtable *m = cur->active;
    uint64_t entries = memtableEntries(m);
    if (m == &slab_empty_memtable || entries == 0) return 0;
    slabView *v = viewDerive(cur, cur->nfrozen + 1);
    /* The new View takes over the active memtable's reference as a frozen one; the
     * next write activates a fresh memtable. */
    v->active = &slab_empty_memtable;
    v->frozen[0] = m;
    slabMemtableRetain(m);
    for (uint32_t i = 0; i < cur->nfrozen; i++) {
        v->frozen[i + 1] = cur->frozen[i];
        slabMemtableRetain(cur->frozen[i]);
    }
    slabTreeRetain(v->tree);
    c->frozen_count++;
    /* Charged from the freeze on with its sorted run (8 bytes per entry), which a flush
     * or scan builds later; the install subtracts exactly this charge. */
    m->charged_bytes = slabMemtableBytes(m) + 8 * entries;
    c->frozen_bytes += m->charged_bytes;
    c->last_frozen_entries = entries;
    publishView(c, v);
    slabMaintNotify(c);
    return entries;
}

/* Replaces the empty active memtable with a fresh one sized from the last frozen
 * memtable (or the collection's trigger). */
static void activate(slabColl *c) {
    slabView *cur = c->view;
    uint64_t prev = c->last_frozen_entries;
    slabMemtable *m = slabMemtableCreate(c->next_memtable_id++, &cur->config, prev, c->frozen_count >= SLAB_MAX_FROZEN_SOFT);
    atomic_store_explicit(&m->published_seq, c->next_seq - 1, memory_order_relaxed);
    slabView *v = viewDerive(cur, cur->nfrozen);
    v->active = m;
    for (uint32_t i = 0; i < cur->nfrozen; i++) {
        v->frozen[i] = cur->frozen[i];
        slabMemtableRetain(cur->frozen[i]);
    }
    slabTreeRetain(v->tree);
    publishView(c, v); /* releases 'cur' */
}

int slabAdmit(slabColl *c, slabOrigin origin, size_t n, size_t bytes) {
    slabView *v = c->view;
    uint64_t cap = slab_max_pending_bytes;
    uint64_t batch = bytes + n * sizeof(slabVlogRec);
    uint64_t active = slabMemtableBytes(v->active);
    int res = SLAB_ADMIT_OK;
    if (batch > cap) return SLAB_ADMIT_TOO_LARGE;
    if (active + c->frozen_bytes + batch > cap)
        res = SLAB_ADMIT_BUSY_BYTES;
    else if (c->frozen_count >= slab_max_frozen_memtables)
        res = SLAB_ADMIT_BUSY_LAYERS;
    if (res == SLAB_ADMIT_BUSY_BYTES && origin == SLAB_ORIGIN_CLIENT && memtableEntries(v->active) > 0 &&
        c->frozen_bytes + batch <= cap) {
        /* The write would pass once the active memtable drained: freeze it, once per
         * episode, so a cap no size trigger reaches cannot stall every write. */
        slabFreeze(c);
    }
    return res;
}

void slabApply(slabColl *c, size_t n, const unsigned char *const *keys, const size_t *klens, const unsigned char *const *vals, const size_t *vlens, bool tombstones) {
    slabView *v = c->view;
    if (v->active == &slab_empty_memtable) {
        activate(c);
        v = c->view;
    } else if (n > slabVlogHeadroom(v->active->log) && memtableEntries(v->active) > 0 &&
               v->active->trig_entries && slabVlogCapacity(v->active->log) >= memtableTargetSlots(v->active->trig_entries)) {
        /* Freeze before crossing: a memtable sized for its trigger never grows its index
         * on the main thread. */
        slabFreeze(c);
        activate(c);
        v = c->view;
    }
    slabMemtable *m = v->active;
    uint64_t seq = c->next_seq++;
    size_t hint = 0, user = 0;
    for (size_t i = 0; i < n; i++) {
        size_t vl = tombstones ? 0 : vlens[i];
        hint += slabVlogRecordBytes(klens[i], vl);
        user += klens[i] + vl;
    }
    slabVlogBegin(m->log, seq, hint);
    for (size_t i = 0; i < n; i++)
        slabVlogAppend(m->log, keys[i], klens[i], slabHash(keys[i], klens[i]), tombstones ? NULL : vals[i],
                       tombstones ? 0 : vlens[i], tombstones);
    slabVlogCommit(m->log);
    /* Release: one command is one visibility step for every reader of this memtable. */
    atomic_store_explicit(&m->published_seq, seq, memory_order_release);
    atomic_fetch_add_explicit(&c->stats.user_bytes_written, user, memory_order_relaxed);

    uint64_t bytes = slabMemtableBytes(m);
    if ((m->trig_entries && slabVlogEntries(m->log) >= m->trig_entries) ||
        (m->trig_records && slabVlogRecords(m->log) >= m->trig_records) || bytes >= slab_max_pending_bytes)
        slabFreeze(c);
}

/* ---------------------------------------------------------------------------------------
 * Reads
 * ------------------------------------------------------------------------------------- */

static inline bool leafMayHaveDelta(const slabLeaf *l) {
    return atomic_load_explicit(&((slabLeaf *)l)->has_delta, memory_order_relaxed);
}

/* Probes a leaf's delta at the View's flushed_through. Returns a SLAB_LOOKUP_* code. */
static inline int probeDelta(const slabLeaf *l, const unsigned char *key, size_t klen, uint64_t hash, uint64_t ft, const unsigned char **val, size_t *vlen) {
    /* Acquire: pairs with the release that published the log (slab_tree.c). */
    slabVlog *own = atomic_load_explicit(&l->delta->own, memory_order_acquire);
    if (own) {
        int r = slabVlogLookup(own, key, klen, hash, ft, val, vlen);
        if (r != SLAB_LOOKUP_MISS) return r;
    }
    if (l->delta->older) {
        int r = slabDeltaSegLookup(l->delta->older, key, klen, hash, val, vlen);
        if (r == SLAB_SEG_PUT) return SLAB_LOOKUP_PUT;
        if (r == SLAB_SEG_TOMBSTONE) return SLAB_LOOKUP_TOMBSTONE;
    }
    return SLAB_LOOKUP_MISS;
}

static inline bool hasMemtableData(const slabView *v) {
    if (v->active->log && slabVlogEntries(v->active->log)) return true;
    return v->nfrozen > 0;
}

/* Memtable layers, newest first. Returns a SLAB_LOOKUP_* code. */
static inline int probeMemtables(const slabSnapshot *s, const unsigned char *key, size_t klen, uint64_t hash, const unsigned char **val, size_t *vlen) {
    const slabView *v = s->view;
    if (v->active->log) {
        int r = slabVlogLookup(v->active->log, key, klen, hash, s->bound, val, vlen);
        if (r != SLAB_LOOKUP_MISS) return r;
    }
    for (uint32_t i = 0; i < v->nfrozen; i++) {
        int r = slabVlogLookup(v->frozen[i]->log, key, klen, hash, UINT64_MAX, val, vlen);
        if (r != SLAB_LOOKUP_MISS) return r;
    }
    return SLAB_LOOKUP_MISS;
}

static inline const slabLeaf *leafById(const slabTree *t, uint64_t id) {
    return &t->pages[id >> 16]->leaves[id & 0xffff];
}

bool slabGet(const slabSnapshot *s, const unsigned char *key, size_t klen, const unsigned char **val, size_t *vlen) {
    const slabView *v = s->view;
    uint64_t h = 0;
    bool hashed = false;
    if (hasMemtableData(v)) {
        h = slabHash(key, klen);
        hashed = true;
        int r = probeMemtables(s, key, klen, h, val, vlen);
        if (r != SLAB_LOOKUP_MISS) return r == SLAB_LOOKUP_PUT;
    }
    const slabTree *t = v->tree;
    if (t->nleaves == 0) return false;
    const slabLeaf *l = leafById(t, slabTreeRoute(t, key, klen));
    if (leafMayHaveDelta(l)) {
        if (!hashed) h = slabHash(key, klen);
        int r = probeDelta(l, key, klen, h, v->flushed_through, val, vlen);
        if (r != SLAB_LOOKUP_MISS) return r == SLAB_LOOKUP_PUT;
    }
    return slabBaseGet(l->base, key, klen, val, vlen);
}

/* Batch lookup: memtables first (hashing each key at most once, and only if a memtable
 * holds data), then a walk through the tree in key order. Below SLAB_SORT_MIN_KEYS
 * unresolved keys each is routed alone. With at least SLAB_SORT_ONCE_PER_LEAF keys per
 * leaf the batch is sorted once and a key stays in its predecessor's leaf while below
 * that leaf's upper bound; otherwise every key is routed (8 lanes in lockstep), the batch
 * sorted by (leaf, key), and memory prefetched ahead, one dependent link per stage. */
#define SLAB_SORT_MIN_KEYS 8
#define SLAB_SORT_ONCE_PER_LEAF 3
#define SLAB_ROUTE_CHUNK 64
#define SLAB_PREFETCH_BASE 8
#define SLAB_PREFETCH_DELTA_HEADER 6
#define SLAB_PREFETCH_BLOCK 4
#define SLAB_HASH_RING 8

typedef struct walkEntry {
    uint64_t sort; /* routed leaf id (per-key routing), else 0 */
    uint64_t head; /* the 8 key bytes after the batch's common prefix, big-endian */
    uint32_t index;
} walkEntry;

/* Batches up to this size keep their walk state on the stack; larger ones allocate it.
 * No scratch outlives a call, so a reading thread may exit at any time. */
#define SLAB_WALK_STACK 128

typedef struct walkSortCtx {
    const unsigned char *const *keys;
    const size_t *klens;
    size_t lcp;
} walkSortCtx;

/* Order of the walk: by leaf, then by key. Integer comparisons settle almost every
 * pair; the full key is compared only when the leaf and the 8 bytes after the batch's
 * common prefix tie. */
static inline bool walkLess(const walkEntry *a, const walkEntry *b, const walkSortCtx *x) {
    if (a->sort != b->sort) return a->sort < b->sort;
    if (a->head != b->head) return a->head < b->head;
    size_t l = x->lcp;
    return slabKeyCmp(x->keys[a->index] + l, x->klens[a->index] - l, x->keys[b->index] + l, x->klens[b->index] - l) < 0;
}

static void walkInsertionSort(walkEntry *a, size_t n, const walkSortCtx *x) {
    for (size_t i = 1; i < n; i++) {
        walkEntry e = a[i];
        size_t j = i;
        while (j > 0 && walkLess(&e, &a[j - 1], x)) {
            a[j] = a[j - 1];
            j--;
        }
        a[j] = e;
    }
}

static void walkSiftDown(walkEntry *a, size_t i, size_t n, const walkSortCtx *x) {
    for (;;) {
        size_t c = 2 * i + 1;
        if (c >= n) return;
        if (c + 1 < n && walkLess(&a[c], &a[c + 1], x)) c++;
        if (!walkLess(&a[i], &a[c], x)) return;
        walkEntry t = a[i];
        a[i] = a[c];
        a[c] = t;
        i = c;
    }
}

/* An inlined introsort: quicksort with median-of-three pivots, insertion sort for
 * short runs, and heapsort past the depth limit. The libc qsort's indirect comparator
 * calls cost about a quarter of a 1024-key lookup. */
static void walkSort(walkEntry *a, size_t n, const walkSortCtx *x, int depth) {
    while (n > 16) {
        if (depth-- == 0) {
            for (size_t i = n / 2; i-- > 0;) walkSiftDown(a, i, n, x);
            for (size_t i = n - 1; i > 0; i--) {
                walkEntry t = a[0];
                a[0] = a[i];
                a[i] = t;
                walkSiftDown(a, 0, i, x);
            }
            return;
        }
        size_t mid = n / 2;
        if (walkLess(&a[mid], &a[0], x)) {
            walkEntry t = a[mid];
            a[mid] = a[0];
            a[0] = t;
        }
        if (walkLess(&a[n - 1], &a[0], x)) {
            walkEntry t = a[n - 1];
            a[n - 1] = a[0];
            a[0] = t;
        }
        if (walkLess(&a[n - 1], &a[mid], x)) {
            walkEntry t = a[n - 1];
            a[n - 1] = a[mid];
            a[mid] = t;
        }
        walkEntry pivot = a[mid];
        size_t i = 0, j = n - 1;
        for (;;) {
            while (walkLess(&a[i], &pivot, x)) i++;
            while (walkLess(&pivot, &a[j], x)) j--;
            if (i >= j) break;
            walkEntry t = a[i];
            a[i] = a[j];
            a[j] = t;
            i++;
            j--;
        }
        /* Recurse into the smaller side, loop on the larger. */
        size_t left = j + 1;
        if (left < n - left) {
            walkSort(a, left, x, depth);
            a += left;
            n -= left;
        } else {
            walkSort(a + left, n - left, x, depth);
            n = left;
        }
    }
    walkInsertionSort(a, n, x);
}

/* Sorts routed entries by leaf id with a stable LSD radix sort (leaf ids are small
 * integers), then each run of one leaf by key. A sparse batch has about one key per
 * leaf, so the runs are short. */
static void walkSortByLeaf(walkEntry *a, walkEntry *tmp, size_t n, const walkSortCtx *x, uint32_t npages) {
    walkEntry *src = a, *dst = tmp;
    unsigned bits = 16 + (npages > 1 ? 64 - __builtin_clzll((uint64_t)npages - 1) : 0);
    for (unsigned shift = 0; shift < bits; shift += 8) {
        size_t count[257] = {0};
        for (size_t i = 0; i < n; i++) count[((src[i].sort >> shift) & 0xff) + 1]++;
        for (int b = 0; b < 256; b++) count[b + 1] += count[b];
        for (size_t i = 0; i < n; i++) dst[count[(src[i].sort >> shift) & 0xff]++] = src[i];
        walkEntry *t = src;
        src = dst;
        dst = t;
    }
    if (src != a) memcpy(a, src, n * sizeof(walkEntry));
    for (size_t i = 0; i < n;) {
        size_t j = i + 1;
        while (j < n && a[j].sort == a[i].sort) j++;
        if (j - i > 1) walkInsertionSort(a + i, j - i, x);
        i = j;
    }
}

/* Routes keys to leaf ids (page << 16 | local): the page fence searched 8 keys in
 * lockstep, then each key's own page fence the same way. */
void slabTreeRouteBatch(const slabTree *t, size_t n, const unsigned char *const *keys, const size_t *klens, uint64_t *out);

/* slabMultiGet with its walk state: order and tmp hold n + 1 entries, hashes n + 1. */
static void multiGet(const slabSnapshot *s, size_t n, const unsigned char *const *keys, const size_t *klens, const unsigned char **vals, size_t *vlens, walkEntry *order, walkEntry *tmp, uint64_t *all_hashes) {
    const slabView *v = s->view;
    for (size_t i = 0; i < n; i++) vals[i] = NULL;
    bool mem = hasMemtableData(v);
    uint64_t *hashes = mem ? all_hashes : NULL;
    size_t open = 0;
    for (size_t i = 0; i < n; i++) {
        if (mem) {
            hashes[i] = slabHash(keys[i], klens[i]);
            int r = probeMemtables(s, keys[i], klens[i], hashes[i], &vals[i], &vlens[i]);
            if (r != SLAB_LOOKUP_MISS) {
                if (r == SLAB_LOOKUP_TOMBSTONE) vals[i] = NULL;
                continue;
            }
        }
        order[open++].index = (uint32_t)i;
    }
    const slabTree *t = v->tree;
    uint64_t ft = v->flushed_through;
    if (open == 0 || t->nleaves == 0) goto done;

    if (open < SLAB_SORT_MIN_KEYS) {
        for (size_t j = 0; j < open; j++) {
            uint32_t i = order[j].index;
            const slabLeaf *l = leafById(t, slabTreeRoute(t, keys[i], klens[i]));
            if (leafMayHaveDelta(l)) {
                uint64_t h = mem ? hashes[i] : slabHash(keys[i], klens[i]);
                int r = probeDelta(l, keys[i], klens[i], h, ft, &vals[i], &vlens[i]);
                if (r != SLAB_LOOKUP_MISS) {
                    if (r == SLAB_LOOKUP_TOMBSTONE) vals[i] = NULL;
                    continue;
                }
            }
            if (!slabBaseGet(l->base, keys[i], klens[i], &vals[i], &vlens[i])) vals[i] = NULL;
        }
        goto done;
    }

    bool sort_once = open >= SLAB_SORT_ONCE_PER_LEAF * t->nleaves;
    walkSortCtx sctx = {keys, klens, 0};
    {
        size_t lcp = klens[order[0].index];
        const unsigned char *first = keys[order[0].index];
        for (size_t j = 1; j < open && lcp; j++) {
            const unsigned char *k = keys[order[j].index];
            size_t kl = klens[order[j].index], m = 0, lim = kl < lcp ? kl : lcp;
            while (m < lim && k[m] == first[m]) m++;
            lcp = m;
        }
        sctx.lcp = lcp;
        for (size_t j = 0; j < open; j++) {
            uint32_t i = order[j].index;
            order[j].head = slabKeyHead(keys[i] + lcp, klens[i] - lcp);
            order[j].sort = 0;
        }
    }
    if (!sort_once) {
        const unsigned char *ck[SLAB_ROUTE_CHUNK];
        size_t cl[SLAB_ROUTE_CHUNK];
        uint64_t ids[SLAB_ROUTE_CHUNK];
        for (size_t j = 0; j < open; j += SLAB_ROUTE_CHUNK) {
            size_t m = open - j < SLAB_ROUTE_CHUNK ? open - j : SLAB_ROUTE_CHUNK;
            for (size_t x = 0; x < m; x++) {
                ck[x] = keys[order[j + x].index];
                cl[x] = klens[order[j + x].index];
            }
            slabTreeRouteBatch(t, m, ck, cl, ids);
            for (size_t x = 0; x < m; x++) order[j + x].sort = ids[x];
        }
    }
    if (sort_once)
        walkSort(order, open, &sctx, 2 * (64 - __builtin_clzll(open)));
    else
        walkSortByLeaf(order, tmp, open, &sctx, t->npages);

    uint64_t ring[SLAB_HASH_RING];
    bool ring_set[SLAB_HASH_RING] = {0};
    uint64_t cur_leaf = UINT64_MAX;
    const slabLeaf *l = NULL;
    slabBaseCursor cursor;
    bool cursor_open = false;
    for (size_t j = 0; j < open; j++) {
        if (!sort_once) {
            /* Staged prefetch: each stage needs the previous stage's line. */
            if (j + SLAB_PREFETCH_BASE < open) {
                const slabLeaf *a = leafById(t, order[j + SLAB_PREFETCH_BASE].sort);
                __builtin_prefetch(a->base, 0, 3);
                if (leafMayHaveDelta(a)) __builtin_prefetch(a->delta, 0, 3);
            }
            if (j + SLAB_PREFETCH_DELTA_HEADER < open) {
                const slabLeaf *a = leafById(t, order[j + SLAB_PREFETCH_DELTA_HEADER].sort);
                if (leafMayHaveDelta(a)) {
                    slabVlog *own = atomic_load_explicit(&a->delta->own, memory_order_relaxed);
                    if (own) __builtin_prefetch(own, 0, 3);
                }
            }
            if (j + SLAB_PREFETCH_BLOCK < open) {
                const slabLeaf *a = leafById(t, order[j + SLAB_PREFETCH_BLOCK].sort);
                slabBasePrefetch(a->base);
                if (leafMayHaveDelta(a)) {
                    slabVlog *own = atomic_load_explicit(&a->delta->own, memory_order_relaxed);
                    if (own) {
                        uint32_t ia = order[j + SLAB_PREFETCH_BLOCK].index;
                        uint64_t h = mem ? hashes[ia] : slabHash(keys[ia], klens[ia]);
                        ring[(j + SLAB_PREFETCH_BLOCK) % SLAB_HASH_RING] = h;
                        ring_set[(j + SLAB_PREFETCH_BLOCK) % SLAB_HASH_RING] = true;
                        slabVlogPrefetch(own, h);
                    }
                }
            }
        }
        uint32_t i = order[j].index;
        bool have_ahead = ring_set[j % SLAB_HASH_RING];
        uint64_t ahead = ring[j % SLAB_HASH_RING];
        ring_set[j % SLAB_HASH_RING] = false;
        uint64_t leaf_id;
        if (sort_once) {
            leaf_id = cur_leaf;
            if (cur_leaf == UINT64_MAX) {
                leaf_id = slabTreeRoute(t, keys[i], klens[i]);
            } else {
                /* Stay while below the leaf's upper bound (the next leaf's lower). */
                uint32_t pg = (uint32_t)(cur_leaf >> 16), lo = (uint32_t)(cur_leaf & 0xffff);
                const slabPage *p = t->pages[pg];
                const unsigned char *up = NULL;
                size_t uplen = 0;
                if (lo + 1 < p->n)
                    up = slabFenceKey(p->fence, lo + 1, &uplen);
                else if (pg + 1 < t->npages)
                    up = slabFenceKey(t->pages[pg + 1]->fence, 0, &uplen);
                if (up && slabKeyCmp(keys[i], klens[i], up, uplen) >= 0) leaf_id = slabTreeRoute(t, keys[i], klens[i]);
            }
        } else {
            leaf_id = order[j].sort;
        }
        if (leaf_id != cur_leaf) {
            cur_leaf = leaf_id;
            l = leafById(t, leaf_id);
            cursor_open = false;
        }
        if (leafMayHaveDelta(l)) {
            uint64_t h = have_ahead ? ahead : (mem ? hashes[i] : slabHash(keys[i], klens[i]));
            int r = probeDelta(l, keys[i], klens[i], h, ft, &vals[i], &vlens[i]);
            if (r != SLAB_LOOKUP_MISS) {
                if (r == SLAB_LOOKUP_TOMBSTONE) vals[i] = NULL;
                continue;
            }
        }
        const slabBase *b = l->base;
        if (klens[i] < b->prefix_len || memcmp(keys[i], b->prefix, b->prefix_len) != 0) continue;
        if (!cursor_open) {
            slabBaseCursorInit(&cursor, b);
            cursor_open = true;
        }
        if (!slabBaseCursorGet(&cursor, keys[i] + b->prefix_len, klens[i] - b->prefix_len, &vals[i], &vlens[i]))
            vals[i] = NULL;
    }
done:
    return;
}

void slabMultiGet(const slabSnapshot *s, size_t n, const unsigned char *const *keys, const size_t *klens, const unsigned char **vals, size_t *vlens) {
    walkEntry order_st[SLAB_WALK_STACK + 1], tmp_st[SLAB_WALK_STACK + 1];
    uint64_t hashes_st[SLAB_WALK_STACK + 1];
    if (n <= SLAB_WALK_STACK) {
        multiGet(s, n, keys, klens, vals, vlens, order_st, tmp_st, hashes_st);
        return;
    }
    walkEntry *order = zmalloc((n + 1) * sizeof(walkEntry)), *tmp = zmalloc((n + 1) * sizeof(walkEntry));
    uint64_t *hashes = zmalloc((n + 1) * sizeof(uint64_t));
    multiGet(s, n, keys, klens, vals, vlens, order, tmp, hashes);
    zfree(order);
    zfree(tmp);
    zfree(hashes);
}

/* ---------------------------------------------------------------------------------------
 * Introspection
 * ------------------------------------------------------------------------------------- */

void slabCollInfo(const slabColl *c, slabInfo *out) {
    const slabView *v = c->view;
    memset(out, 0, sizeof(*out));
    out->entries = slabCollCountEstimate(c);
    out->blocks = v->tree->nleaves;
    out->delta_blocks = v->delta.delta_leaves;
    out->delta_entries = v->delta.entries;
    out->delta_puts = v->delta.puts;
    out->delta_tombstones = v->delta.tombstones;
    out->delta_bytes = v->delta.bytes;
    out->base_bytes = v->tree->info.base_bytes;
    out->snapshot_bytes = v->tree->info.alloc_bytes + v->tree->alloc_bytes + v->delta.bytes;
    out->data_bytes = v->tree->info.base_bytes + v->delta.payload_bytes;
    uint64_t active = memtableEntries(v->active);
    uint64_t draining = 0, bytes = slabMemtableBytes(v->active);
    for (uint32_t i = 0; i < v->nfrozen; i++) {
        draining += memtableEntries(v->frozen[i]);
        bytes += slabMemtableBytes(v->frozen[i]);
    }
    out->pending_writes = active + draining;
    out->pending_writes_draining = draining;
    out->pending_bytes = bytes;
    const slabMemtable *ms = v->active;
    if (ms->log) {
        out->memtable_puts = slabVlogPuts(ms->log);
        out->memtable_tombstones = slabVlogTombstones(ms->log);
    }
    for (uint32_t i = 0; i < v->nfrozen; i++) {
        out->memtable_puts += slabVlogPuts(v->frozen[i]->log);
        out->memtable_tombstones += slabVlogTombstones(v->frozen[i]->log);
    }
    out->frozen_memtables = v->nfrozen;
    out->flush_inflight = atomic_load_explicit(&c->flush_inflight, memory_order_relaxed);
    out->compaction_inflight = atomic_load_explicit(&c->compaction_inflight, memory_order_relaxed);
    out->compaction_requested = atomic_load_explicit(&((slabColl *)c)->compact_pending, memory_order_relaxed);
    out->background_queue_depth = atomic_load_explicit(&c->installs_outstanding, memory_order_relaxed);
}

uint64_t slabCountExact(const slabSnapshot *s) {
    slabIter *it = slabIterCreate(s, NULL, 0, false);
    const unsigned char *k, *v;
    size_t kl, vl;
    uint64_t n = 0;
    while (slabIterNext(it, &k, &kl, &v, &vl)) n++;
    slabIterFree(it);
    return n;
}
