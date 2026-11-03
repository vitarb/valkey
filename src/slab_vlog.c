/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* VersionedLog; see slab_vlog.h for the protocol. */

#include "slab_vlog.h"
#include "slab_hash.h"
#include "serverassert.h"
#include "zmalloc.h"

#include <string.h>

#define VLOG_MIN_SLOTS 4
#define VLOG_MIN_CHUNK 64
#define VLOG_MAX_CHUNK (256 * 1024)
/* A record larger than this gets a chunk of its own. */
#define VLOG_OWN_CHUNK_ABOVE (64 * 1024)
/* Undo entries a log keeps allocated between batches. */
#define VLOG_UNDO_KEEP 1024

static inline _Atomic uint64_t *tableSlots(uint64_t ix) {
    return (_Atomic uint64_t *)(uintptr_t)(ix & SLAB_VLOG_ADDR_MASK);
}

static inline uint64_t tableCap(uint64_t ix) {
    return UINT64_C(1) << (ix >> 56);
}

static inline uint64_t makeIndex(void *slots, uint64_t cap) {
    uint64_t addr = (uint64_t)(uintptr_t)slots;
    /* Tagging needs the top byte; heaps that tag pointers there are unsupported. */
    assert((addr & ~SLAB_VLOG_ADDR_MASK) == 0);
    return addr | (uint64_t)__builtin_ctzll(cap) << 56;
}

static inline uint64_t makeWord(const slabVlogRec *r, uint64_t hash) {
    uint64_t addr = (uint64_t)(uintptr_t)r;
    assert((addr & ~SLAB_VLOG_ADDR_MASK) == 0);
    return (hash >> 56) << 56 | addr;
}

static inline slabVlogRec *wordRec(uint64_t w) {
    return (slabVlogRec *)(uintptr_t)(w & SLAB_VLOG_ADDR_MASK);
}

static inline bool recKeyEq(const slabVlogRec *r, const unsigned char *key, size_t klen) {
    return r->klen == klen && memcmp(r->data, key, klen) == 0;
}

static inline void addBytes(slabVlog *log, uint64_t n) {
    atomic_store_explicit(&log->bytes, atomic_load_explicit(&log->bytes, memory_order_relaxed) + n,
                          memory_order_relaxed);
}

static uint64_t nextPow2(uint64_t v) {
    uint64_t c = VLOG_MIN_SLOTS;
    while (c < v) c <<= 1;
    return c;
}

slabVlog *slabVlogCreate(uint64_t min_slots, size_t first_chunk) {
    uint64_t cap = nextPow2(min_slots);
    size_t header = (sizeof(slabVlog) + 7) & ~(size_t)7;
    size_t table = cap * sizeof(uint64_t);
    size_t chunk = first_chunk ? sizeof(slabVlogChunk) + ((first_chunk + 7) & ~(size_t)7) : 0;
    size_t usable;
    char *mem = zmalloc_usable(header + table + chunk, &usable);
    slabVlog *log = (slabVlog *)mem;
    memset(log, 0, sizeof(*log));
    void *slots = mem + header;
    memset(slots, 0, table);
    log->coalloc_table = slots;
    log->table = slots;
    atomic_init(&log->index, makeIndex(slots, cap));
    if (chunk) {
        slabVlogChunk *c = (slabVlogChunk *)(mem + header + table);
        c->next = NULL;
        c->cap = usable - header - table - sizeof(slabVlogChunk);
        c->used = 0;
        log->chunk = c;
        log->coalloc_chunk = c;
        log->arena_bytes = c->cap;
    }
    atomic_init(&log->bytes, usable);
    return log;
}

void slabVlogFree(slabVlog *log) {
    if (!log) return;
    slabVlogChunk *c = log->chunk;
    while (c) {
        slabVlogChunk *next = c->next;
        if (c != log->coalloc_chunk) zfree(c);
        c = next;
    }
    for (uint32_t i = 0; i < log->nretired; i++)
        if (log->retired[i] != log->coalloc_table) zfree(log->retired[i]);
    if (log->table != log->coalloc_table) zfree(log->table);
    zfree(log->retired);
    zfree(log->undo);
    zfree(log);
}

/* Arena space for one record. Chunks never move, so record addresses are stable. */
static slabVlogRec *arenaAlloc(slabVlog *log, size_t n) {
    slabVlogChunk *c = log->chunk;
    if (n <= VLOG_OWN_CHUNK_ABOVE && c && c->cap - c->used >= n) {
        void *p = c->data + c->used;
        c->used += n;
        log->batch_hint_left = log->batch_hint_left > n ? log->batch_hint_left - n : 0;
        return p;
    }
    size_t want;
    if (n > VLOG_OWN_CHUNK_ABOVE) {
        want = n;
    } else {
        /* The rest of the batch, or a quarter of the arena so far if that is more,
         * within [64 B, 256 KiB]: a log appended once per flush gets batch-sized chunks,
         * and unhinted growth keeps O(log n) chunks with at most 25 % unused. */
        want = log->batch_hint_left > log->arena_bytes / 4 ? log->batch_hint_left : log->arena_bytes / 4;
        if (want < VLOG_MIN_CHUNK) want = VLOG_MIN_CHUNK;
        if (want > VLOG_MAX_CHUNK) want = VLOG_MAX_CHUNK;
        if (want < n) want = n;
    }
    size_t usable;
    slabVlogChunk *nc = zmalloc_usable(sizeof(slabVlogChunk) + want, &usable);
    assert(((uint64_t)(uintptr_t)nc & ~SLAB_VLOG_ADDR_MASK) == 0);
    nc->cap = usable - sizeof(slabVlogChunk);
    nc->used = n;
    if (n > VLOG_OWN_CHUNK_ABOVE && c) {
        /* Keep filling the current chunk. */
        nc->next = c->next;
        c->next = nc;
    } else {
        nc->next = c;
        log->chunk = nc;
    }
    log->arena_bytes += nc->cap;
    addBytes(log, usable);
    log->batch_hint_left = log->batch_hint_left > n ? log->batch_hint_left - n : 0;
    return (slabVlogRec *)nc->data;
}

static void retireTable(slabVlog *log, void *slots) {
    if (log->nretired == log->retired_cap) {
        log->retired_cap = log->retired_cap ? log->retired_cap * 2 : 4;
        log->retired = zrealloc(log->retired, log->retired_cap * sizeof(void *));
    }
    log->retired[log->nretired++] = slots;
}

/* Doubles the table: re-inserts every live slot word at the home of its key's hash,
 * dropping DEAD slots, then publishes the new table with release. The old table is
 * never written again and lives until the log is freed. */
static void grow(slabVlog *log) {
    uint64_t ix = atomic_load_explicit(&log->index, memory_order_relaxed);
    _Atomic uint64_t *old = tableSlots(ix);
    uint64_t cap = tableCap(ix), ncap = cap * 2, nmask = ncap - 1;
    size_t usable;
    _Atomic uint64_t *slots = zcalloc_usable(ncap * sizeof(uint64_t), &usable);
    uint64_t live = 0;
    for (uint64_t i = 0; i < cap; i++) {
        uint64_t w = atomic_load_explicit(&old[i], memory_order_relaxed);
        if (w == 0 || w == SLAB_VLOG_DEAD) continue;
        const slabVlogRec *r = wordRec(w);
        uint64_t j = slabHash(r->data, r->klen) & nmask;
        while (atomic_load_explicit(&slots[j], memory_order_relaxed)) j = (j + 1) & nmask;
        atomic_store_explicit(&slots[j], w, memory_order_relaxed);
        live++;
    }
    /* Pairs with the acquire load of 'index' in lookups and iterations. */
    atomic_store_explicit(&log->index, makeIndex((void *)slots, ncap), memory_order_release);
    retireTable(log, (void *)old);
    log->table = (void *)slots;
    log->used = live;
    addBytes(log, usable);
}

void slabVlogBegin(slabVlog *log, uint64_t ver, size_t hint_bytes) {
    assert(!log->in_batch);
    log->in_batch = true;
    log->batch_ver = ver;
    log->batch_hint_left = hint_bytes;
    log->nundo = 0;
    log->snap_entries = atomic_load_explicit(&log->entries, memory_order_relaxed);
    log->snap_puts = atomic_load_explicit(&log->puts, memory_order_relaxed);
    log->snap_tombstones = atomic_load_explicit(&log->tombstones, memory_order_relaxed);
    log->snap_records = atomic_load_explicit(&log->records, memory_order_relaxed);
    log->snap_payload = atomic_load_explicit(&log->payload_bytes, memory_order_relaxed);
}

static void pushUndo(slabVlog *log, uint64_t hash, uint64_t new_word, uint64_t prev_word) {
    if (log->nundo == log->undo_cap) {
        log->undo_cap = log->undo_cap ? log->undo_cap * 2 : 16;
        log->undo = zrealloc(log->undo, log->undo_cap * sizeof(slabVlogUndo));
    }
    log->undo[log->nundo++] = (slabVlogUndo){hash, new_word, prev_word};
}

#define RELAXED_ADD(field, delta) \
    atomic_store_explicit(&(field), atomic_load_explicit(&(field), memory_order_relaxed) + (delta), memory_order_relaxed)

void slabVlogAppend(slabVlog *log, const unsigned char *key, size_t klen, uint64_t hash, const unsigned char *val, size_t vlen, bool tombstone) {
    assert(log->in_batch && klen < SLAB_VLOG_TOMBSTONE && vlen < SLAB_VLOG_TOMBSTONE);
    if (tombstone) vlen = 0;
    slabVlogRec *r = arenaAlloc(log, slabVlogRecordBytes(klen, vlen));
    r->ver = log->batch_ver;
    r->klen = (uint32_t)klen;
    r->vlen = tombstone ? SLAB_VLOG_TOMBSTONE : (uint32_t)vlen;
    memcpy(r->data, key, klen);
    if (vlen) memcpy(r->data + klen, val, vlen);
    uint64_t word = makeWord(r, hash), tag = hash >> 56;

    uint64_t ix = atomic_load_explicit(&log->index, memory_order_relaxed);
    _Atomic uint64_t *slots = tableSlots(ix);
    uint64_t mask = tableCap(ix) - 1, i = hash & mask;
    for (;;) {
        uint64_t w = atomic_load_explicit(&slots[i], memory_order_relaxed);
        if (w == 0) {
            if ((log->used + 1) * 2 > mask + 1) {
                grow(log);
                ix = atomic_load_explicit(&log->index, memory_order_relaxed);
                slots = tableSlots(ix);
                mask = tableCap(ix) - 1;
                i = hash & mask;
                while (atomic_load_explicit(&slots[i], memory_order_relaxed)) i = (i + 1) & mask;
            }
            r->prev = NULL;
            /* Release: the record is complete before a reader can reach it. */
            atomic_store_explicit(&slots[i], word, memory_order_release);
            log->used++;
            pushUndo(log, hash, word, 0);
            RELAXED_ADD(log->entries, 1);
            if (tombstone)
                RELAXED_ADD(log->tombstones, 1);
            else
                RELAXED_ADD(log->puts, 1);
            break;
        }
        if (w != SLAB_VLOG_DEAD && (w >> 56) == tag && recKeyEq(wordRec(w), key, klen)) {
            slabVlogRec *old = wordRec(w);
            r->prev = old;
            atomic_store_explicit(&slots[i], word, memory_order_release);
            pushUndo(log, hash, word, w);
            bool was_tomb = slabVlogRecIsTombstone(old);
            if (was_tomb != tombstone) {
                RELAXED_ADD(log->tombstones, tombstone ? 1 : -1);
                RELAXED_ADD(log->puts, tombstone ? -1 : 1);
            }
            break;
        }
        i = (i + 1) & mask;
    }
    RELAXED_ADD(log->records, 1);
    RELAXED_ADD(log->payload_bytes, klen + vlen);
}

void slabVlogCommit(slabVlog *log) {
    assert(log->in_batch);
    log->in_batch = false;
    log->nundo = 0;
    /* A very large batch (an RDB's pending entries) does not keep its undo log. */
    if (log->undo_cap > VLOG_UNDO_KEEP) {
        zfree(log->undo);
        log->undo = NULL;
        log->undo_cap = 0;
    }
}

/* Restores every slot the batch changed, newest first. A slot is found by its word,
 * not its key: a key written twice in the batch must be restored to its first new
 * word before that one is undone, and a mid-batch grow moved every word. A key that
 * was new becomes DEAD rather than empty, which keeps the probe chains of keys placed
 * after it intact. */
void slabVlogAbort(slabVlog *log) {
    assert(log->in_batch);
    uint64_t ix = atomic_load_explicit(&log->index, memory_order_relaxed);
    _Atomic uint64_t *slots = tableSlots(ix);
    uint64_t mask = tableCap(ix) - 1;
    for (size_t u = log->nundo; u-- > 0;) {
        slabVlogUndo *e = &log->undo[u];
        uint64_t i = e->hash & mask;
        while (atomic_load_explicit(&slots[i], memory_order_relaxed) != e->new_word) i = (i + 1) & mask;
        atomic_store_explicit(&slots[i], e->prev_word ? e->prev_word : SLAB_VLOG_DEAD, memory_order_release);
    }
    atomic_store_explicit(&log->entries, log->snap_entries, memory_order_relaxed);
    atomic_store_explicit(&log->puts, log->snap_puts, memory_order_relaxed);
    atomic_store_explicit(&log->tombstones, log->snap_tombstones, memory_order_relaxed);
    atomic_store_explicit(&log->records, log->snap_records, memory_order_relaxed);
    atomic_store_explicit(&log->payload_bytes, log->snap_payload, memory_order_relaxed);
    log->in_batch = false;
    log->nundo = 0;
}

uint64_t slabVlogCapacity(const slabVlog *log) {
    return tableCap(atomic_load_explicit(&log->index, memory_order_relaxed));
}

uint64_t slabVlogHeadroom(const slabVlog *log) {
    uint64_t half = slabVlogCapacity(log) / 2;
    return half > log->used ? half - log->used : 0;
}

int slabVlogLookup(const slabVlog *log, const unsigned char *key, size_t klen, uint64_t hash, uint64_t bound, const unsigned char **val, size_t *vlen) {
    /* Acquire: pairs with the release in grow(). The caller took its bound first. */
    uint64_t ix = atomic_load_explicit(&log->index, memory_order_acquire);
    const _Atomic uint64_t *slots = tableSlots(ix);
    uint64_t mask = tableCap(ix) - 1, i = hash & mask, tag = hash >> 56;
    for (;;) {
        /* Acquire: pairs with the release store that published the slot's record. */
        uint64_t w = atomic_load_explicit(&slots[i], memory_order_acquire);
        if (w == 0) return SLAB_LOOKUP_MISS;
        if (w != SLAB_VLOG_DEAD && (w >> 56) == tag) {
            const slabVlogRec *r = wordRec(w);
            if (recKeyEq(r, key, klen)) {
                while (r->ver > bound) {
                    r = r->prev;
                    if (!r) return SLAB_LOOKUP_MISS;
                }
                if (slabVlogRecIsTombstone(r)) return SLAB_LOOKUP_TOMBSTONE;
                *val = r->data + r->klen;
                *vlen = r->vlen;
                return SLAB_LOOKUP_PUT;
            }
        }
        i = (i + 1) & mask;
    }
}

void slabVlogPrefetch(const slabVlog *log, uint64_t hash) {
    uint64_t ix = atomic_load_explicit(&log->index, memory_order_relaxed);
    __builtin_prefetch((const void *)(tableSlots(ix) + (hash & (tableCap(ix) - 1))), 0, 3);
}

void slabVlogIterInit(slabVlogIter *it, const slabVlog *log, uint64_t bound) {
    uint64_t ix = atomic_load_explicit(&log->index, memory_order_acquire);
    it->slots = (const void *)tableSlots(ix);
    it->cap = tableCap(ix);
    it->pos = 0;
    it->bound = bound;
}

const slabVlogRec *slabVlogIterNext(slabVlogIter *it) {
    while (it->pos < it->cap) {
        const _Atomic uint64_t *slots = it->slots;
        uint64_t w = atomic_load_explicit(&slots[it->pos++], memory_order_acquire);
        if (w == 0 || w == SLAB_VLOG_DEAD) continue;
        const slabVlogRec *r = wordRec(w);
        while (r && r->ver > it->bound) r = r->prev;
        if (r) return r;
    }
    return NULL;
}

uint32_t slabVlogEntries(const slabVlog *log) {
    return atomic_load_explicit(&log->entries, memory_order_relaxed);
}

uint32_t slabVlogPuts(const slabVlog *log) {
    return atomic_load_explicit(&log->puts, memory_order_relaxed);
}

uint32_t slabVlogTombstones(const slabVlog *log) {
    return atomic_load_explicit(&log->tombstones, memory_order_relaxed);
}

uint64_t slabVlogRecords(const slabVlog *log) {
    return atomic_load_explicit(&log->records, memory_order_relaxed);
}

uint64_t slabVlogPayloadBytes(const slabVlog *log) {
    return atomic_load_explicit(&log->payload_bytes, memory_order_relaxed);
}

uint64_t slabVlogBytes(const slabVlog *log) {
    return atomic_load_explicit(&log->bytes, memory_order_relaxed);
}
