/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef SLAB_VLOG_H
#define SLAB_VLOG_H

#ifndef __cplusplus
#include <stdatomic.h>
#endif
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* VersionedLog: a single-writer, append-only hash table of versioned records that any
 * number of threads read concurrently without locks. A slab collection uses it both as
 * its memtable (version = write command sequence number) and as each leaf's delta log
 * (version = id of the newest memtable the appending flush consumed).
 *
 * Records live in arena chunks and never change once published: a 24-byte header
 * (ver, prev, klen, vlen; vlen == UINT32_MAX marks a tombstone) followed by the key and
 * the value, padded to 8 bytes. An overwrite appends a new record whose prev is the old
 * head, so older versions stay reachable for readers with older bounds.
 *
 * A slot is 0 (empty), SLAB_VLOG_DEAD (an aborted insert) or the record address with the
 * top 8 bits of the key's hash in its top byte, so most probes of other keys never touch
 * their record. The table pointer carries log2 of the capacity in its top byte, so one
 * acquire load gives a reader the slots and their count. Live plus dead slots stay at or
 * below half the table; the writer grows (re-hashing live keys into a doubled table)
 * before an insert would cross that, and keeps every superseded table until the log is
 * freed, since a reader may still probe it.
 *
 * Writes happen in batches: slabVlogBegin(version), slabVlogAppend..., then
 * slabVlogCommit or slabVlogAbort. Abort restores every slot the batch changed and the
 * counters. The caller publishes the batch's version (memtable: published_seq; delta
 * log: a View's flushed_through) only after commit.
 *
 * Ordering: a reader must obtain its bound before calling lookup or
 * starting an iteration. Every append at a version <= that bound happens-before the
 * bound's publication; a table swap preceding such an append therefore happens-before
 * the reader's acquire load of the table pointer, so the reader sees a table holding
 * every record at or below its bound. Records are complete before their slot word is
 * stored with release, and never written again.
 *
 * There is exactly one writer per log at a time; the caller guarantees it (the main
 * thread for a memtable, the collection's maintenance unit for a delta log). */

#define SLAB_VLOG_DEAD UINT64_MAX
#define SLAB_VLOG_ADDR_MASK ((UINT64_C(1) << 56) - 1)
#define SLAB_VLOG_TOMBSTONE UINT32_MAX

typedef struct slabVlogRec {
    uint64_t ver;
    struct slabVlogRec *prev;
    uint32_t klen;
    uint32_t vlen;
    unsigned char data[];
} slabVlogRec;

typedef struct slabVlogUndo {
    uint64_t hash;
    uint64_t new_word;
    uint64_t prev_word; /* 0 if the key was new in this log */
} slabVlogUndo;

typedef struct slabVlogChunk {
    struct slabVlogChunk *next;
    size_t cap;
    size_t used;
    unsigned char data[];
} slabVlogChunk;

typedef struct slabVlog {
    /* Read by any thread. */
    _Atomic(uint64_t) index;         /* table address | log2(capacity) << 56 */
    _Atomic(uint32_t) entries;       /* distinct keys */
    _Atomic(uint32_t) puts;          /* keys whose newest version is a value */
    _Atomic(uint32_t) tombstones;    /* keys whose newest version is a tombstone */
    _Atomic(uint64_t) records;       /* committed versions, superseded ones included */
    _Atomic(uint64_t) payload_bytes; /* sum of klen + vlen over committed records */
    _Atomic(uint64_t) bytes;         /* allocator bytes: header, tables, chunks */

    /* Writer only. */
    uint64_t used;        /* live + dead slots in the current table */
    slabVlogChunk *chunk; /* current chunk; earlier ones follow 'next' */
    size_t arena_bytes;
    void **retired; /* superseded tables */
    uint32_t nretired, retired_cap;
    void *coalloc_table; /* the first table when allocated with this header */
    void *table;         /* the current table: 'index' tags its pointer, which heap checkers miss */
    void *coalloc_chunk;
    /* The open batch. */
    bool in_batch;
    uint64_t batch_ver;
    size_t batch_hint_left;
    slabVlogUndo *undo;
    size_t nundo, undo_cap;
    uint32_t snap_entries, snap_puts, snap_tombstones;
    uint64_t snap_records, snap_payload;
} slabVlog;

/* Creates an empty log with at least min_slots table slots (a power of two, >= 4) and,
 * when first_chunk is nonzero, room for that many record bytes, all in one allocation. */
slabVlog *slabVlogCreate(uint64_t min_slots, size_t first_chunk);
void slabVlogFree(slabVlog *log);

/* Bytes one record of these lengths takes in the arena. */
static inline size_t slabVlogRecordBytes(size_t klen, size_t vlen) {
    return (sizeof(slabVlogRec) + klen + vlen + 7) & ~(size_t)7;
}

void slabVlogBegin(slabVlog *log, uint64_t ver, size_t hint_bytes);
/* Appends one record of the open batch. 'val' is ignored for a tombstone. */
void slabVlogAppend(slabVlog *log, const unsigned char *key, size_t klen, uint64_t hash, const unsigned char *val, size_t vlen, bool tombstone);
void slabVlogCommit(slabVlog *log);
void slabVlogAbort(slabVlog *log);

/* Free table slots before the next insert of a new key would grow the table. */
uint64_t slabVlogHeadroom(const slabVlog *log);
uint64_t slabVlogCapacity(const slabVlog *log);

#define SLAB_LOOKUP_MISS 0
#define SLAB_LOOKUP_PUT 1
#define SLAB_LOOKUP_TOMBSTONE 2

/* Newest version of key at or below bound. Any thread. */
int slabVlogLookup(const slabVlog *log, const unsigned char *key, size_t klen, uint64_t hash, uint64_t bound, const unsigned char **val, size_t *vlen);
/* Hint the key's home slot. Never faults; reads the table pointer relaxed and never
 * dereferences what it loads. */
void slabVlogPrefetch(const slabVlog *log, uint64_t hash);

/* Enumeration of the newest version <= bound of every key, in table order. Safe beside
 * the writer only if bound is below every version the writer may be appending. */
typedef struct slabVlogIter {
    const void *slots; /* the table loaded at init (_Atomic uint64_t[cap]) */
    uint64_t cap;
    uint64_t pos;
    uint64_t bound;
} slabVlogIter;

void slabVlogIterInit(slabVlogIter *it, const slabVlog *log, uint64_t bound);
/* Returns the next key's newest record <= bound (a tombstone included), or NULL. */
const slabVlogRec *slabVlogIterNext(slabVlogIter *it);

static inline bool slabVlogRecIsTombstone(const slabVlogRec *r) {
    return r->vlen == SLAB_VLOG_TOMBSTONE;
}

/* Counters, read relaxed: exact for the writer, estimates for other threads. */
uint32_t slabVlogEntries(const slabVlog *log);
uint32_t slabVlogPuts(const slabVlog *log);
uint32_t slabVlogTombstones(const slabVlog *log);
uint64_t slabVlogRecords(const slabVlog *log);
uint64_t slabVlogPayloadBytes(const slabVlog *log);
uint64_t slabVlogBytes(const slabVlog *log);

#endif /* SLAB_VLOG_H */
