/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef SLAB_BLOCK_H
#define SLAB_BLOCK_H

#ifndef __cplusplus
#include <stdatomic.h>
#endif
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Immutable blocks of a slab collection.
 *
 * A base block stores a leaf's sorted entries in one buffer that is persisted as is.
 * Keys share a block prefix that is stored once; the rest of each key (its suffix)
 * is delta-encoded against the previous suffix, except at every key_restart-th entry
 * (a restart), which stores its whole suffix. Values are concatenated, with varint
 * lengths in a separate section and the value offset of every value_restart-th entry
 * in a table, so a value is at most value_restart - 1 lengths away.
 *
 * Layout (little-endian, header 40 bytes):
 *
 *   0  u32 magic "SLB1"     4  u32 entries       8  u16 key_restart  10 u16 value_restart
 *   12 u32 prefix_len      16 u32 kdat_len      20 u32 vblb_len     24 u32 vlen_len
 *   28 u32 reserved (0)    32 u64 total_len
 *   40 BPRF[prefix_len] KDAT[kdat_len] pad4 KRST[u32 x ceil(entries/key_restart)]
 *      VBLB[vblb_len] VLEN[vlen_len] pad4 VORS[u32 x ceil(entries/value_restart)]
 *
 * A KDAT restart entry is varint(len) + suffix; any other entry is varint(shared) +
 * varint(unshared) + the unshared tail. KRST holds the KDAT offset of each restart,
 * VORS the VBLB offset of each value group's first value.
 *
 * Every loaded block is validated completely (slabBaseLoad): sections in bounds,
 * canonical varints, restart tables matching the data, keys strictly increasing and
 * each shared length maximal. Lookups compare keys in their encoded form and rely on
 * the last two properties. */

#define SLAB_MAX_KEY_BYTES (64 * 1024)
#define SLAB_MAX_VALUE_BYTES (64 * 1024 * 1024)
#define SLAB_MAX_BLOCK_BYTES ((size_t)128 * 1024 * 1024)
#define SLAB_MAX_RESTART 1024
#define SLAB_BASE_HEADER_BYTES 40

typedef struct slabBase {
    _Atomic(uint32_t) refcount; /* leaves sharing this block (copy-on-write pages, COPY) */
    const unsigned char *buf;   /* the block bytes, owned (zmalloc) */
    size_t len;
    uint32_t entries;
    uint32_t key_restart;
    uint32_t value_restart;
    uint32_t restarts; /* key restart groups */
    uint32_t vgroups;  /* value restart groups */
    uint32_t prefix_len;
    uint32_t kdat_len;
    uint32_t vblb_len;
    uint32_t vlen_len;
    const unsigned char *prefix;
    const unsigned char *kdat;
    const unsigned char *krst; /* u32 LE per restart */
    const unsigned char *vblb;
    const unsigned char *vlen;
    const unsigned char *vors; /* u32 LE per value group */
    uint32_t *vlen_group_off;  /* derived: VLEN offset of each value group */
    uint64_t *rpfx;            /* derived: first 8 suffix bytes of each restart key, big-endian */
    size_t alloc_bytes;        /* allocator bytes of the buffer and this struct */
} slabBase;

/* Validates 'buf' (len bytes, allocated with zmalloc) as a base block. On success the
 * returned block owns 'buf'. On failure returns NULL, sets *err to a static message and
 * leaves 'buf' to the caller. */
slabBase *slabBaseLoad(unsigned char *buf, size_t len, const char **err);
void slabBaseFree(slabBase *b);
/* A copy of a validated block in new allocations (active defrag), without validating
 * it again. */
slabBase *slabBaseMove(const slabBase *b);

/* Point lookup of a full key. Returns true and sets the value if found. */
bool slabBaseGet(const slabBase *b, const unsigned char *key, size_t klen, const unsigned char **val, size_t *vlen);

/* Hint the cache lines a lookup in this block reads first. Never faults. */
void slabBasePrefetch(const slabBase *b);

/* Forward-only cursor that answers a non-decreasing sequence of suffix targets in one
 * block. It remembers where the previous target stopped and how much of it matched,
 * gallops over restart groups (1, 2, 4, ... groups ahead) comparing the 8-byte rpfx
 * heads as integers, then scans inside one group comparing in encoded form. Dense
 * targets cost a linear decode, sparse ones a shrinking seek, with no threshold.
 * Targets may also go backwards; the cursor then seeks from scratch. */
typedef struct slabBaseCursor {
    const slabBase *b;
    uint32_t group;
    bool positioned;
    /* the current entry */
    uint32_t index;
    uint32_t shared;           /* the entry's shared length (0 at a restart) */
    const unsigned char *tail; /* its unshared bytes */
    uint32_t tail_len;
    uint32_t next_kdat; /* KDAT offset of the next entry */
    uint32_t next_vlen; /* VLEN offset of the next entry's length */
    uint32_t val_off;
    uint32_t val_len;
    uint32_t common;           /* bytes the entry's suffix shares with the last target */
    int order;                 /* the entry's suffix compared with the last target */
    const unsigned char *last; /* the last target suffix (borrowed until the next call) */
    size_t last_len;
} slabBaseCursor;

void slabBaseCursorInit(slabBaseCursor *c, const slabBase *b);
/* Looks up a key suffix (the full key minus the block prefix). */
bool slabBaseCursorGet(slabBaseCursor *c, const unsigned char *suffix, size_t slen, const unsigned char **val, size_t *vlen);

/* Sequential iteration over every entry in key order. The key buffer is owned by the
 * iterator and valid until the next call. */
typedef struct slabBaseIter {
    const slabBase *b;
    uint32_t index;
    uint32_t kdat_off;
    uint32_t vlen_off;
    uint32_t vblb_off;
    unsigned char *key;
    size_t key_len;
    size_t key_cap;
    bool pending; /* the decoded entry is returned by the next call (after a seek) */
    const unsigned char *pending_val;
    size_t pending_vlen;
} slabBaseIter;

void slabBaseIterInit(slabBaseIter *it, const slabBase *b);
/* Positions the iterator on the first entry whose key is >= 'key'. */
void slabBaseIterSeek(slabBaseIter *it, const unsigned char *key, size_t klen);
bool slabBaseIterNext(slabBaseIter *it, const unsigned char **key, size_t *klen, const unsigned char **val, size_t *vlen);
void slabBaseIterRelease(slabBaseIter *it);

/* The first and last key of a non-empty block. The returned buffers are zmalloc'd. */
void slabBaseKeyBounds(const slabBase *b, unsigned char **first, size_t *first_len, unsigned char **last, size_t *last_len);

/* Encoder of one base block from entries added in strictly increasing key order. It
 * keeps the exact encoded size as entries arrive, so a packer can close a block at a
 * size class without encoding it. */
typedef struct slabBaseBuilder {
    uint32_t key_restart;
    uint32_t value_restart;
    uint32_t entries;
    /* entries, kept until the block prefix is known */
    unsigned char *keys;
    size_t keys_len, keys_cap;
    uint32_t *key_off; /* entries + 1 offsets into keys */
    size_t key_off_cap;
    unsigned char *vals;
    size_t vals_len, vals_cap;
    uint32_t *val_off; /* entries + 1 offsets into vals */
    /* size accounting (see slabBaseBuilderBytes) */
    size_t prefix;
    size_t key_bytes;
    size_t restart_key_bytes;
    size_t key_restarts;
    size_t value_bytes;
    size_t value_restarts;
} slabBaseBuilder;

void slabBaseBuilderInit(slabBaseBuilder *bb, uint32_t key_restart, uint32_t value_restart);
void slabBaseBuilderReset(slabBaseBuilder *bb);
void slabBaseBuilderRelease(slabBaseBuilder *bb);
/* The encoded size if an entry of these lengths were added, sharing 'shared' bytes with
 * the last key (an upper bound; exact unless a length varint crosses 128 bytes only
 * after the prefix is taken off). */
size_t slabBaseBuilderBytesWith(const slabBaseBuilder *bb, size_t klen, size_t vlen, size_t shared);
size_t slabBaseBuilderBytes(const slabBaseBuilder *bb);
/* Bytes 'key' shares with the last key added (0 if empty). */
size_t slabBaseBuilderShared(const slabBaseBuilder *bb, const unsigned char *key, size_t klen);
void slabBaseBuilderAdd(slabBaseBuilder *bb, const unsigned char *key, size_t klen, const unsigned char *val, size_t vlen);
/* Encodes the entries into a new block, validates it and resets the builder. */
slabBase *slabBaseBuilderFinish(slabBaseBuilder *bb);
/* Encodes an arbitrary sorted list in one call (tests, small leaves). */
slabBase *slabBaseBuild(uint32_t key_restart, uint32_t value_restart, size_t n, const unsigned char *const *keys, const size_t *klens, const unsigned char *const *vals, const size_t *vlens);

/* An immutable delta segment: a leaf's delta records as persisted (RDB, DUMP, copy).
 *
 * Persisted form: u32 magic "SLD1", u32 entries, then per record in strictly
 * increasing key order u32 klen, u32 vlen (UINT32_MAX = tombstone), key, value. The
 * hash table that answers lookups is built at load; it is not persisted, so the
 * format does not depend on the hash function. */
typedef struct slabDeltaSeg {
    _Atomic(uint32_t) refcount;
    const unsigned char *buf; /* owned (zmalloc) */
    size_t len;
    uint32_t entries;
    uint32_t puts;
    uint32_t tombstones;
    uint64_t mask;
    uint64_t *slots; /* 0 = empty, else (hash >> 32) << 32 | (record offset + 1) */
    uint64_t payload_bytes;
    size_t alloc_bytes;
} slabDeltaSeg;

#define SLAB_SEG_MISS 0
#define SLAB_SEG_PUT 1
#define SLAB_SEG_TOMBSTONE 2

slabDeltaSeg *slabDeltaSegLoad(unsigned char *buf, size_t len, const char **err);
void slabDeltaSegFree(slabDeltaSeg *s);
int slabDeltaSegLookup(const slabDeltaSeg *s, const unsigned char *key, size_t klen, uint64_t hash, const unsigned char **val, size_t *vlen);
/* Iteration in key order. 'pos' starts at 0; returns false at the end. A tombstone has
 * *val == NULL. */
bool slabDeltaSegNext(const slabDeltaSeg *s, size_t *pos, const unsigned char **key, size_t *klen, const unsigned char **val, size_t *vlen);

/* Persisted delta segment encoder (records added in strictly increasing key order). */
typedef struct slabSegWriter {
    unsigned char *buf;
    size_t len, cap;
    uint32_t entries;
} slabSegWriter;

void slabSegWriterInit(slabSegWriter *w);
void slabSegWriterAdd(slabSegWriter *w, const unsigned char *key, size_t klen, const unsigned char *val, size_t vlen, bool tombstone);
/* Returns the encoded buffer (zmalloc'd) and its length; resets the writer. */
unsigned char *slabSegWriterFinish(slabSegWriter *w, size_t *len);

/* Varint (canonical LEB128, at most 5 bytes for a u32). */
size_t slabVarintLen(uint32_t v);

/* The largest allocator size class not above 'bytes' (blocks close at it, so a block
 * fills its allocation). */
size_t slabSizeClassFloor(size_t bytes);

/* Head of a key suffix: its first 8 bytes as a big-endian integer, zero-padded. */
static inline uint64_t slabKeyHead(const unsigned char *s, size_t len) {
    uint64_t h = 0;
    if (len >= 8) {
        return (uint64_t)s[0] << 56 | (uint64_t)s[1] << 48 | (uint64_t)s[2] << 40 | (uint64_t)s[3] << 32 |
               (uint64_t)s[4] << 24 | (uint64_t)s[5] << 16 | (uint64_t)s[6] << 8 | (uint64_t)s[7];
    }
    for (size_t i = 0; i < len; i++) h |= (uint64_t)s[i] << (56 - 8 * i);
    return h;
}

/* memcmp-style comparison of two byte strings, shorter-is-smaller on ties. */
static inline int slabKeyCmp(const unsigned char *a, size_t alen, const unsigned char *b, size_t blen) {
    size_t n = alen < blen ? alen : blen;
    int c = n ? memcmp(a, b, n) : 0;
    if (c) return c;
    return (alen > blen) - (alen < blen);
}

#endif /* SLAB_BLOCK_H */
