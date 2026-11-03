/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef SLAB_FENCE_H
#define SLAB_FENCE_H

#include <stddef.h>
#include <stdint.h>

/* A fence index routes keys to the leaves (or pages) of a slab tree: given strictly
 * increasing lower bounds, slabFenceRank(key) is the number of bounds <= key, so a key
 * belongs to entry rank - 1.
 *
 * The bounds are stored whole in one buffer, with their longest common prefix and, per
 * bound, a head: the 8 bytes after the common prefix as a big-endian integer. Heads
 * preserve order, so a search is an integer binary search, with full comparisons only
 * among equal heads (usually none). slabFenceRankBatch searches 8 keys in lockstep, one
 * branch-free step per key per level, so the cache misses of different keys overlap. */

#define SLAB_FENCE_LANES 8

typedef struct slabFence {
    uint32_t n;
    uint32_t lcp;
    /* The first bound is the empty key (a tree's first leaf), which would make the
     * common prefix empty and every head tie. It is left out of the prefix, gets head 0
     * and is <= every key. */
    uint32_t skip_first;
    uint64_t pfx[2];      /* the first 16 bytes of the common prefix, big-endian */
    uint64_t pfx_mask[2]; /* which of those bytes belong to it */
    uint64_t *heads;
    uint32_t *offs; /* n + 1 offsets into data */
    unsigned char *data;
    size_t alloc_bytes;
} slabFence;

/* Builds an index over n strictly increasing keys (n may be 0). One allocation. */
slabFence *slabFenceCreate(size_t n, const unsigned char *const *keys, const size_t *lens);
void slabFenceFree(slabFence *f);
size_t slabFenceRank(const slabFence *f, const unsigned char *key, size_t len);
/* Ranks n keys of one index; out[i] = slabFenceRank(keys[i]). */
void slabFenceRankBatch(const slabFence *f, size_t n, const unsigned char *const *keys, const size_t *lens, size_t *out);
/* Ranks up to SLAB_FENCE_LANES keys, each in its own index, in lockstep. */
void slabFenceRankLanes(const slabFence *const *idx, size_t lanes, const unsigned char *const *keys, const size_t *lens, size_t *out);

static inline const unsigned char *slabFenceKey(const slabFence *f, size_t i, size_t *len) {
    *len = f->offs[i + 1] - f->offs[i];
    return f->data + f->offs[i];
}

#endif /* SLAB_FENCE_H */
