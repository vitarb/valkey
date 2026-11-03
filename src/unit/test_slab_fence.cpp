/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <stdio.h>
#include <string.h>

extern "C" {
#include "slab_block.h"
#include "slab_fence.h"
#include "zmalloc.h"
}

static uint64_t rngNext(uint64_t *s) {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    return *s;
}

/* The number of bounds <= key, by linear scan. */
static size_t linearRank(char **keys, size_t n, const char *k, size_t kl) {
    size_t r = 0;
    while (r < n && slabKeyCmp((unsigned char *)keys[r], strlen(keys[r]), (const unsigned char *)k, kl) <= 0) r++;
    return r;
}

static int cmpStr(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}

/* Bounds that share long prefixes and tie on their first 8 bytes after them. */
static size_t makeBounds(uint64_t *rng, size_t n, char ***out) {
    char **k = (char **)zmalloc(n * sizeof(char *));
    for (size_t i = 0; i < n; i++) {
        k[i] = (char *)zmalloc(64);
        switch (rngNext(rng) % 3) {
        case 0: snprintf(k[i], 64, "src:1:entity_%08llu", (unsigned long long)(rngNext(rng) % 100000)); break;
        case 1: snprintf(k[i], 64, "src:1:entity_00001234%llu", (unsigned long long)(rngNext(rng) % 1000)); break;
        default: snprintf(k[i], 64, "src:%llu", (unsigned long long)(rngNext(rng) % 50)); break;
        }
    }
    qsort(k, n, sizeof(char *), cmpStr);
    size_t m = 0;
    for (size_t i = 0; i < n; i++) {
        if (m && strcmp(k[m - 1], k[i]) == 0) {
            zfree(k[i]);
            continue;
        }
        k[m++] = k[i];
    }
    *out = k;
    return m;
}

static slabFence *fenceOf(char **keys, size_t n) {
    const unsigned char **kp = (const unsigned char **)zmalloc((n + 1) * sizeof(char *));
    size_t *kl = (size_t *)zmalloc((n + 1) * sizeof(size_t));
    for (size_t i = 0; i < n; i++) {
        kp[i] = (const unsigned char *)keys[i];
        kl[i] = strlen(keys[i]);
    }
    slabFence *f = slabFenceCreate(n, kp, kl);
    zfree(kp);
    zfree(kl);
    return f;
}

TEST(SlabFence, RankMatchesLinearScan) {
    uint64_t rng = 99;
    for (int round = 0; round < 30; round++) {
        char **bounds;
        size_t n = makeBounds(&rng, 1 + rngNext(&rng) % 600, &bounds);
        slabFence *f = fenceOf(bounds, n);
        char **probes;
        size_t np = makeBounds(&rng, 500, &probes);
        const unsigned char **pk = (const unsigned char **)zmalloc(np * sizeof(char *));
        size_t *pl = (size_t *)zmalloc(np * sizeof(size_t)), *out = (size_t *)zmalloc(np * sizeof(size_t));
        for (size_t i = 0; i < np; i++) {
            pk[i] = (const unsigned char *)probes[i];
            pl[i] = strlen(probes[i]);
            ASSERT_EQ(slabFenceRank(f, pk[i], pl[i]), linearRank(bounds, n, probes[i], pl[i])) << probes[i];
        }
        slabFenceRankBatch(f, np, pk, pl, out);
        for (size_t i = 0; i < np; i++) ASSERT_EQ(out[i], linearRank(bounds, n, probes[i], pl[i]));
        /* Lanes: each probe against the same index, and against an empty one. */
        slabFence *empty = slabFenceCreate(0, NULL, NULL);
        for (size_t i = 0; i + SLAB_FENCE_LANES <= np; i += SLAB_FENCE_LANES) {
            const slabFence *idx[SLAB_FENCE_LANES];
            for (int l = 0; l < SLAB_FENCE_LANES; l++) idx[l] = l == 3 ? empty : f;
            slabFenceRankLanes(idx, SLAB_FENCE_LANES, pk + i, pl + i, out);
            for (int l = 0; l < SLAB_FENCE_LANES; l++)
                ASSERT_EQ(out[l], l == 3 ? 0 : linearRank(bounds, n, probes[i + l], pl[i + l]));
        }
        /* Every bound itself, and the empty key. */
        for (size_t i = 0; i < n; i++)
            ASSERT_EQ(slabFenceRank(f, (unsigned char *)bounds[i], strlen(bounds[i])), i + 1);
        ASSERT_EQ(slabFenceRank(f, (const unsigned char *)"", 0), linearRank(bounds, n, "", 0));
        slabFenceFree(empty);
        slabFenceFree(f);
        zfree(pk);
        zfree(pl);
        zfree(out);
        for (size_t i = 0; i < n; i++) zfree(bounds[i]);
        for (size_t i = 0; i < np; i++) zfree(probes[i]);
        zfree(bounds);
        zfree(probes);
    }
}

TEST(SlabFence, EmptyFirstBound) {
    /* A tree's first leaf has the empty lower bound, so the common prefix is empty. */
    char *keys[] = {(char *)"", (char *)"b", (char *)"d"};
    slabFence *f = fenceOf(keys, 3);
    ASSERT_EQ(f->lcp, 0u);
    ASSERT_EQ(slabFenceRank(f, (const unsigned char *)"", 0), 1u);
    ASSERT_EQ(slabFenceRank(f, (const unsigned char *)"a", 1), 1u);
    ASSERT_EQ(slabFenceRank(f, (const unsigned char *)"b", 1), 2u);
    ASSERT_EQ(slabFenceRank(f, (const unsigned char *)"c", 1), 2u);
    ASSERT_EQ(slabFenceRank(f, (const unsigned char *)"zz", 2), 3u);
    slabFenceFree(f);
}
