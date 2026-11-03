/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "slab_fence.h"
#include "slab_block.h"
#include "serverassert.h"
#include "zmalloc.h"

#include <string.h>

static uint64_t prefixWordMask(size_t n) {
    if (n == 0) return 0;
    if (n >= 8) return UINT64_MAX;
    return ~(UINT64_MAX >> (8 * n));
}

slabFence *slabFenceCreate(size_t n, const unsigned char *const *keys, const size_t *lens) {
    size_t total = 0, lcp = 0;
    size_t skip = n > 1 && lens[0] == 0;
    for (size_t i = 0; i < n; i++) {
        total += lens[i];
        if (i > 0) assert(slabKeyCmp(keys[i - 1], lens[i - 1], keys[i], lens[i]) < 0);
        if (i < skip) continue;
        if (i == skip) {
            lcp = lens[i];
        } else {
            if (lens[i] < lcp) lcp = lens[i];
            size_t j = 0;
            while (j < lcp && keys[skip][j] == keys[i][j]) j++;
            lcp = j;
        }
    }
    assert(total <= UINT32_MAX);
    size_t heads_bytes = n * sizeof(uint64_t), offs_bytes = (n + 1) * sizeof(uint32_t);
    size_t usable;
    slabFence *f = zmalloc_usable(sizeof(*f) + heads_bytes + offs_bytes + total, &usable);
    memset(f, 0, sizeof(*f));
    f->n = (uint32_t)n;
    f->lcp = (uint32_t)lcp;
    f->skip_first = (uint32_t)skip;
    f->heads = (uint64_t *)(f + 1);
    f->offs = (uint32_t *)((char *)f->heads + heads_bytes);
    f->data = (unsigned char *)f->offs + offs_bytes;
    size_t off = 0;
    for (size_t i = 0; i < n; i++) {
        f->offs[i] = (uint32_t)off;
        memcpy(f->data + off, keys[i], lens[i]);
        off += lens[i];
        f->heads[i] = i < skip ? 0 : slabKeyHead(keys[i] + lcp, lens[i] - lcp);
    }
    f->offs[n] = (uint32_t)off;
    unsigned char prefix[16] = {0};
    memcpy(prefix, f->data + f->offs[skip < n ? skip : 0], lcp < 16 ? lcp : 16);
    f->pfx[0] = slabKeyHead(prefix, 8);
    f->pfx[1] = slabKeyHead(prefix + 8, 8);
    f->pfx_mask[0] = prefixWordMask(lcp < 8 ? lcp : 8);
    f->pfx_mask[1] = prefixWordMask(lcp > 8 ? (lcp - 8 < 8 ? lcp - 8 : 8) : 0);
    f->alloc_bytes = usable;
    return f;
}

void slabFenceFree(slabFence *f) {
    zfree(f);
}

/* Whether key starts with the common prefix (16-byte fast path for short prefixes). */
static inline int hasPrefix(const slabFence *f, const unsigned char *key, size_t len) {
    if (f->lcp <= 16 && len >= 16) {
        uint64_t a = slabKeyHead(key, 8), b = slabKeyHead(key + 8, 8);
        return (((a ^ f->pfx[0]) & f->pfx_mask[0]) | ((b ^ f->pfx[1]) & f->pfx_mask[1])) == 0;
    }
    return len >= f->lcp && memcmp(key, f->data + f->offs[f->skip_first], f->lcp) == 0;
}

/* Rank of a key that does not start with the common prefix: all bounds are above or
 * all are below it. */
static inline size_t rankWithoutPrefix(const slabFence *f, const unsigned char *key, size_t len) {
    return slabKeyCmp(key, len, f->data + f->offs[f->skip_first], f->lcp) < 0 ? f->skip_first : f->n;
}

/* 'upto' is the number of heads <= h; full comparisons settle the run of equal heads. */
static size_t resolveTies(const slabFence *f, size_t upto, uint64_t h, const unsigned char *s, size_t slen) {
    size_t lo = upto;
    if (lo > 0 && f->heads[lo - 1] == h) {
        lo--;
        if (lo > 0 && f->heads[lo - 1] == h) {
            size_t a = 0, b = lo;
            while (a < b) {
                size_t mid = a + (b - a) / 2;
                if (f->heads[mid] < h)
                    a = mid + 1;
                else
                    b = mid;
            }
            lo = a;
        }
    } else {
        return upto;
    }
    size_t hi = upto;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2, klen;
        const unsigned char *k = slabFenceKey(f, mid, &klen);
        if (mid < f->skip_first || slabKeyCmp(k + f->lcp, klen - f->lcp, s, slen) <= 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

size_t slabFenceRank(const slabFence *f, const unsigned char *key, size_t len) {
    if (f->n == 0) return 0;
    if (!hasPrefix(f, key, len)) return rankWithoutPrefix(f, key, len);
    const unsigned char *s = key + f->lcp;
    size_t slen = len - f->lcp;
    uint64_t h = slabKeyHead(s, slen);
    size_t lo = 0, hi = f->n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (f->heads[mid] <= h)
            lo = mid + 1;
        else
            hi = mid;
    }
    return resolveTies(f, lo, h, s, slen);
}

void slabFenceRankBatch(const slabFence *f, size_t n, const unsigned char *const *keys, const size_t *lens, size_t *out) {
    if (f->n == 0) {
        for (size_t i = 0; i < n; i++) out[i] = 0;
        return;
    }
    const uint64_t *heads = f->heads;
    for (size_t start = 0; start < n; start += SLAB_FENCE_LANES) {
        size_t lanes = n - start < SLAB_FENCE_LANES ? n - start : SLAB_FENCE_LANES;
        uint64_t h[SLAB_FENCE_LANES] = {0};
        size_t base[SLAB_FENCE_LANES] = {0};
        int pre[SLAB_FENCE_LANES];
        for (size_t l = 0; l < lanes; l++) {
            pre[l] = hasPrefix(f, keys[start + l], lens[start + l]);
            if (pre[l]) h[l] = slabKeyHead(keys[start + l] + f->lcp, lens[start + l] - f->lcp);
        }
        /* Branch-free lockstep search: every lane takes the same number of steps. */
        size_t size = f->n;
        while (size > 1) {
            size_t half = size / 2;
            for (size_t l = 0; l < lanes; l++) {
                size_t mid = base[l] + half;
                base[l] = heads[mid] <= h[l] ? mid : base[l];
            }
            size -= half;
        }
        for (size_t l = 0; l < lanes; l++) {
            const unsigned char *k = keys[start + l];
            size_t kl = lens[start + l];
            if (!pre[l]) {
                out[start + l] = rankWithoutPrefix(f, k, kl);
            } else {
                size_t upto = base[l] + (heads[base[l]] <= h[l]);
                out[start + l] = resolveTies(f, upto, h[l], k + f->lcp, kl - f->lcp);
            }
        }
    }
}

void slabFenceRankLanes(const slabFence *const *idx, size_t lanes, const unsigned char *const *keys, const size_t *lens, size_t *out) {
    assert(lanes <= SLAB_FENCE_LANES);
    uint64_t h[SLAB_FENCE_LANES] = {0};
    size_t base[SLAB_FENCE_LANES] = {0};
    int pre[SLAB_FENCE_LANES];
    size_t widest = 0;
    for (size_t l = 0; l < lanes; l++) {
        const slabFence *f = idx[l];
        if (f->n > widest) widest = f->n;
        pre[l] = f->n && hasPrefix(f, keys[l], lens[l]);
        if (pre[l]) h[l] = slabKeyHead(keys[l] + f->lcp, lens[l] - f->lcp);
    }
    size_t size = widest;
    while (size > 1) {
        size_t half = size / 2;
        for (size_t l = 0; l < lanes; l++) {
            size_t mid = base[l] + half;
            uint64_t at = mid < idx[l]->n ? idx[l]->heads[mid] : UINT64_MAX;
            base[l] = at <= h[l] ? mid : base[l];
        }
        size -= half;
    }
    for (size_t l = 0; l < lanes; l++) {
        const slabFence *f = idx[l];
        if (f->n == 0) {
            out[l] = 0;
        } else if (!pre[l]) {
            out[l] = rankWithoutPrefix(f, keys[l], lens[l]);
        } else {
            size_t b = base[l] < f->n - 1 ? base[l] : f->n - 1;
            size_t upto = b + (f->heads[b] <= h[l]);
            out[l] = resolveTies(f, upto, h[l], keys[l] + f->lcp, lens[l] - f->lcp);
        }
    }
}
