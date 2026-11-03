/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef SLAB_HASH_H
#define SLAB_HASH_H

#include <stdint.h>
#include <string.h>

/* Key hash for slab collections: the home slot of a key in a memtable or delta log
 * table (low bits) and the 8-bit tag stored next to a record pointer (high bits).
 * It is never persisted, so it can change without a format change.
 *
 * The construction is wyhash's (public domain): a 64x64->128 multiply folded to 64
 * bits mixes 16 input bytes per step. Keys in a collection are short and share
 * long prefixes, so the short-key path reads the first and last words, never the
 * prefix alone. The seed is random per process (slabInitServer), so clients can't
 * choose keys that collide; tables are rebuilt on load, never persisted. */

extern uint64_t slab_hash_seed;

#define SLAB_HASH_P0 0xa0761d6478bd642fULL
#define SLAB_HASH_P1 0xe7037ed1a0b428dbULL
#define SLAB_HASH_P2 0x8ebc6af09c88c6e3ULL

static inline uint64_t slabHashMum(uint64_t a, uint64_t b) {
#if defined(__SIZEOF_INT128__)
    __uint128_t r = (__uint128_t)a * b;
    return (uint64_t)r ^ (uint64_t)(r >> 64);
#else
    uint64_t ha = a >> 32, hb = b >> 32, la = (uint32_t)a, lb = (uint32_t)b;
    uint64_t rh = ha * hb, rm0 = ha * lb, rm1 = hb * la, rl = la * lb;
    uint64_t t = rl + (rm0 << 32), c = t < rl;
    uint64_t lo = t + (rm1 << 32);
    c += lo < t;
    uint64_t hi = rh + (rm0 >> 32) + (rm1 >> 32) + c;
    return lo ^ hi;
#endif
}

static inline uint64_t slabHashRead8(const unsigned char *p) {
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}

static inline uint64_t slabHashRead4(const unsigned char *p) {
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static inline uint64_t slabHash(const void *key, size_t len) {
    const unsigned char *p = (const unsigned char *)key;
    uint64_t seed = SLAB_HASH_P0 ^ slab_hash_seed ^ slabHashMum(len, SLAB_HASH_P1);
    uint64_t a, b;
    if (len <= 16) {
        if (len >= 4) {
            size_t mid = (len >> 3) << 2;
            a = (slabHashRead4(p) << 32) | slabHashRead4(p + mid);
            b = (slabHashRead4(p + len - 4) << 32) | slabHashRead4(p + len - 4 - mid);
        } else if (len > 0) {
            a = ((uint64_t)p[0] << 16) | ((uint64_t)p[len >> 1] << 8) | p[len - 1];
            b = 0;
        } else {
            a = b = 0;
        }
    } else {
        size_t i = len;
        while (i > 16) {
            seed = slabHashMum(slabHashRead8(p) ^ SLAB_HASH_P1, slabHashRead8(p + 8) ^ seed);
            p += 16;
            i -= 16;
        }
        a = slabHashRead8(p + i - 16);
        b = slabHashRead8(p + i - 8);
    }
    a ^= SLAB_HASH_P1;
    b ^= seed;
    return slabHashMum(SLAB_HASH_P2 ^ len, slabHashMum(a, b));
}

#endif /* SLAB_HASH_H */
