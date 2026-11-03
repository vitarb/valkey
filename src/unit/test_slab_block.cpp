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
#include "slab_hash.h"
#include "zmalloc.h"
}

/* A sorted list of entries in plain C arrays. */
struct Entries {
    size_t n;
    unsigned char **keys;
    size_t *klens;
    unsigned char **vals;
    size_t *vlens;
};

static uint64_t rngNext(uint64_t *s) {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    return *s;
}

static unsigned char *dupBytes(const void *p, size_t n) {
    unsigned char *d = (unsigned char *)zmalloc(n ? n : 1);
    memcpy(d, p, n);
    return d;
}

static void entriesInit(Entries *e, size_t cap) {
    e->n = 0;
    e->keys = (unsigned char **)zcalloc(cap * sizeof(unsigned char *));
    e->klens = (size_t *)zcalloc(cap * sizeof(size_t));
    e->vals = (unsigned char **)zcalloc(cap * sizeof(unsigned char *));
    e->vlens = (size_t *)zcalloc(cap * sizeof(size_t));
}

static void entriesAdd(Entries *e, const void *k, size_t kl, const void *v, size_t vl) {
    e->keys[e->n] = dupBytes(k, kl);
    e->klens[e->n] = kl;
    e->vals[e->n] = dupBytes(v, vl);
    e->vlens[e->n] = vl;
    e->n++;
}

static void entriesFree(Entries *e) {
    for (size_t i = 0; i < e->n; i++) {
        zfree(e->keys[i]);
        zfree(e->vals[i]);
    }
    zfree(e->keys);
    zfree(e->klens);
    zfree(e->vals);
    zfree(e->vlens);
}

static slabBase *buildFrom(const Entries *e, uint32_t kr, uint32_t vr) {
    return slabBaseBuild(kr, vr, e->n, (const unsigned char *const *)e->keys, e->klens,
                         (const unsigned char *const *)e->vals, e->vlens);
}

/* Index of key in the sorted entries, or -1. */
static long plainFind(const Entries *e, const unsigned char *k, size_t kl) {
    long lo = 0, hi = (long)e->n - 1;
    while (lo <= hi) {
        long mid = (lo + hi) / 2;
        int c = slabKeyCmp(e->keys[mid], e->klens[mid], k, kl);
        if (c == 0) return mid;
        if (c < 0)
            lo = mid + 1;
        else
            hi = mid - 1;
    }
    return -1;
}

static int cmpKeyPtr(const void *a, const void *b) {
    const unsigned char *const *x = (const unsigned char *const *)a;
    const unsigned char *const *y = (const unsigned char *const *)b;
    /* Keys here are NUL-free strings built by the tests. */
    return strcmp((const char *)*x, (const char *)*y);
}

/* Every string over a small alphabet up to max_len, sorted, as NUL-terminated
 * buffers (the alphabet has no NUL). */
static size_t allStrings(const char *alphabet, size_t max_len, char ***out) {
    size_t a = strlen(alphabet), total = 1, level = 1;
    for (size_t l = 1; l <= max_len; l++) {
        level *= a;
        total += level;
    }
    char **s = (char **)zmalloc(total * sizeof(char *));
    size_t n = 0;
    s[n++] = (char *)dupBytes("", 1);
    size_t start = 0;
    for (size_t l = 1; l <= max_len; l++) {
        size_t end = n;
        for (size_t i = start; i < end; i++) {
            for (size_t c = 0; c < a; c++) {
                size_t pl = strlen(s[i]);
                char *t = (char *)zmalloc(pl + 2);
                memcpy(t, s[i], pl);
                t[pl] = alphabet[c];
                t[pl + 1] = 0;
                s[n++] = t;
            }
        }
        start = end;
    }
    qsort(s, n, sizeof(char *), cmpKeyPtr);
    *out = s;
    return n;
}

/* Checks get, a fresh cursor per target set and the iterator against a plain search. */
static void checkTargets(const Entries *e, const slabBase *b, char **targets, size_t ntargets) {
    slabBaseCursor cur;
    slabBaseCursorInit(&cur, b);
    for (size_t t = 0; t < ntargets; t++) {
        const unsigned char *k = (const unsigned char *)targets[t];
        size_t kl = strlen(targets[t]);
        long want = plainFind(e, k, kl);
        const unsigned char *v;
        size_t vl;
        bool got = slabBaseGet(b, k, kl, &v, &vl);
        ASSERT_EQ(got, want >= 0) << "get " << targets[t];
        if (got) {
            ASSERT_EQ(vl, e->vlens[want]);
            ASSERT_EQ(0, memcmp(v, e->vals[want], vl));
        }
        if (kl < b->prefix_len || memcmp(k, b->prefix, b->prefix_len) != 0) continue;
        got = slabBaseCursorGet(&cur, k + b->prefix_len, kl - b->prefix_len, &v, &vl);
        ASSERT_EQ(got, want >= 0) << "cursor " << targets[t];
        if (got) {
            ASSERT_EQ(0, memcmp(v, e->vals[want], vl));
        }
    }
}

static void valueFor(size_t i, unsigned char *buf, size_t *len) {
    *len = i % 5;
    memset(buf, (int)(i % 251), *len);
}

TEST(SlabBlock, SearchMatchesPlainSearch) {
    char **probes;
    size_t np = allStrings("0ab", 4, &probes);
    uint64_t rng = 0x5eed;
    const uint32_t intervals[] = {1, 2, 3, 4, 5, 7, 8, 16, 31, 64, 1024};
    for (int round = 0; round < 24; round++) {
        const char *lead = round % 3 == 0 ? "pre:" : "";
        Entries e;
        entriesInit(&e, np);
        char key[64];
        unsigned char val[8];
        size_t vl;
        for (size_t i = 0; i < np; i++) {
            if (rngNext(&rng) % 3) continue;
            int kl = snprintf(key, sizeof(key), "%s%s", lead, probes[i]);
            valueFor(e.n, val, &vl);
            entriesAdd(&e, key, (size_t)kl, val, vl);
        }
        /* Targets: every probe with the lead, plus keys past the end. */
        size_t nt = np + 2;
        char **targets = (char **)zmalloc(nt * sizeof(char *));
        for (size_t i = 0; i < np; i++) {
            targets[i] = (char *)zmalloc(strlen(lead) + strlen(probes[i]) + 1);
            sprintf(targets[i], "%s%s", lead, probes[i]);
        }
        targets[np] = (char *)dupBytes("zzzzzz", 7);
        targets[np + 1] = (char *)zmalloc(strlen(lead) + 7);
        sprintf(targets[np + 1], "%szzzzzz", lead);
        qsort(targets, nt, sizeof(char *), cmpKeyPtr);
        for (size_t ii = 0; ii < sizeof(intervals) / sizeof(intervals[0]); ii++) {
            const uint32_t vrs[] = {1, 3, 16, 1024};
            slabBase *b = buildFrom(&e, intervals[ii], vrs[round % 4]);
            ASSERT_NE(b, nullptr);
            /* Sorted targets at several strides, duplicates, and a shuffled set. */
            const size_t strides[] = {1, 2, 3, 7, 16, 61};
            for (size_t s = 0; s < 6; s++) {
                size_t m = 0;
                char **sub = (char **)zmalloc(nt * sizeof(char *));
                for (size_t i = 0; i < nt; i += strides[s]) sub[m++] = targets[i];
                checkTargets(&e, b, sub, m);
                zfree(sub);
            }
            char **dups = (char **)zmalloc(128 * sizeof(char *));
            for (size_t i = 0; i < 64; i++) dups[2 * i] = dups[2 * i + 1] = targets[i % nt];
            checkTargets(&e, b, dups, 128);
            for (size_t i = 0; i < 64; i++) {
                size_t j = rngNext(&rng) % nt;
                dups[i] = targets[j];
            }
            checkTargets(&e, b, dups, 64);
            zfree(dups);
            slabBaseFree(b);
        }
        for (size_t i = 0; i < nt; i++) zfree(targets[i]);
        zfree(targets);
        entriesFree(&e);
    }
    for (size_t i = 0; i < np; i++) zfree(probes[i]);
    zfree(probes);
}

TEST(SlabBlock, RestartHeadTies) {
    /* Keys whose first 8 suffix bytes tie, so the cursor must fall back to full
     * comparisons of restart keys. */
    Entries e;
    entriesInit(&e, 256);
    const char *stems[] = {"ab", "abcdefg", "abcdefgh", "abcdefghi", "abcdefghij"};
    const char *tails[] = {"", "x", "xy", "y", "z"};
    char **all = (char **)zmalloc(25 * sizeof(char *));
    size_t n = 0;
    for (int s = 0; s < 5; s++)
        for (int t = 0; t < 5; t++) {
            all[n] = (char *)zmalloc(32);
            sprintf(all[n], "%s%s", stems[s], tails[t]);
            n++;
        }
    qsort(all, n, sizeof(char *), cmpKeyPtr);
    for (size_t i = 0; i < n; i++) {
        if (i > 0 && strcmp(all[i], all[i - 1]) == 0) continue;
        unsigned char val[8];
        size_t vl;
        valueFor(i + 1, val, &vl);
        entriesAdd(&e, all[i], strlen(all[i]), val, vl);
    }
    const uint32_t intervals[] = {1, 2, 3, 5, 1024};
    for (int ii = 0; ii < 5; ii++) {
        slabBase *b = buildFrom(&e, intervals[ii], 2);
        checkTargets(&e, b, all, n);
        slabBaseFree(b);
    }
    for (size_t i = 0; i < n; i++) zfree(all[i]);
    zfree(all);
    entriesFree(&e);
}

TEST(SlabBlock, IteratorAndSeek) {
    Entries e;
    entriesInit(&e, 1000);
    char key[32];
    for (int i = 0; i < 1000; i++) {
        int kl = snprintf(key, sizeof(key), "prefix3:%08d", i * 2);
        entriesAdd(&e, key, (size_t)kl, key, (size_t)kl);
    }
    slabBase *b = buildFrom(&e, 16, 16);
    ASSERT_EQ(b->prefix_len, 12u); /* "prefix3:0000" */
    slabBaseIter it;
    slabBaseIterInit(&it, b);
    const unsigned char *k, *v;
    size_t kl, vl, i = 0;
    while (slabBaseIterNext(&it, &k, &kl, &v, &vl)) {
        ASSERT_EQ(kl, e.klens[i]);
        ASSERT_EQ(0, memcmp(k, e.keys[i], kl));
        ASSERT_EQ(0, memcmp(v, e.vals[i], vl));
        i++;
    }
    ASSERT_EQ(i, 1000u);
    for (int t = -1; t < 2002; t += 7) {
        int tl = snprintf(key, sizeof(key), "prefix3:%08d", t < 0 ? 0 : t);
        slabBaseIterSeek(&it, (unsigned char *)key, (size_t)tl);
        size_t want = (size_t)((t < 0 ? 0 : t) + 1) / 2;
        if (want >= 1000) {
            ASSERT_FALSE(slabBaseIterNext(&it, &k, &kl, &v, &vl));
            continue;
        }
        ASSERT_TRUE(slabBaseIterNext(&it, &k, &kl, &v, &vl));
        ASSERT_EQ(0, memcmp(k, e.keys[want], kl)) << t;
        if (want + 1 < 1000) {
            ASSERT_TRUE(slabBaseIterNext(&it, &k, &kl, &v, &vl));
            ASSERT_EQ(0, memcmp(k, e.keys[want + 1], kl));
        }
    }
    /* Keys outside the block prefix. */
    slabBaseIterSeek(&it, (const unsigned char *)"a", 1);
    ASSERT_TRUE(slabBaseIterNext(&it, &k, &kl, &v, &vl));
    ASSERT_EQ(0, memcmp(k, e.keys[0], kl));
    slabBaseIterSeek(&it, (const unsigned char *)"z", 1);
    ASSERT_FALSE(slabBaseIterNext(&it, &k, &kl, &v, &vl));
    slabBaseIterRelease(&it);

    unsigned char *first, *last;
    size_t fl, ll;
    slabBaseKeyBounds(b, &first, &fl, &last, &ll);
    ASSERT_EQ(0, memcmp(first, e.keys[0], fl));
    ASSERT_EQ(0, memcmp(last, e.keys[999], ll));
    zfree(first);
    zfree(last);
    slabBaseFree(b);
    entriesFree(&e);
}

TEST(SlabBlock, LongKeysValuesAndEmptyKey) {
    Entries e;
    entriesInit(&e, 10);
    unsigned char big[3000];
    memset(big, 'k', sizeof(big));
    entriesAdd(&e, "", 0, "empty", 5);
    for (int i = 0; i < 5; i++) {
        big[200 + i] = (unsigned char)('a' + i);
        entriesAdd(&e, big, 200 + i + 1, big, 2000 + i);
    }
    slabBase *b = buildFrom(&e, 2, 2);
    for (size_t i = 0; i < e.n; i++) {
        const unsigned char *v;
        size_t vl;
        ASSERT_TRUE(slabBaseGet(b, e.keys[i], e.klens[i], &v, &vl));
        ASSERT_EQ(vl, e.vlens[i]);
    }
    slabBaseFree(b);
    entriesFree(&e);
}

TEST(SlabBlock, BuilderSizeIsAnUpperBound) {
    uint64_t rng = 7;
    for (int round = 0; round < 50; round++) {
        slabBaseBuilder bb;
        slabBaseBuilderInit(&bb, 1 + rngNext(&rng) % 20, 1 + rngNext(&rng) % 20);
        char key[300];
        size_t n = 1 + rngNext(&rng) % 300;
        for (size_t i = 0; i < n; i++) {
            /* Some keys get a long tail, so length varints cross 128 bytes. */
            int kl = snprintf(key, sizeof(key), "k%08zu", i * 3);
            if (rngNext(&rng) % 4 == 0) {
                memset(key + kl, 'x', 150);
                kl += 150;
            }
            size_t shared = slabBaseBuilderShared(&bb, (unsigned char *)key, (size_t)kl);
            size_t with = slabBaseBuilderBytesWith(&bb, (size_t)kl, i % 70, shared);
            unsigned char val[70] = {0};
            slabBaseBuilderAdd(&bb, (unsigned char *)key, (size_t)kl, val, i % 70);
            ASSERT_EQ(with, slabBaseBuilderBytes(&bb));
        }
        size_t bound = slabBaseBuilderBytes(&bb);
        slabBase *b = slabBaseBuilderFinish(&bb);
        ASSERT_LE(b->len, bound);
        ASSERT_EQ(b->entries, n);
        slabBaseFree(b);
        slabBaseBuilderRelease(&bb);
    }
}

TEST(SlabBlock, ValidationRejectsCorruption) {
    Entries e;
    entriesInit(&e, 200);
    char key[32];
    for (int i = 0; i < 200; i++) {
        int kl = snprintf(key, sizeof(key), "key:%06d", i);
        entriesAdd(&e, key, (size_t)kl, key, (size_t)kl);
    }
    slabBase *good = buildFrom(&e, 4, 4);
    size_t len = good->len;
    /* Every single-byte flip either fails validation or still decodes every entry
     * consistently (a flipped value byte); it never crashes. */
    for (size_t pos = 0; pos < len; pos++) {
        unsigned char *copy = dupBytes(good->buf, len);
        copy[pos] ^= 0x41;
        const char *err = NULL;
        slabBase *b = slabBaseLoad(copy, len, &err);
        if (!b) {
            ASSERT_NE(err, nullptr);
            zfree(copy);
            continue;
        }
        slabBaseIter it;
        slabBaseIterInit(&it, b);
        const unsigned char *k, *v;
        size_t kl, vl, n = 0;
        while (slabBaseIterNext(&it, &k, &kl, &v, &vl)) n++;
        ASSERT_EQ(n, b->entries);
        slabBaseIterRelease(&it);
        slabBaseFree(b);
    }
    /* Truncation always fails. */
    for (size_t cut = 0; cut < len; cut += 13) {
        unsigned char *copy = dupBytes(good->buf, cut);
        const char *err = NULL;
        ASSERT_EQ(slabBaseLoad(copy, cut, &err), nullptr);
        zfree(copy);
    }
    slabBaseFree(good);
    entriesFree(&e);
}

TEST(SlabBlock, EmptyBlock) {
    slabBase *b = slabBaseBuild(16, 16, 0, NULL, NULL, NULL, NULL);
    ASSERT_NE(b, nullptr);
    ASSERT_EQ(b->entries, 0u);
    const unsigned char *v;
    size_t vl;
    ASSERT_FALSE(slabBaseGet(b, (const unsigned char *)"x", 1, &v, &vl));
    slabBaseFree(b);
}

TEST(SlabBlock, DeltaSegmentRoundTrip) {
    slabSegWriter w;
    slabSegWriterInit(&w);
    char key[32];
    for (int i = 0; i < 500; i++) {
        int kl = snprintf(key, sizeof(key), "d%05d", i);
        slabSegWriterAdd(&w, (unsigned char *)key, (size_t)kl, (unsigned char *)key, (size_t)kl, i % 7 == 0);
    }
    size_t len;
    unsigned char *buf = slabSegWriterFinish(&w, &len);
    const char *err = NULL;
    slabDeltaSeg *s = slabDeltaSegLoad(buf, len, &err);
    ASSERT_NE(s, nullptr) << err;
    ASSERT_EQ(s->entries, 500u);
    ASSERT_EQ(s->tombstones, 72u);
    for (int i = 0; i < 520; i++) {
        int kl = snprintf(key, sizeof(key), "d%05d", i);
        const unsigned char *v;
        size_t vl;
        int r = slabDeltaSegLookup(s, (unsigned char *)key, (size_t)kl, slabHash(key, (size_t)kl), &v, &vl);
        if (i >= 500)
            ASSERT_EQ(r, SLAB_SEG_MISS);
        else if (i % 7 == 0)
            ASSERT_EQ(r, SLAB_SEG_TOMBSTONE);
        else {
            ASSERT_EQ(r, SLAB_SEG_PUT);
            ASSERT_EQ(0, memcmp(v, key, vl));
        }
    }
    size_t pos = 0, n = 0;
    const unsigned char *k, *v;
    size_t kl, vl;
    while (slabDeltaSegNext(s, &pos, &k, &kl, &v, &vl)) n++;
    ASSERT_EQ(n, 500u);
    slabDeltaSegFree(s);

    /* Out-of-order or duplicate keys are rejected. */
    slabSegWriterInit(&w);
    slabSegWriterAdd(&w, (const unsigned char *)"b", 1, (const unsigned char *)"1", 1, false);
    slabSegWriterAdd(&w, (const unsigned char *)"a", 1, (const unsigned char *)"1", 1, false);
    buf = slabSegWriterFinish(&w, &len);
    ASSERT_EQ(slabDeltaSegLoad(buf, len, &err), nullptr);
    zfree(buf);
}

TEST(SlabBlock, SizeClassFloor) {
    const size_t sizes[] = {8, 100, 4096, 5000, 65536, 70000, 1 << 20};
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
        size_t f = slabSizeClassFloor(sizes[i]);
        ASSERT_LE(f, sizes[i]);
        size_t usable;
        void *p = zmalloc_usable(f, &usable);
        zfree(p);
        /* Below the smallest class (16 bytes with libc on macOS) the size is kept. */
        if (f == sizes[i] && usable > f) continue;
        ASSERT_EQ(usable, f);
    }
}
