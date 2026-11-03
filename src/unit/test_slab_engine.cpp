/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

extern "C" {
#include "rax.h"
#include "sds.h"
#include "slab.h"
#include "zmalloc.h"
}

/* A randomized differential test of the engine against a model (a rax of sds values):
 * puts and deletes in batches, freezes, flushes and compactions at random, with point
 * reads, batch reads, ordered iteration, COPY and an export/import round trip checked
 * against the model. SLAB_FUZZ_ROUNDS raises the iteration count for long runs. */

static uint64_t rngNext(uint64_t *s) {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    return *s;
}

#define KEY_MAX 48

static size_t keyOf(uint64_t i, char *buf) {
    return (size_t)snprintf(buf, KEY_MAX, "src:%llu:entity_%08llu", (unsigned long long)(i % 7), (unsigned long long)i);
}

static void freeVal(void *v) {
    sdsfree((sds)v);
}

static void modelPut(rax *m, const char *k, size_t kl, sds v) {
    void *old = NULL;
    raxInsert(m, (unsigned char *)k, kl, v, &old);
    if (old) sdsfree((sds)old);
}

static void modelDel(rax *m, const char *k, size_t kl) {
    void *old = NULL;
    if (raxRemove(m, (unsigned char *)k, kl, &old)) sdsfree((sds)old);
}

static rax *modelCopy(rax *m) {
    rax *d = raxNew();
    raxIterator ri;
    raxStart(&ri, m);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri)) raxInsert(d, ri.key, ri.key_len, sdsdup((sds)ri.data), NULL);
    raxStop(&ri);
    return d;
}

static void applyBatch(slabColl *c, rax *m, uint64_t *rng, uint64_t keyspace, int n, bool del, int round) {
    char ks[256][KEY_MAX], vs[256][64];
    const unsigned char *k[256], *v[256];
    size_t kl[256], vl[256];
    for (int i = 0; i < n; i++) {
        kl[i] = keyOf(rngNext(rng) % keyspace, ks[i]);
        snprintf(vs[i], sizeof(vs[i]), "v%d:%d:%llu", round, i, (unsigned long long)(rngNext(rng) % 1000));
        vl[i] = rngNext(rng) % 3 == 0 ? 0 : strlen(vs[i]);
        k[i] = (const unsigned char *)ks[i];
        v[i] = (const unsigned char *)vs[i];
    }
    slabApply(c, (size_t)n, k, kl, v, vl, del);
    for (int i = 0; i < n; i++) {
        if (del)
            modelDel(m, ks[i], kl[i]);
        else
            modelPut(m, ks[i], kl[i], sdsnewlen(vs[i], vl[i]));
    }
}

static void checkAll(slabColl *c, rax *m, uint64_t keyspace) {
    slabSnapshot s;
    slabSnapshotTake(c, &s, false);
    /* Point and batch reads of every key of the keyspace. */
    const size_t B = 97;
    char ks[B][KEY_MAX];
    const unsigned char *k[B], *v[B];
    size_t kl[B], vl[B];
    for (uint64_t start = 0; start < keyspace; start += B) {
        size_t n = 0;
        for (uint64_t i = start; i < start + B && i < keyspace; i++, n++) {
            kl[n] = keyOf(i, ks[n]);
            k[n] = (const unsigned char *)ks[n];
        }
        slabMultiGet(&s, n, k, kl, v, vl);
        for (size_t j = 0; j < n; j++) {
            void *want = NULL;
            bool present = raxFind(m, (unsigned char *)ks[j], kl[j], &want);
            if (!present) {
                ASSERT_EQ(v[j], nullptr) << ks[j];
            } else {
                ASSERT_NE(v[j], nullptr) << ks[j];
                ASSERT_EQ(vl[j], sdslen((sds)want)) << ks[j];
                ASSERT_EQ(memcmp(v[j], want, vl[j]), 0) << ks[j];
            }
            const unsigned char *pv;
            size_t pvl;
            bool got = slabGet(&s, k[j], kl[j], &pv, &pvl);
            ASSERT_EQ(got, present) << ks[j];
        }
    }
    /* Ordered iteration, whole and resumed in budgeted steps. */
    slabIter *it = slabIterCreate(&s, NULL, 0, false);
    const unsigned char *ik, *iv;
    size_t ikl, ivl;
    raxIterator ri;
    raxStart(&ri, m);
    raxSeek(&ri, "^", NULL, 0);
    while (slabIterNext(it, &ik, &ikl, &iv, &ivl)) {
        ASSERT_TRUE(raxNext(&ri));
        ASSERT_EQ(ikl, ri.key_len);
        ASSERT_EQ(memcmp(ik, ri.key, ikl), 0);
        ASSERT_EQ(ivl, sdslen((sds)ri.data));
        ASSERT_EQ(memcmp(iv, ri.data, ivl), 0);
    }
    ASSERT_FALSE(raxNext(&ri));
    raxStop(&ri);
    slabIterFree(it);
    ASSERT_EQ(slabCountExact(&s), raxSize(m));
    sds start = sdsempty();
    size_t seen = 0;
    for (;;) {
        it = slabIterCreate(&s, (const unsigned char *)start, sdslen(start), false);
        slabIterSetBudget(it, 13);
        while (slabIterNext(it, &ik, &ikl, &iv, &ivl)) seen++;
        bool more = slabIterStoppedOnBudget(it);
        if (more) {
            slabIterLastKey(it, &ik, &ikl);
            sdsclear(start);
            start = sdscatlen(start, ik, ikl);
            start = sdscatlen(start, "\0", 1);
        }
        slabIterFree(it);
        if (!more) break;
    }
    sdsfree(start);
    ASSERT_EQ(seen, raxSize(m));
    char *err = slabVerify(&s);
    ASSERT_EQ(err, nullptr) << err;
}

/* Exports a snapshot and imports it into a new collection. */
static slabColl *roundTrip(slabColl *c) {
    slabSnapshot s;
    slabSnapshotTake(c, &s, false);
    slabExport *e = slabExportBegin(&s);
    slabImport *im = slabImportBegin(slabCollConfig(c));
    for (uint64_t i = 0; i < slabExportLeafCount(e); i++) {
        const unsigned char *lower, *base;
        size_t ll, bl, dl;
        unsigned char *delta;
        slabExportLeaf(e, i, &lower, &ll, &base, &bl, &delta, &dl);
        unsigned char *bcopy = (unsigned char *)zmalloc(bl);
        memcpy(bcopy, base, bl);
        const char *err = slabImportLeaf(im, lower, ll, bcopy, bl, delta, dl);
        EXPECT_EQ(err, nullptr) << err;
    }
    const unsigned char *k, *v;
    size_t kl, vl;
    slabExportPendingCount(e);
    while (slabExportPendingNext(e, &k, &kl, &v, &vl)) {
        const char *err = slabImportPending(im, k, kl, v, vl, v == NULL);
        EXPECT_EQ(err, nullptr) << err;
    }
    slabExportEnd(e);
    const char *err;
    slabColl *out = slabImportFinish(im, &err);
    EXPECT_EQ(err, nullptr);
    return out;
}

TEST(SlabEngine, DifferentialAgainstModel) {
    int rounds = getenv("SLAB_FUZZ_ROUNDS") ? atoi(getenv("SLAB_FUZZ_ROUNDS")) : 3;
    for (int seed = 1; seed <= rounds; seed++) {
        uint64_t rng = 0x9e3779b97f4a7c15ULL * (uint64_t)seed;
        slabConfig cfg;
        slabConfigDefaults(&cfg);
        cfg.block_bytes = 1024 + (uint32_t)(rngNext(&rng) % 8192);
        cfg.key_restart = 1 + (uint32_t)(rngNext(&rng) % 20);
        cfg.value_restart = 1 + (uint32_t)(rngNext(&rng) % 20);
        cfg.flush_entries = 50 + rngNext(&rng) % 500;
        cfg.compact_ratio_bp = 1000 + (uint32_t)(rngNext(&rng) % 6000);
        uint64_t keyspace = 300 + rngNext(&rng) % 3000;
        slabColl *c = slabCollCreate(&cfg);
        rax *m = raxNew();
        for (int round = 0; round < 200; round++) {
            int op = (int)(rngNext(&rng) % 10);
            if (op < 6)
                applyBatch(c, m, &rng, keyspace, 1 + (int)(rngNext(&rng) % 200), false, round);
            else if (op < 8)
                applyBatch(c, m, &rng, keyspace, 1 + (int)(rngNext(&rng) % 50), true, round);
            else if (op == 8)
                slabFreeze(c);
            else {
                uint64_t target;
                slabRequestCompaction(c, rngNext(&rng) % 2, &target);
            }
            if (round % 50 == 49) checkAll(c, m, keyspace);
        }
        checkAll(c, m, keyspace);
        /* COPY: both sides independent afterwards. */
        slabColl *d = slabCollCopy(c);
        rax *md = modelCopy(m);
        applyBatch(d, md, &rng, keyspace, 100, false, 9999);
        applyBatch(c, m, &rng, keyspace, 100, true, 9998);
        checkAll(c, m, keyspace);
        checkAll(d, md, keyspace);
        /* Export/import keeps the content. */
        slabColl *r = roundTrip(d);
        checkAll(r, md, keyspace);
        slabCollDetach(r);
        slabCollDetach(d);
        slabCollDetach(c);
        raxFreeWithCallback(m, freeVal);
        raxFreeWithCallback(md, freeVal);
    }
}

/* Sustained overwrites spread over every leaf: automatic compaction must reach every
 * leaf, not only the first ones in key order, so the deltas (which keep every version
 * until their leaf is compacted) stay within a small multiple of the bases. */
TEST(SlabEngine, OverwritesStayCompacted) {
    slabConfig cfg;
    slabConfigDefaults(&cfg);
    cfg.block_bytes = 1024;
    cfg.flush_entries = 500;
    slabColl *c = slabCollCreate(&cfg);
    rax *m = raxNew();
    uint64_t rng = 11;
    const uint64_t keyspace = 20000;
    for (int round = 0; round < 200; round++) applyBatch(c, m, &rng, keyspace, 100, false, round);
    uint64_t target;
    slabRequestFlush(c, &target);
    slabInfo info;
    slabCollInfo(c, &info);
    ASSERT_GT(info.blocks, 100u);
    for (int round = 200; round < 3000; round++) applyBatch(c, m, &rng, keyspace, 100, false, round);
    slabCollInfo(c, &info);
    EXPECT_LT(info.delta_bytes, 2 * info.base_bytes) << "delta " << info.delta_bytes << " base " << info.base_bytes;
    checkAll(c, m, keyspace);
    slabCollDetach(c);
    raxFreeWithCallback(m, freeVal);
}
