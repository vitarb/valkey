/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <atomic>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

extern "C" {
#include "slab_hash.h"
#include "slab_vlog.h"
#include "zmalloc.h"
}

static int lookup(slabVlog *log, const char *key, uint64_t bound, char *out) {
    const unsigned char *v;
    size_t vl;
    size_t kl = strlen(key);
    int r = slabVlogLookup(log, (const unsigned char *)key, kl, slabHash(key, kl), bound, &v, &vl);
    if (r == SLAB_LOOKUP_PUT) {
        memcpy(out, v, vl);
        out[vl] = 0;
    }
    return r;
}

static void put(slabVlog *log, const char *key, const char *val) {
    size_t kl = strlen(key);
    slabVlogAppend(log, (const unsigned char *)key, kl, slabHash(key, kl), (const unsigned char *)val,
                   val ? strlen(val) : 0, val == NULL);
}

TEST(SlabVlog, VersionsAndBounds) {
    slabVlog *log = slabVlogCreate(4, 64);
    char buf[64];
    slabVlogBegin(log, 1, 0);
    put(log, "a", "a1");
    put(log, "b", "b1");
    slabVlogCommit(log);
    slabVlogBegin(log, 2, 0);
    put(log, "a", "a2");
    put(log, "b", NULL);
    slabVlogCommit(log);
    ASSERT_EQ(lookup(log, "a", 0, buf), SLAB_LOOKUP_MISS);
    ASSERT_EQ(lookup(log, "a", 1, buf), SLAB_LOOKUP_PUT);
    ASSERT_STREQ(buf, "a1");
    ASSERT_EQ(lookup(log, "a", 2, buf), SLAB_LOOKUP_PUT);
    ASSERT_STREQ(buf, "a2");
    ASSERT_EQ(lookup(log, "b", 1, buf), SLAB_LOOKUP_PUT);
    ASSERT_EQ(lookup(log, "b", 5, buf), SLAB_LOOKUP_TOMBSTONE);
    ASSERT_EQ(lookup(log, "c", 5, buf), SLAB_LOOKUP_MISS);
    ASSERT_EQ(slabVlogEntries(log), 2u);
    ASSERT_EQ(slabVlogPuts(log), 1u);
    ASSERT_EQ(slabVlogTombstones(log), 1u);
    ASSERT_EQ(slabVlogRecords(log), 4u);
    slabVlogFree(log);
}

TEST(SlabVlog, GrowKeepsEveryVersion) {
    slabVlog *log = slabVlogCreate(4, 0);
    char key[32], val[32], buf[64];
    for (int v = 1; v <= 3; v++) {
        slabVlogBegin(log, v, 0);
        for (int i = 0; i < 5000; i++) {
            snprintf(key, sizeof(key), "key:%d", i);
            snprintf(val, sizeof(val), "%d:%d", i, v);
            put(log, key, val);
        }
        slabVlogCommit(log);
    }
    ASSERT_GE(slabVlogCapacity(log), 10000u);
    for (int v = 1; v <= 3; v++) {
        for (int i = 0; i < 5000; i += 37) {
            snprintf(key, sizeof(key), "key:%d", i);
            snprintf(val, sizeof(val), "%d:%d", i, v);
            ASSERT_EQ(lookup(log, key, v, buf), SLAB_LOOKUP_PUT);
            ASSERT_STREQ(buf, val);
        }
    }
    /* Iteration under a bound sees each key once, at its newest version <= bound. */
    slabVlogIter it;
    slabVlogIterInit(&it, log, 2);
    const slabVlogRec *r;
    int n = 0;
    while ((r = slabVlogIterNext(&it))) {
        ASSERT_EQ(r->ver, 2u);
        n++;
    }
    ASSERT_EQ(n, 5000);
    slabVlogFree(log);
}

TEST(SlabVlog, AbortRestoresSlotsAndCounters) {
    slabVlog *log = slabVlogCreate(4, 0);
    char key[32], buf[64];
    slabVlogBegin(log, 1, 0);
    for (int i = 0; i < 10; i++) {
        snprintf(key, sizeof(key), "k%d", i);
        put(log, key, "v1");
    }
    slabVlogCommit(log);
    uint64_t cap_before = slabVlogCapacity(log);
    /* Overwrites, new keys enough to force a grow, and a key written twice. */
    slabVlogBegin(log, 2, 0);
    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "k%d", i);
        put(log, key, "v2");
    }
    put(log, "k3", NULL);
    put(log, "k3", "v2b");
    ASSERT_GT(slabVlogCapacity(log), cap_before);
    slabVlogAbort(log);
    ASSERT_EQ(slabVlogEntries(log), 10u);
    ASSERT_EQ(slabVlogRecords(log), 10u);
    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "k%d", i);
        int r = lookup(log, key, UINT64_MAX, buf);
        if (i < 10) {
            ASSERT_EQ(r, SLAB_LOOKUP_PUT);
            ASSERT_STREQ(buf, "v1");
        } else {
            ASSERT_EQ(r, SLAB_LOOKUP_MISS);
        }
    }
    /* The version may be reused after an abort. */
    slabVlogBegin(log, 2, 0);
    put(log, "k150", "again");
    slabVlogCommit(log);
    ASSERT_EQ(lookup(log, "k150", 2, buf), SLAB_LOOKUP_PUT);
    ASSERT_STREQ(buf, "again");
    slabVlogFree(log);
}

TEST(SlabVlog, LargeRecordsGetOwnChunks) {
    slabVlog *log = slabVlogCreate(4, 128);
    static char big[200000];
    memset(big, 'x', sizeof(big) - 1);
    char buf[16];
    slabVlogBegin(log, 1, 0);
    put(log, "small", "s");
    put(log, "big", big);
    put(log, "small2", "s2");
    slabVlogCommit(log);
    const unsigned char *v;
    size_t vl;
    ASSERT_EQ(slabVlogLookup(log, (const unsigned char *)"big", 3, slabHash("big", 3), 1, &v, &vl), SLAB_LOOKUP_PUT);
    ASSERT_EQ(vl, sizeof(big) - 1);
    ASSERT_EQ(lookup(log, "small2", 1, buf), SLAB_LOOKUP_PUT);
    ASSERT_GE(slabVlogBytes(log), sizeof(big));
    slabVlogFree(log);
}

/* One writer appends batches at increasing versions and publishes each version with
 * release only after commit (as a memtable's published_seq); some batches abort and
 * reuse their version. Readers take the bound with acquire, then look up keys: every
 * key must show exactly the value of the newest committed version <= the bound. */
struct Shared {
    slabVlog *log;
    std::atomic<uint64_t> published;
    std::atomic<bool> stop;
};

#define NKEYS 512

static void *readerThread(void *arg) {
    Shared *s = (Shared *)arg;
    char key[32], buf[64];
    uint64_t rng = (uint64_t)(uintptr_t)&rng | 1;
    while (!s->stop.load(std::memory_order_relaxed)) {
        uint64_t bound = s->published.load(std::memory_order_acquire);
        if (bound == 0) continue;
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        int k = (int)(rng % NKEYS);
        snprintf(key, sizeof(key), "key:%d", k);
        int r = lookup(s->log, key, bound, buf);
        /* Committed batch v writes every key k with v % 4 != 0 as "k:v"; keys with
         * k % 5 == 0 are tombstoned in batches with v % 3 == 0. */
        uint64_t want = bound;
        while (want > 0 && want % 4 == 0) want--;
        if (k % 5 == 0 && want % 3 == 0) {
            if (r != SLAB_LOOKUP_TOMBSTONE) abort();
            continue;
        }
        char exp[64];
        snprintf(exp, sizeof(exp), "%d:%llu", k, (unsigned long long)want);
        if (r != SLAB_LOOKUP_PUT || strcmp(buf, exp) != 0) abort();
    }
    return NULL;
}

TEST(SlabVlog, ConcurrentReadersSeeCommittedPrefix) {
    Shared s;
    s.log = slabVlogCreate(4, 0);
    s.published.store(0);
    s.stop.store(false);
    pthread_t threads[4];
    for (int i = 0; i < 4; i++) pthread_create(&threads[i], NULL, readerThread, &s);
    char key[32], val[64];
    for (uint64_t v = 1; v <= 600; v++) {
        if (v % 4 == 0) {
            /* An aborted batch: never published, its version is then skipped. */
            slabVlogBegin(s.log, v, 0);
            for (int k = 0; k < NKEYS; k++) {
                snprintf(key, sizeof(key), "key:%d", k);
                put(s.log, key, "aborted");
            }
            slabVlogAbort(s.log);
        } else {
            slabVlogBegin(s.log, v, 0);
            for (int k = 0; k < NKEYS; k++) {
                snprintf(key, sizeof(key), "key:%d", k);
                if (k % 5 == 0 && v % 3 == 0) {
                    put(s.log, key, NULL);
                } else {
                    snprintf(val, sizeof(val), "%d:%llu", k, (unsigned long long)v);
                    put(s.log, key, val);
                }
            }
            slabVlogCommit(s.log);
        }
        s.published.store(v, std::memory_order_release);
    }
    s.stop.store(true);
    for (int i = 0; i < 4; i++) pthread_join(threads[i], NULL);
    slabVlogFree(s.log);
}
