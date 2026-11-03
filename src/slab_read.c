/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Large slab reads off the main thread.
 *
 * A large read is dispatched by the main thread: it takes the read's snapshot (so the
 * read takes effect at the command's position), copies the keys, blocks the client
 * (BLOCKED_SLAB) and hands the task to its runners. A runner answers from the snapshot
 * and writes the RESP reply into detached chains of reply blocks; it never touches the
 * client. The main thread joins the chains to the client's output in O(1) and unblocks
 * the client.
 *
 * Runners are IO-thread step jobs (ioStepJob): an IO thread runs one bounded step of a
 * read per pass of its loop, after its client jobs, so a large read delays the other
 * clients of that thread by one step at most. An SLMGET is cut into chunks of
 * SLAB_STEP_KEYS keys, each with its own reply chain; up to one runner per active IO
 * thread takes chunks in turn, and the chains are joined in key order. SLSCAN and
 * SLCOUNT EXACT have one runner, which keeps its iterator between steps of
 * SLAB_STEP_VISITS entries. With no IO thread active, reads run inline.
 *
 * Ordering of a reply against pushes. While the read is in flight the client's output
 * goes to its deferred reply buffer, so a tracking invalidation (or a pubsub message)
 * produced meanwhile is held there. At completion the reply is joined first and the
 * deferred buffer after it, so a client never sees the invalidation of a write made
 * after its snapshot before the reply itself. Tracking connections therefore read off
 * the main thread too, except with REDIRECT, whose invalidations go to another
 * connection.
 *
 * Completion. The runner that finishes last releases the snapshot and hands the task
 * to the main thread through the IO threads' result queue; the main thread keeps
 * polling while a result is expected. */

#include "server.h"
#include "cluster_slot_stats.h"
#include "io_threads.h"
#include "slab.h"
#include "slab_read.h"

extern int ProcessingEventsWhileBlocked;

#define SLAB_ASYNC_MIN_KEYS 64      /* smaller SLMGET batches are answered inline */
#define SLAB_MAX_INFLIGHT_READS 512 /* beyond it reads run inline */
#define SLAB_STEP_KEYS 128          /* SLMGET keys per step */
#define SLAB_STEP_VISITS 512        /* SLSCAN / SLCOUNT EXACT entries per step */
#define SLAB_MAX_RUNNERS 8          /* runners of one SLMGET */

typedef enum {
    SLAB_TASK_MGET,
    SLAB_TASK_SCAN,
    SLAB_TASK_COUNT
} slabTaskKind;

typedef struct slabReadTask slabReadTask;

/* One runner of a task: takes the task's chunks one step at a time. */
typedef struct slabReadRunner {
    ioStepJob job; /* first: the IO threads see the runner as a step job */
    slabReadTask *task;
} slabReadRunner;

struct slabReadTask {
    ioResult result; /* first: the completion, on the IO-thread path */
    client *c;       /* main thread only; NULL once the client is gone */
    slabSnapshot snap;
    slabTaskKind kind;
    int resp;
    /* SLMGET: the keys, copied into one buffer. */
    unsigned char *keybuf;
    size_t *koff; /* nkeys + 1 offsets into keybuf */
    size_t nkeys;
    /* SLSCAN */
    sds start, prefix;
    long count;
    bool novalues;
    /* SLSCAN and SLCOUNT EXACT: the one runner's state between steps. */
    uint32_t prepared; /* frozen memtables whose run a step has made sure of */
    slabIter *it;
    long long found;
    replyChain entries;
    /* The reply: one chain per chunk (SLMGET), else the whole reply in chunks[0]. */
    size_t nchunks;
    replyChain *chunks;
    _Atomic size_t next_chunk;
    int nrunners;
    _Atomic int runners_left;
    slabReadRunner *runners;
    _Atomic long long run_us; /* step time, over every runner */
    monotime start_time;
    /* Set with release by the last runner; the main thread loads it with acquire
     * before reading the result. The queues order the handoff already, but this keeps
     * it explicit and visible to ThreadSanitizer. */
    _Atomic int finished;
};

static struct {
    /* Main thread only. */
    long long inflight;
    list *deferred; /* completions to retry: behind an IO thread's write, or run here */
    bool hold;      /* DEBUG SLAB HOLD-READS: dispatched reads wait in 'held' */
    list *held;
    /* Counters. */
    long long dispatched, sync_small, sync_context, sync_inflight, sync_no_thread, completions, orphaned;
} R;

static void slabReadComplete(slabReadTask *t);
static size_t slabReadDrain(void);

/* ---------------------------------------------------------------------------------------
 * Running a task (IO threads, or inline)
 * ------------------------------------------------------------------------------------- */

/* One chunk of an SLMGET; the first carries the array header. Returns whether chunks
 * may be left. */
static bool stepMget(slabReadTask *t) {
    size_t chunk = atomic_fetch_add_explicit(&t->next_chunk, 1, memory_order_relaxed);
    if (chunk >= t->nchunks) return false;
    size_t from = chunk * SLAB_STEP_KEYS, n = t->nkeys - from < SLAB_STEP_KEYS ? t->nkeys - from : SLAB_STEP_KEYS;
    serverAssert(n > 0); /* every chunk has keys; tells GCC the arrays get written */
    const unsigned char *keys[SLAB_STEP_KEYS], *vals[SLAB_STEP_KEYS];
    size_t klens[SLAB_STEP_KEYS], vlens[SLAB_STEP_KEYS];
    for (size_t i = 0; i < n; i++) {
        keys[i] = t->keybuf + t->koff[from + i];
        klens[i] = t->koff[from + i + 1] - t->koff[from + i];
    }
    slabMultiGet(&t->snap, n, keys, klens, vals, vlens);
    replyChain *rc = &t->chunks[chunk];
    if (chunk == 0) replyChainLongLongWithPrefix(rc, (long long)t->nkeys, '*');
    for (size_t i = 0; i < n; i++) {
        if (vals[i])
            replyChainBulkCBuffer(rc, vals[i], vlens[i]);
        else
            replyChainNull(rc, t->resp);
    }
    return atomic_load_explicit(&t->next_chunk, memory_order_relaxed) < t->nchunks;
}

/* The iterator of an SLSCAN or SLCOUNT EXACT, set up over several steps: each sorts at
 * most one memtable (a frozen memtable's shared run, then, creating the iterator, the
 * active memtable privately). Returns false while a setup step was taken. */
static bool iterReady(slabReadTask *t, const unsigned char *start, size_t start_len) {
    if (t->it) return true;
    if (slabSnapshotPrepareRun(&t->snap, t->prepared)) {
        t->prepared++;
        return false;
    }
    t->it = slabIterCreate(&t->snap, start, start_len, false);
    return false;
}

/* Up to SLAB_STEP_VISITS entries of an SLSCAN. Returns whether entries may be left. */
static bool stepScan(slabReadTask *t) {
    if (!iterReady(t, (unsigned char *)t->start, sdslen(t->start))) {
        if (t->it) slabIterSetBudget(t->it, (uint64_t)t->count);
        return true;
    }
    const unsigned char *k, *v;
    size_t kl, vl;
    size_t plen = t->prefix ? sdslen(t->prefix) : 0;
    for (int i = 0; i < SLAB_STEP_VISITS; i++) {
        if (!slabIterNext(t->it, &k, &kl, &v, &vl)) goto done;
        if (t->prefix && (kl < plen || memcmp(k, t->prefix, plen) != 0)) goto done;
        /* Entries first, into their own chain: the cursor precedes them on the wire. */
        replyChainBulkCBuffer(&t->entries, k, kl);
        t->found++;
        if (!t->novalues) {
            replyChainBulkCBuffer(&t->entries, v, vl);
            t->found++;
        }
    }
    return true;

done:;
    replyChain *rc = &t->chunks[0];
    replyChainLongLongWithPrefix(rc, 2, '*');
    const unsigned char *lk;
    size_t lkl;
    bool more = false;
    if (slabIterStoppedOnBudget(t->it)) {
        slabIterLastKey(t->it, &lk, &lkl);
        more = !t->prefix || (lkl >= plen && memcmp(lk, t->prefix, plen) == 0);
    }
    if (more) {
        replyChainLongLongWithPrefix(rc, (long long)lkl + 1, '$');
        replyChainProto(rc, "1", 1);
        replyChainProto(rc, (const char *)lk, lkl);
        replyChainProto(rc, "\r\n", 2);
    } else {
        replyChainProto(rc, "$1\r\n0\r\n", 7);
    }
    replyChainLongLongWithPrefix(rc, t->found, '*');
    replyChainJoin(rc, &t->entries);
    slabIterFree(t->it);
    t->it = NULL;
    return false;
}

/* Up to SLAB_STEP_VISITS entries of an SLCOUNT EXACT. */
static bool stepCount(slabReadTask *t) {
    if (!iterReady(t, NULL, 0)) return true;
    const unsigned char *k, *v;
    size_t kl, vl;
    for (int i = 0; i < SLAB_STEP_VISITS; i++) {
        if (!slabIterNext(t->it, &k, &kl, &v, &vl)) {
            replyChainLongLongWithPrefix(&t->chunks[0], t->found, ':');
            slabIterFree(t->it);
            t->it = NULL;
            return false;
        }
        t->found++;
    }
    return true;
}

/* One step of a runner, on any thread. Returns 0 once the runner is done. The last
 * runner to finish completes the task. */
static int runnerRun(slabReadRunner *rn) {
    slabReadTask *t = rn->task;
    monotime start = getMonotonicUs();
    bool more = false;
    switch (t->kind) {
    case SLAB_TASK_MGET: more = stepMget(t); break;
    case SLAB_TASK_SCAN: more = stepScan(t); break;
    case SLAB_TASK_COUNT: more = stepCount(t); break;
    }
    atomic_fetch_add_explicit(&t->run_us, (long long)(getMonotonicUs() - start), memory_order_relaxed);
    if (more) return 1;
    if (atomic_fetch_sub_explicit(&t->runners_left, 1, memory_order_acq_rel) != 1) return 0;
    /* The snapshot is dropped here; a last View reference goes to the reclaimer. */
    slabSnapshotRelease(&t->snap);
    atomic_store_explicit(&t->finished, 1, memory_order_release);
    if (inMainThread()) {
        /* Finished here (a runner the IO threads could not take): only IO threads may
         * fill their result queue, so the next drain completes it. */
        ioThreadsCancelExpectedResult();
        listAddNodeTail(R.deferred, t);
    } else {
        sendResultToMainThread(&t->result);
    }
    return 0;
}

static int runnerStep(ioStepJob *job) {
    return runnerRun((slabReadRunner *)job);
}

/* ---------------------------------------------------------------------------------------
 * Dispatch (main thread)
 * ------------------------------------------------------------------------------------- */

/* Whether a read of 'nkeys' keys (SIZE_MAX for a whole-collection read) is answered
 * off the main thread; otherwise it runs inline. Transactions, scripts and module
 * calls must run their reads inline to stay atomic; the primary's and the AOF's
 * clients never block; during loading the completion path does not run. Either way
 * the reply is the same. */
bool slabReadAsync(client *c, size_t nkeys) {
    if (server.io_threads_num <= 1) return false;
    if (nkeys < SLAB_ASYNC_MIN_KEYS) {
        R.sync_small++;
        return false;
    }
    if (c->flag.deny_blocking || c->flag.module || c->flag.fake || mustObeyClient(c) || server.loading ||
        ProcessingEventsWhileBlocked ||
        /* Tracking with REDIRECT sends invalidations on another connection, which this
         * client's deferred buffer cannot hold back behind the reply. */
        (c->flag.tracking && c->pubsub_data && c->pubsub_data->client_tracking_redirection)) {
        R.sync_context++;
        return false;
    }
    if (R.inflight >= SLAB_MAX_INFLIGHT_READS) {
        R.sync_inflight++;
        return false;
    }
    if (server.active_io_threads_num <= 1) {
        /* Runs inline now; the next large read finds an IO thread active. */
        R.sync_no_thread++;
        ioThreadsWantActive();
        return false;
    }
    return true;
}

static void slabReadResultReady(ioResult *r) {
    slabReadComplete((slabReadTask *)r);
}

static slabReadTask *newTask(client *c, slabColl *coll, slabTaskKind kind, size_t nchunks) {
    slabReadTask *t = zcalloc(sizeof(*t));
    t->result.handler = slabReadResultReady;
    t->c = c;
    t->kind = kind;
    t->resp = c->resp;
    t->nchunks = nchunks;
    t->chunks = zmalloc(nchunks * sizeof(replyChain));
    for (size_t i = 0; i < nchunks; i++) replyChainInit(&t->chunks[i]);
    if (kind == SLAB_TASK_SCAN) replyChainInit(&t->entries);
    /* The read takes effect here, at the command's position. */
    slabSnapshotTake(coll, &t->snap, true);
    return t;
}

/* Hands a task's runners to the IO threads. A runner they can't take (no active thread
 * left, or a full queue) runs here; the reply is the same. */
static void sendRunners(slabReadTask *t) {
    ioThreadsExpectResult();
    for (int i = 0; i < t->nrunners; i++)
        if (trySendStepJobToIOThreads(&t->runners[i].job) != C_OK)
            while (runnerRun(&t->runners[i]));
}

static void submit(client *c, slabReadTask *t) {
    t->start_time = getMonotonicUs();
    /* No timeout: clear one a previous blocking command left behind. */
    initClientBlockingState(c);
    c->bstate->timeout = 0;
    blockClient(c, BLOCKED_SLAB);
    /* The reply comes from the completion; the command is not executed again. */
    c->flag.pending_command = 0;
    c->bstate->slab_task = t;
    /* Output produced meanwhile (tracking invalidations, pushes) waits behind the
     * reply in the deferred buffer. */
    enableDeferredReplyBuffer(c);
    R.inflight++;
    R.dispatched++;

    if (!R.deferred) {
        R.deferred = listCreate();
        R.held = listCreate();
    }

    /* One runner per active IO thread, up to the chunks (SLMGET) or one. */
    size_t runners = (size_t)server.active_io_threads_num - 1;
    if (runners > SLAB_MAX_RUNNERS) runners = SLAB_MAX_RUNNERS;
    if (runners > t->nchunks) runners = t->nchunks;
    t->nrunners = runners ? (int)runners : 1;
    atomic_init(&t->runners_left, t->nrunners);
    t->runners = zcalloc(t->nrunners * sizeof(slabReadRunner));
    for (int i = 0; i < t->nrunners; i++) {
        t->runners[i].job.step = runnerStep;
        t->runners[i].task = t;
    }

    if (R.hold) {
        listAddNodeTail(R.held, t);
        return;
    }
    sendRunners(t);
}

void slabReadDispatchMget(client *c, slabColl *coll, int first, int count) {
    serverAssert(count > 0); /* a large read; tells GCC the sizes below are positive */
    slabReadTask *t = newTask(c, coll, SLAB_TASK_MGET, ((size_t)count + SLAB_STEP_KEYS - 1) / SLAB_STEP_KEYS);
    size_t total = 0;
    for (int j = 0; j < count; j++) total += sdslen(objectGetVal(c->argv[first + j]));
    t->nkeys = (size_t)count;
    t->keybuf = zmalloc(total ? total : 1);
    t->koff = zmalloc((count + 1) * sizeof(size_t));
    size_t off = 0;
    for (int j = 0; j < count; j++) {
        sds k = objectGetVal(c->argv[first + j]);
        t->koff[j] = off;
        memcpy(t->keybuf + off, k, sdslen(k));
        off += sdslen(k);
    }
    t->koff[count] = off;
    submit(c, t);
}

void slabReadDispatchScan(client *c, slabColl *coll, sds start, sds prefix, long count, bool novalues) {
    slabReadTask *t = newTask(c, coll, SLAB_TASK_SCAN, 1);
    t->start = sdsdup(start);
    t->prefix = prefix ? sdsdup(prefix) : NULL;
    t->count = count;
    t->novalues = novalues;
    submit(c, t);
}

void slabReadDispatchCount(client *c, slabColl *coll) {
    submit(c, newTask(c, coll, SLAB_TASK_COUNT, 1));
}

/* ---------------------------------------------------------------------------------------
 * Completion (main thread)
 * ------------------------------------------------------------------------------------- */

static void freeTask(slabReadTask *t) {
    for (size_t i = 0; i < t->nchunks; i++) replyChainRelease(&t->chunks[i]);
    zfree(t->chunks);
    if (t->kind == SLAB_TASK_SCAN) replyChainRelease(&t->entries);
    zfree(t->runners);
    zfree(t->keybuf);
    zfree(t->koff);
    sdsfree(t->start);
    sdsfree(t->prefix);
    zfree(t);
}

/* Joins the reply to the client, then the output held meanwhile, and unblocks it. */
static void slabReadComplete(slabReadTask *t) {
    serverAssert(atomic_load_explicit(&t->finished, memory_order_acquire));
    client *c = t->c;
    R.completions++;
    if (!c) {
        R.orphaned++;
        R.inflight--;
        freeTask(t);
        return;
    }
    if (c->io_write_state == CLIENT_PENDING_IO) {
        /* An IO thread owns the client's output; try again at the next drain (the
         * event loop does not sleep meanwhile, see slabReadHasDeferred). */
        listAddNodeTail(R.deferred, t);
        return;
    }
    monotime reply_start = getMonotonicUs();
    c->bstate->slab_task = NULL;
    /* The chunks in key order. */
    replyChain *reply = &t->chunks[0];
    for (size_t i = 1; i < t->nchunks; i++) replyChainJoin(reply, &t->chunks[i]);
    /* The reply goes ahead of the output held in the deferred buffer meanwhile. The
     * command already ran (afterCommand counted its slot's output then), so the
     * reply's bytes are counted for the slot here. */
    size_t bytes = reply->used;
    addReplyChain(c, reply);
    clusterSlotStatsAddNetworkBytesOutForSlot(c->slot, bytes);
    commitDeferredReplyBuffer(c, 0);
    updateStatsOnUnblock(c, (long)atomic_load_explicit(&t->run_us, memory_order_relaxed),
                         (long)(getMonotonicUs() - reply_start), 0);
    unblockClient(c, 1);
    R.inflight--;
    freeTask(t);
}

/* Completions deferred behind an IO-thread write, in order. */
static size_t slabReadDrain(void) {
    if (!slabReadHasDeferred()) return 0;
    list *retry = R.deferred;
    size_t n = listLength(retry);
    R.deferred = listCreate();
    listIter li;
    listNode *ln;
    listRewind(retry, &li);
    while ((ln = listNext(&li))) slabReadComplete(listNodeValue(ln));
    listRelease(retry);
    return n;
}

/* beforeSleep: completions that waited for an IO thread's write. */
void slabReadBeforeSleep(void) {
    slabReadDrain();
}

/* The client of an in-flight read is being unblocked by something else (freed,
 * disconnected): the task finishes, but its reply is dropped. */
void slabReadClientGone(client *c) {
    slabReadTask *t = c->bstate->slab_task;
    if (!t) return;
    t->c = NULL;
    c->bstate->slab_task = NULL;
    /* A task deferred behind an IO write holds no client either. Output held back for
     * the reply (an error from a forced unblock, pushes) goes out now instead. */
    commitDeferredReplyBuffer(c, 0);
}

/* Completions wait behind IO-thread writes: beforeSleep must not block in poll, since
 * no event may come to retry them. */
bool slabReadHasDeferred(void) {
    return R.deferred && listLength(R.deferred) > 0;
}

/* Held reads are dispatched (snapshot taken, client blocked) but not handed to the IO
 * threads until released, so no IO thread holds a job that can't finish (a drain of the
 * IO threads waits for every job). */
void slabReadSetHold(bool hold) {
    R.hold = hold;
    if (hold || !R.held) return;
    while (listLength(R.held)) {
        listNode *ln = listFirst(R.held);
        slabReadTask *t = listNodeValue(ln);
        listDelNode(R.held, ln);
        sendRunners(t);
    }
}

sds slabReadInfo(sds info, bool debug) {
    if (!debug)
        return sdscatprintf(info, "slab_reads_inflight:%lld\r\n", R.inflight);
    return sdscatprintf(info,
                        "slab_reads_dispatched:%lld\r\n"
                        "slab_reads_completed:%lld\r\n"
                        "slab_reads_orphaned:%lld\r\n"
                        "slab_reads_inline_small:%lld\r\n"
                        "slab_reads_inline_context:%lld\r\n"
                        "slab_reads_inline_inflight:%lld\r\n"
                        "slab_reads_inline_no_thread:%lld\r\n",
                        R.dispatched, R.completions, R.orphaned, R.sync_small, R.sync_context, R.sync_inflight,
                        R.sync_no_thread);
}
