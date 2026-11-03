/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Slab maintenance: the threads that run flush and compaction units, the hand-off of
 * their results to the main thread, the reclaimer, and the fork gate.
 *
 * Scheduling. A collection with work is queued once (state IDLE -> QUEUED under the
 * scheduler lock). A maintenance thread takes it (RUNNING), runs one bounded unit and
 * requeues it if more may be needed; at most one unit of a collection runs at a time,
 * so a collection's builder sees a linear history and each delta log has one writer.
 *
 * Inputs. A unit never loads the collection's published View. On each
 * notify the main thread puts a retained View in the collection's mailbox, under the
 * scheduler lock; a unit takes its own reference while holding that lock, which is
 * safe because the mailbox's reference cannot be dropped meanwhile.
 *
 * Results. A unit pushes its install request to the collection and wakes the main
 * thread through the wake pipe (once per burst); the main thread applies it and
 * notifies the collection again.
 *
 * Data-parallel units. A unit stays the only one of its collection, but its leaf-local
 * work (a flush's appends to disjoint leaves, a compaction's runs) can be split into
 * chunks: the unit posts the job in the help slot, idle maintenance threads join it and
 * every worker takes chunks from one atomic counter. A chunk enters and leaves its own
 * fork section, and no worker waits for another inside a section, so a pending fork
 * never deadlocks against the join.
 *
 * Reclamation. A View whose last reference drops anywhere is handed to the reclaimer
 * and freed by a maintenance thread between units, so neither the main thread nor a
 * query thread frees large structures. While a fork child exists, nothing is freed
 * and no compaction starts, so the parent dirties as few shared pages as possible.
 *
 * The threads start with the first unit, so a server without slab collections runs
 * none. With zero threads (configured, or none could start), units run inline on the
 * main thread at notify. */

#include "server.h"
#include "slab_int.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define SLAB_SCHED_IDLE 0
#define SLAB_SCHED_QUEUED 1
#define SLAB_SCHED_RUNNING 2
#define RECLAIM_BATCH 64
#define FORK_PREPARE_LIMIT_US 2000000

static struct {
    pthread_mutex_t lock;
    pthread_cond_t cond; /* idle maintenance threads wait here */
    int wanted;          /* threads the first unit starts (slabMaintConfigure) */
    bool started;
    /* Running threads; 0 runs units inline. Set once by the main thread, with release
     * after the lock, queues and pipe exist, and read with acquire on other threads. */
    _Atomic int nthreads;
    pthread_t *threads;
    bool stop;
    int idle;
    /* Collections with work, FIFO; each queued entry holds a collection reference. */
    slabColl **queue;
    size_t qhead, qlen, qcap;
    /* Views to free. */
    slabView **reclaim;
    size_t nreclaim, reclaim_cap;
    /* Collections whose compaction waited for a fork child to exit; each holds a
     * reference, and the main thread notifies them when the child is gone. */
    slabColl **deferred;
    size_t ndeferred, deferred_cap;
    /* Collections with install requests for the main thread; each holds a reference. */
    slabColl **ready;
    size_t nready, ready_cap;
    /* Main-thread wake. */
    int wake_fd[2];
    _Atomic int wake_pending;
    /* Fork gate and child state. */
    _Atomic int fork_pending;
    _Atomic int sections;
    _Atomic int child_active;
    bool child_image_verified;
    _Atomic int hold_sections; /* DEBUG SLAB HOLD-SECTIONS: maintenance threads stop inside a section */
    _Atomic int sections_held;
    bool paused;             /* DEBUG SLAB PAUSE-MAINTENANCE: no unit starts (under lock) */
    struct slabParJob *help; /* a data-parallel job idle threads may join (under lock) */
    /* Counters for INFO. */
    _Atomic uint64_t units, installs, fork_waits, fork_prepare_timeouts, reclaimed, helper_chunks;
} M = {.lock = PTHREAD_MUTEX_INITIALIZER, .cond = PTHREAD_COND_INITIALIZER, .wake_fd = {-1, -1}, .child_image_verified = true};

static int nthreads(void) {
    return atomic_load_explicit(&M.nthreads, memory_order_acquire);
}

slabInstall *slabUnitRun(slabColl *c, slabView *v, bool allow_compaction);
void slabInstallApply(slabColl *c, slabInstall *in);
void slabInstallFree(slabInstall *in);

/* ---------------------------------------------------------------------------------------
 * Fork gate
 * ------------------------------------------------------------------------------------- */

/* Every in-place publication a fork child can read (delta-log appends, the store of a
 * frozen memtable's sorted run) runs inside a section. A pending fork waits for running
 * sections and keeps new ones out; seq_cst on both sides makes it a Dekker handshake:
 * either the forking thread sees the section, or the section sees the pending fork. */
void slabSectionEnter(void) {
    for (;;) {
        while (atomic_load(&M.fork_pending)) sched_yield();
        atomic_fetch_add(&M.sections, 1);
        if (!atomic_load(&M.fork_pending)) break;
        atomic_fetch_sub(&M.sections, 1);
    }
    if (atomic_load_explicit(&M.hold_sections, memory_order_relaxed) && !onServerMainThread()) {
        atomic_fetch_add(&M.sections_held, 1);
        while (atomic_load(&M.hold_sections)) usleep(1000);
        atomic_fetch_sub(&M.sections_held, 1);
    }
}

void slabSectionLeave(void) {
    atomic_fetch_sub(&M.sections, 1);
}

/* Lets a pending fork proceed between two publications of a long section. */
void slabSectionYield(void) {
    if (!atomic_load_explicit(&M.fork_pending, memory_order_relaxed)) return;
    slabSectionLeave();
    slabSectionEnter();
}

static long long nowUs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

/* Called by the forking (main) thread right before fork(). Returns false if running
 * sections did not finish in time; the child then refuses to save slab collections.
 * The child can't save from such an image: fork() write-protects the pages one at a
 * time while other threads keep storing, so the image need not respect the release
 * order a concurrent reader relies on (a slot can be there without its record). */
bool slabForkPrepare(void) {
    atomic_fetch_add(&M.fork_pending, 1);
    if (atomic_load(&M.sections) == 0) return true;
    atomic_fetch_add_explicit(&M.fork_waits, 1, memory_order_relaxed);
    long long deadline = nowUs() + FORK_PREPARE_LIMIT_US;
    int spins = 0;
    while (atomic_load(&M.sections) != 0) {
        if (nowUs() > deadline) {
            atomic_fetch_add_explicit(&M.fork_prepare_timeouts, 1, memory_order_relaxed);
            return false;
        }
        if (++spins < 64)
            sched_yield();
        else
            usleep(20);
    }
    return true;
}

/* After fork(), in both processes. In the child, no other thread exists, and the
 * image is trusted only if the prepare step saw no section running. */
void slabForkDone(bool child, bool prepared) {
    if (child) {
        M.child_image_verified = M.child_image_verified && prepared;
        atomic_store_explicit(&M.nthreads, 0, memory_order_relaxed);
        atomic_store(&M.sections, 0);
        atomic_store(&M.fork_pending, 0);
    } else {
        atomic_fetch_sub(&M.fork_pending, 1);
    }
}

bool slabForkImageVerified(void) {
    return M.child_image_verified;
}

void slabMaintSetSectionHold(bool hold) {
    atomic_store(&M.hold_sections, hold);
}

int slabMaintSectionsHeld(void) {
    return atomic_load(&M.sections_held);
}

/* Main thread, when a fork child starts or exits. */
void slabSetChildActive(bool active) {
    atomic_store(&M.child_active, active);
    if (!active && nthreads()) {
        pthread_mutex_lock(&M.lock);
        if (M.nreclaim || M.qlen) pthread_cond_broadcast(&M.cond);
        slabColl **deferred = M.deferred;
        size_t n = M.ndeferred;
        M.deferred = NULL;
        M.ndeferred = M.deferred_cap = 0;
        pthread_mutex_unlock(&M.lock);
        /* Compaction skipped while the child ran: give those collections a turn. */
        for (size_t i = 0; i < n; i++) {
            slabMaintNotify(deferred[i]);
            slabCollRelease(deferred[i]);
        }
        zfree(deferred);
    }
}

bool slabChildActive(void) {
    return atomic_load_explicit(&M.child_active, memory_order_relaxed);
}

/* ---------------------------------------------------------------------------------------
 * Data-parallel jobs
 * ------------------------------------------------------------------------------------- */

typedef struct slabParJob {
    void (*chunk)(void *ctx, size_t idx);
    void *ctx;
    size_t nchunks;
    _Atomic size_t next; /* the next chunk to take */
    int joined;          /* helpers that joined (under M.lock) */
    _Atomic int left;    /* helpers done with the job */
} slabParJob;

/* Takes and runs chunks until none is left; returns how many this worker ran. */
static size_t parWork(slabParJob *job) {
    size_t i, ran = 0;
    while ((i = atomic_fetch_add_explicit(&job->next, 1, memory_order_relaxed)) < job->nchunks) {
        job->chunk(job->ctx, i);
        ran++;
    }
    return ran;
}

/* Runs chunk(ctx, 0 .. nchunks-1), on the calling maintenance thread and on idle ones,
 * and returns when every chunk is done. Whatever the chunks wrote is visible to the
 * caller then. The caller must not be inside a fork section. */
void slabParRun(void (*chunk)(void *ctx, size_t idx), void *ctx, size_t nchunks) {
    slabParJob job = {.chunk = chunk, .ctx = ctx, .nchunks = nchunks};
    atomic_init(&job.next, 0);
    atomic_init(&job.left, 0);
    bool posted = false;
    if (nthreads() > 1 && nchunks > 1) {
        /* Posting under the lock orders the job's inputs before any helper's reads. */
        pthread_mutex_lock(&M.lock);
        if (!M.help && M.idle > 0) {
            M.help = &job;
            posted = true;
            pthread_cond_broadcast(&M.cond);
        }
        pthread_mutex_unlock(&M.lock);
    }
    parWork(&job);
    if (!posted) return;
    /* Retract the job: after this no helper can join, so 'joined' is final. */
    pthread_mutex_lock(&M.lock);
    if (M.help == &job) M.help = NULL;
    int joined = job.joined;
    pthread_mutex_unlock(&M.lock);
    /* Acquire: pairs with each helper's release, so their chunks' writes are visible. */
    int spins = 0;
    while (atomic_load_explicit(&job.left, memory_order_acquire) != joined) {
        if (++spins < 64)
            sched_yield();
        else
            usleep(10);
    }
}

/* ---------------------------------------------------------------------------------------
 * Reclamation
 * ------------------------------------------------------------------------------------- */

void slabReclaimView(slabView *v) {
    if (nthreads() == 0) {
        slabViewFree(v);
        return;
    }
    pthread_mutex_lock(&M.lock);
    if (M.nreclaim == M.reclaim_cap) {
        M.reclaim_cap = M.reclaim_cap ? M.reclaim_cap * 2 : 64;
        M.reclaim = zrealloc(M.reclaim, M.reclaim_cap * sizeof(slabView *));
    }
    M.reclaim[M.nreclaim++] = v;
    /* Wake a thread only if none is busy: a busy one drains after its unit. */
    if (M.idle == nthreads()) pthread_cond_signal(&M.cond);
    pthread_mutex_unlock(&M.lock);
}

uint64_t slabReclaimQueueLen(void) {
    pthread_mutex_lock(&M.lock);
    uint64_t n = M.nreclaim;
    pthread_mutex_unlock(&M.lock);
    return n;
}

/* ---------------------------------------------------------------------------------------
 * Scheduling
 * ------------------------------------------------------------------------------------- */

static void queuePush(slabColl *c) {
    if (M.qlen == M.qcap) {
        size_t ncap = M.qcap ? M.qcap * 2 : 64;
        slabColl **nq = zmalloc(ncap * sizeof(slabColl *));
        for (size_t i = 0; i < M.qlen; i++) nq[i] = M.queue[(M.qhead + i) % M.qcap];
        zfree(M.queue);
        M.queue = nq;
        M.qcap = ncap;
        M.qhead = 0;
    }
    M.queue[(M.qhead + M.qlen) % M.qcap] = c;
    M.qlen++;
}

static slabColl *queuePop(void) {
    slabColl *c = M.queue[M.qhead];
    M.qhead = (M.qhead + 1) % M.qcap;
    M.qlen--;
    return c;
}

static void *maintThread(void *arg);

/* Main thread, at startup: the number of threads the first unit starts. */
void slabMaintConfigure(int threads) {
    M.wanted = threads;
}

static void slabMaintWakeReadable(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(privdata);
    UNUSED(mask);
    char buf[64];
    while (read(M.wake_fd[0], buf, sizeof(buf)) > 0) {
    }
    /* Clients waiting for maintenance are answered from beforeSleep, which follows. */
    slabMaintDrain();
}

/* The first unit starts the threads and the pipe that wakes the main thread when they
 * finish one. If none can start, units run inline. */
static void startThreads(void) {
    M.started = true;
    if (M.wanted == 0) return;
    if (pipe(M.wake_fd) == -1) goto fail;
    for (int i = 0; i < 2; i++) {
        fcntl(M.wake_fd[i], F_SETFL, fcntl(M.wake_fd[i], F_GETFL) | O_NONBLOCK);
        fcntl(M.wake_fd[i], F_SETFD, FD_CLOEXEC);
    }
    if (aeCreateFileEvent(server.el, M.wake_fd[0], AE_READABLE, slabMaintWakeReadable, NULL) == AE_ERR) goto fail;
    M.threads = zcalloc(M.wanted * sizeof(pthread_t));
    int started = 0;
    while (started < M.wanted && pthread_create(&M.threads[started], NULL, maintThread, (void *)(intptr_t)started) == 0)
        started++;
    if (started < M.wanted) serverLog(LL_WARNING, "Started %d of %d slab maintenance threads", started, M.wanted);
    if (started == 0) goto fail;
    atomic_store_explicit(&M.nthreads, started, memory_order_release);
    return;
fail:
    serverLog(LL_WARNING, "Can't start the slab maintenance threads: maintenance runs on the main thread");
}

/* Common setup of a slab thread, as bio.c does for its own. */
void slabThreadSetup(const char *name, const char *cpulist) {
    valkey_set_thread_title(name);
    serverSetCpuAffinity(cpulist);
    makeThreadKillable();
    /* The watchdog's SIGALRM is for the main thread only. */
    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    int err = pthread_sigmask(SIG_BLOCK, &sigset, NULL);
    if (err) serverLog(LL_WARNING, "Warning: can't mask SIGALRM in a slab thread: %s", strerror(err));
}

/* killThreads(), before the crash memory test. */
void slabThreadKill(pthread_t tid, const char *name) {
    if (pthread_equal(tid, pthread_self())) return;
    int err;
    if (pthread_cancel(tid) == 0) {
        if ((err = pthread_join(tid, NULL)) != 0)
            serverLog(LL_WARNING, "Slab %s thread can't be joined: %s", name, strerror(err));
        else
            serverLog(LL_WARNING, "Slab %s thread terminated", name);
    }
}

void slabMaintKillThreads(void) {
    for (int i = 0; i < nthreads(); i++) slabThreadKill(M.threads[i], "maintenance");
}

static void runInline(slabColl *c) {
    slabInstall *in;
    while ((in = slabUnitRun(c, c->view, true))) slabInstallApply(c, in);
}

/* Main thread: the collection may have work (a freeze, an alter, an install, an
 * explicit request). Hands the current View to maintenance and queues the collection. */
void slabMaintNotify(slabColl *c) {
    if (atomic_load_explicit(&c->detached, memory_order_acquire)) return;
    if (!M.started) startThreads();
    if (nthreads() == 0) {
        runInline(c);
        return;
    }
    pthread_mutex_lock(&M.lock);
    slabView *old = c->maint_view;
    c->maint_view = c->view;
    slabViewRetain(c->view);
    c->sched_rerun = true;
    if (c->sched == SLAB_SCHED_IDLE) {
        c->sched = SLAB_SCHED_QUEUED;
        slabCollRetain(c);
        queuePush(c);
        pthread_cond_signal(&M.cond);
    }
    pthread_mutex_unlock(&M.lock);
    if (old) slabViewRelease(old);
}

/* Pushes an install request (maintenance thread) and wakes the main thread once per
 * burst. The pipe write happens-after the push: the main thread resets
 * wake_pending before draining, so a request pushed after a drain saw nothing finds the
 * flag clear and writes the pipe. */
static void pushInstall(slabColl *c, slabInstall *in) {
    atomic_fetch_add(&c->installs_outstanding, 1);
    pthread_mutex_lock(&c->install_lock);
    in->next = NULL;
    if (c->installs_tail)
        c->installs_tail->next = in;
    else
        c->installs_head = in;
    c->installs_tail = in;
    pthread_mutex_unlock(&c->install_lock);
    pthread_mutex_lock(&M.lock);
    if (M.nready == M.ready_cap) {
        M.ready_cap = M.ready_cap ? M.ready_cap * 2 : 64;
        M.ready = zrealloc(M.ready, M.ready_cap * sizeof(slabColl *));
    }
    slabCollRetain(c);
    M.ready[M.nready++] = c;
    pthread_mutex_unlock(&M.lock);
    atomic_thread_fence(memory_order_seq_cst);
    if (!atomic_exchange(&M.wake_pending, 1)) {
        ssize_t w = write(M.wake_fd[1], "s", 1);
        (void)w;
    }
}

static void *maintThread(void *arg) {
    char name[16];
    snprintf(name, sizeof(name), "slab_maint_%d", (int)(intptr_t)arg);
    slabThreadSetup(name, server.bio_cpulist);
    pthread_mutex_lock(&M.lock);
    while (!M.stop) {
        bool child = slabChildActive();
        if (M.nreclaim && !child) {
            size_t n = M.nreclaim < RECLAIM_BATCH ? M.nreclaim : RECLAIM_BATCH;
            slabView *batch[RECLAIM_BATCH];
            M.nreclaim -= n;
            memcpy(batch, M.reclaim + M.nreclaim, n * sizeof(slabView *));
            pthread_mutex_unlock(&M.lock);
            for (size_t i = 0; i < n; i++) slabViewFree(batch[i]);
            atomic_fetch_add_explicit(&M.reclaimed, n, memory_order_relaxed);
            pthread_mutex_lock(&M.lock);
            continue;
        }
        if (M.qlen && !M.paused) {
            slabColl *c = queuePop();
            c->sched = SLAB_SCHED_RUNNING;
            c->sched_rerun = false;
            slabView *v = c->maint_view;
            if (v) slabViewRetain(v); /* safe: the mailbox holds a reference under the lock */
            pthread_mutex_unlock(&M.lock);

            slabInstall *in = NULL;
            bool detached = atomic_load_explicit(&c->detached, memory_order_acquire);
            if (v && !detached && atomic_load(&c->installs_outstanding) < SLAB_MAX_OUTSTANDING_INSTALLS) {
                in = slabUnitRun(c, v, !child);
                atomic_fetch_add_explicit(&M.units, 1, memory_order_relaxed);
            }
            if (v) slabViewRelease(v);
            if (in) pushInstall(c, in);

            pthread_mutex_lock(&M.lock);
            if (child && !detached && atomic_load_explicit(&c->compact_pending, memory_order_relaxed)) {
                if (M.ndeferred == M.deferred_cap) {
                    M.deferred_cap = M.deferred_cap ? M.deferred_cap * 2 : 8;
                    M.deferred = zrealloc(M.deferred, M.deferred_cap * sizeof(slabColl *));
                }
                slabCollRetain(c);
                M.deferred[M.ndeferred++] = c;
            }
            if ((in || c->sched_rerun) && !detached &&
                atomic_load(&c->installs_outstanding) < SLAB_MAX_OUTSTANDING_INSTALLS) {
                /* More may be pending: requeue behind the others. */
                c->sched = SLAB_SCHED_QUEUED;
                c->sched_rerun = false;
                queuePush(c);
                continue; /* the queue keeps the reference */
            }
            c->sched = SLAB_SCHED_IDLE;
            pthread_mutex_unlock(&M.lock);
            slabCollRelease(c);
            pthread_mutex_lock(&M.lock);
            continue;
        }
        slabParJob *job = M.help;
        if (job && atomic_load_explicit(&job->next, memory_order_relaxed) < job->nchunks) {
            job->joined++;
            pthread_mutex_unlock(&M.lock);
            size_t ran = parWork(job);
            atomic_fetch_add_explicit(&M.helper_chunks, ran, memory_order_relaxed);
            /* Release: this helper's chunk results before the unit thread reads them.
             * The job lives on the unit thread's stack: no access after this. */
            atomic_fetch_add_explicit(&job->left, 1, memory_order_release);
            pthread_mutex_lock(&M.lock);
            continue;
        }
        M.idle++;
        pthread_cond_wait(&M.cond, &M.lock);
        M.idle--;
    }
    pthread_mutex_unlock(&M.lock);
    return NULL;
}

/* Main thread: applies the install requests maintenance produced (wake handler and
 * beforeSleep). Returns the number applied. */
size_t slabMaintDrain(void) {
    /* Nothing was pushed since the last drain (pushInstall sets the flag after its
     * push, then writes the pipe, which brings the event loop back here). */
    if (!atomic_load_explicit(&M.wake_pending, memory_order_acquire)) return 0;
    atomic_store(&M.wake_pending, 0);
    atomic_thread_fence(memory_order_seq_cst);
    pthread_mutex_lock(&M.lock);
    size_t n = M.nready;
    slabColl **ready = NULL;
    if (n) {
        ready = M.ready;
        M.ready = NULL;
        M.nready = M.ready_cap = 0;
    }
    pthread_mutex_unlock(&M.lock);
    size_t applied = 0;
    for (size_t i = 0; i < n; i++) {
        slabColl *c = ready[i];
        for (;;) {
            pthread_mutex_lock(&c->install_lock);
            slabInstall *in = c->installs_head;
            if (in) {
                c->installs_head = in->next;
                if (!c->installs_head) c->installs_tail = NULL;
            }
            pthread_mutex_unlock(&c->install_lock);
            if (!in) break;
            if (atomic_load_explicit(&c->detached, memory_order_acquire))
                slabInstallFree(in);
            else
                slabInstallApply(c, in);
            atomic_fetch_sub(&c->installs_outstanding, 1);
            atomic_fetch_add_explicit(&M.installs, 1, memory_order_relaxed);
            applied++;
        }
        slabMaintNotify(c);
        slabCollRelease(c);
    }
    zfree(ready);
    return applied;
}

void slabMaintStats(uint64_t *units, uint64_t *installs, uint64_t *fork_waits, uint64_t *fork_timeouts, uint64_t *reclaimed) {
    *units = atomic_load_explicit(&M.units, memory_order_relaxed);
    *installs = atomic_load_explicit(&M.installs, memory_order_relaxed);
    *fork_waits = atomic_load_explicit(&M.fork_waits, memory_order_relaxed);
    *fork_timeouts = atomic_load_explicit(&M.fork_prepare_timeouts, memory_order_relaxed);
    *reclaimed = atomic_load_explicit(&M.reclaimed, memory_order_relaxed);
}

int slabMaintThreads(void) {
    return nthreads();
}

/* DEBUG SLAB PAUSE-MAINTENANCE / RESUME-MAINTENANCE: tests use it to hold work queued
 * (backpressure, settle gauges). Running units finish. */
void slabMaintSetPaused(bool paused) {
    pthread_mutex_lock(&M.lock);
    M.paused = paused;
    if (!paused) pthread_cond_broadcast(&M.cond);
    pthread_mutex_unlock(&M.lock);
}
