/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* The slab data type: a collection of many small entries under one key, read in
 * batches. The engine is in slab.c and slab_tree.c; this file has the commands and
 * the object glue. */

#include "server.h"
#include "slab.h"
#include "slab_block.h"
#include "slab_hash.h"

#include <math.h>

/* "SLCREATE key NX" and every option as a name and a value. */
#define SLAB_CREATE_ARGV_MAX (3 + 2 * SLAB_CONFIG_PAIRS)
static int slabCreateArgv(robj *key, const slabConfig *cfg, robj **argv);

/* ---------------------------------------------------------------------------------------
 * Objects
 * ------------------------------------------------------------------------------------- */

robj *createSlabObject(const slabConfig *cfg) {
    robj *o = createObject(OBJ_SLAB, slabCollCreate(cfg));
    o->encoding = OBJ_ENCODING_SLAB;
    return o;
}

void freeSlabObject(robj *o) {
    slabCollDetach(objectGetVal(o));
}

robj *slabTypeDup(robj *o) {
    robj *n = createObject(OBJ_SLAB, slabCollCopy(objectGetVal(o)));
    n->encoding = OBJ_ENCODING_SLAB;
    return n;
}

size_t slabTypeMemUsage(robj *o) {
    return slabCollMemUsage(objectGetVal(o));
}

size_t slabTypeLength(robj *o) {
    return (size_t)slabCollCountEstimate(objectGetVal(o));
}

size_t slabTypeFreeEffort(robj *o) {
    return slabCollFreeEffort(objectGetVal(o));
}

/* DEBUG DIGEST: the live entries in key order, independent of how maintenance laid
 * them out, so a primary and its replicas agree. */
void slabTypeDigest(unsigned char *digest, robj *o) {
    slabSnapshot s;
    slabSnapshotTake(objectGetVal(o), &s, false);
    slabIter *it = slabIterCreate(&s, NULL, 0, false);
    const unsigned char *k, *v;
    size_t kl, vl;
    while (slabIterNext(it, &k, &kl, &v, &vl)) {
        mixDigest(digest, k, kl);
        mixDigest(digest, v, vl);
    }
    slabIterFree(it);
}

/* The snapshot a save reads (RDB, AOF rewrite, DUMP, slot migration). On the main
 * thread, including a fork child, which is an image of it, that is the published
 * state. A forkless save runs on another thread and reads the View pinned on the main
 * thread when bgiteration handed the key out; it never loads the published pointer
 * itself, which an install may be replacing. */
bool slabTypeSnapshotForSave(robj *o, slabSnapshot *s) {
    slabColl *c = objectGetVal(o);
    if (!onServerMainThread()) {
        if (slabPinnedSnapshot(c, s)) return true;
        /* Unpinned off the main thread: a module thread holding the GIL (DUMP or DEBUG
         * OBJECT through the module API). The main thread cannot install meanwhile, so
         * the published state is as stable as on the main thread. */
        slabSnapshotTake(c, s, false);
        return true;
    }
    /* A fork child whose parent had an in-place publication running at fork time. */
    if (server.in_fork_child && !slabForkImageVerified()) return false;
    slabSnapshotTake(c, s, false);
    return true;
}

void slabTypePinForIteration(robj *o) {
    slabPinForIteration(objectGetVal(o));
}

void slabTypeUnpinForIteration(robj *o) {
    slabUnpinForIteration(objectGetVal(o));
}

#define AOF_SLAB_BATCH_PAIRS 1000
#define AOF_SLAB_BATCH_BYTES (4 * 1024 * 1024)

static int rioWriteArgv(rio *r, robj **argv, int argc) {
    if (rioWriteBulkCount(r, '*', argc) == 0) return 0;
    for (int j = 0; j < argc; j++) {
        sds a = objectGetVal(argv[j]);
        if (rioWriteBulkString(r, a, sdslen(a)) == 0) return 0;
    }
    return 1;
}

/* AOF rewrite (and atomic slot migration): SLCREATE key NX with every option, then
 * the live entries as SLMSET batches of at most AOF_SLAB_BATCH_PAIRS pairs or
 * AOF_SLAB_BATCH_BYTES. Returns 0 on error. */
int rewriteSlabObject(rio *r, robj *key, robj *o) {
    slabSnapshot s;
    if (!slabTypeSnapshotForSave(o, &s)) {
        serverLog(LL_WARNING, "Can't rewrite a slab collection: its state in this process is not verified");
        return 0;
    }
    robj *argv[SLAB_CREATE_ARGV_MAX];
    int argc = slabCreateArgv(key, slabSnapshotConfig(&s), argv);
    int ok = rioWriteArgv(r, argv, argc);
    for (int j = 0; j < argc; j++) decrRefCount(argv[j]);
    if (!ok) return 0;

    slabIter *it = slabIterCreate(&s, NULL, 0, true);
    sds keys[AOF_SLAB_BATCH_PAIRS];
    const unsigned char *vals[AOF_SLAB_BATCH_PAIRS];
    size_t vlens[AOF_SLAB_BATCH_PAIRS];
    int n = 0;
    size_t bytes = 0;
    const unsigned char *k, *v;
    size_t kl, vl;
    bool more = true;
    sds keystr = objectGetVal(key);
    while (ok && more) {
        more = slabIterNext(it, &k, &kl, &v, &vl);
        if (more) {
            /* Values stay valid while the snapshot does; keys live in the iterator. */
            keys[n] = sdsnewlen(k, kl);
            vals[n] = v;
            vlens[n] = vl;
            n++;
            bytes += kl + vl;
        }
        if (n && (!more || n == AOF_SLAB_BATCH_PAIRS || bytes >= AOF_SLAB_BATCH_BYTES)) {
            ok = rioWriteBulkCount(r, '*', 2 + 2 * n) && rioWriteBulkString(r, "SLMSET", 6) &&
                 rioWriteBulkString(r, keystr, sdslen(keystr));
            for (int j = 0; j < n; j++) {
                if (ok) ok = rioWriteBulkString(r, keys[j], sdslen(keys[j])) && rioWriteBulkString(r, (const char *)vals[j], vlens[j]);
                sdsfree(keys[j]);
            }
            n = 0;
            bytes = 0;
        }
    }
    for (int j = 0; j < n; j++) sdsfree(keys[j]);
    slabIterFree(it);
    return ok;
}

/* ---------------------------------------------------------------------------------------
 * Maintenance threads and waiting clients
 * ------------------------------------------------------------------------------------- */

/* A client blocked in SLFLUSH WAIT or SLCOMPACT WAIT. */
typedef struct slabWaiter {
    client *c;
    slabColl *coll; /* retained */
    bool compaction;
    uint64_t target;
    long long reply;
} slabWaiter;

static list *slab_waiters;

/* Two threads from 8 cores, else one: a single thread is 11 % slower than three on
 * sustained batch writes at 32 connections. */
static int slabDefaultMaintenanceThreads(void) {
    return sysconf(_SC_NPROCESSORS_ONLN) >= 8 ? 2 : 1;
}

/* At startup. Threads start later, with the first unit. */
void slabInitServer(void) {
    slab_waiters = listCreate();
    /* A random seed keeps clients from choosing keys that collide in the tables. */
    getRandomBytes((unsigned char *)&slab_hash_seed, sizeof(slab_hash_seed));
    slabMaintConfigure(server.slab_maintenance_threads < 0 ? slabDefaultMaintenanceThreads() : server.slab_maintenance_threads);
}

void slabKillThreads(void) {
    slabMaintKillThreads();
}

static void replyToWaiter(slabWaiter *w, bool timedout) {
    client *c = w->c;
    if (timedout)
        addReplyNull(c);
    else if (slabCollDetached(w->coll))
        addReplyError(c, "slab collection was deleted while waiting");
    else
        addReplyLongLong(c, w->reply);
}

/* Applies finished maintenance and answers the clients waiting for it. Main thread:
 * from the wake pipe and from beforeSleep (a backstop). */
void slabBeforeSleep(void) {
    slabMaintDrain();
    if (!slab_waiters || listLength(slab_waiters) == 0) return;
    listIter li;
    listNode *ln;
    listRewind(slab_waiters, &li);
    while ((ln = listNext(&li))) {
        slabWaiter *w = listNodeValue(ln);
        client *c = w->c;
        bool gone = slabCollDetached(w->coll);
        bool done = w->compaction ? slabCompactionDone(w->coll, w->target) : slabFlushDone(w->coll, w->target);
        if (!gone && !done) continue;
        replyToWaiter(w, false);
        updateStatsOnUnblock(c, 0, 0, gone ? ERROR_COMMAND_FAILED : 0);
        unblockClient(c, 1); /* removes the waiter (slabUnblockClient) */
    }
}

/* The WAIT of 'c' timed out (or CLIENT UNBLOCK TIMEOUT). */
void slabReplyToBlockedClientTimedOut(client *c) {
    listIter li;
    listNode *ln;
    listRewind(slab_waiters, &li);
    while ((ln = listNext(&li))) {
        slabWaiter *w = listNodeValue(ln);
        if (w->c == c) {
            replyToWaiter(w, true);
            return;
        }
    }
}

/* DEBUG SLAB <subcommand>: verification and test hooks. Returns false for an unknown
 * subcommand. */
bool slabDebugCommand(client *c) {
    if (c->argc < 3) return false;
    sds sub = objectGetVal(c->argv[2]);
    if (!strcasecmp(sub, "verify") && c->argc == 4) {
        robj *o = lookupKeyRead(c->db, c->argv[3]);
        if (o == NULL) {
            addReplyErrorObject(c, shared.nokeyerr);
            return true;
        }
        if (checkType(c, o, OBJ_SLAB)) return true;
        slabSnapshot s;
        slabSnapshotTake(objectGetVal(o), &s, false);
        char *err = slabVerify(&s);
        if (err) {
            addReplyErrorFormat(c, "slab verify failed: %s", err);
            zfree(err);
            return true;
        }
    } else if (!strcasecmp(sub, "max-pending-bytes") && c->argc == 4) {
        long v;
        if (getRangeLongFromObjectOrReply(c, c->argv[3], 0, LONG_MAX, &v, NULL) != C_OK) return true;
        slab_max_pending_bytes = v ? (uint64_t)v : SLAB_MAX_PENDING_BYTES;
    } else if (!strcasecmp(sub, "max-frozen-memtables") && c->argc == 4) {
        long v;
        if (getRangeLongFromObjectOrReply(c, c->argv[3], 0, 1024, &v, NULL) != C_OK) return true;
        slab_max_frozen_memtables = v ? (uint32_t)v : SLAB_MAX_FROZEN_MEMTABLES;
    } else if (!strcasecmp(sub, "pause-maintenance") && c->argc == 3) {
        slabMaintSetPaused(true);
    } else if (!strcasecmp(sub, "resume-maintenance") && c->argc == 3) {
        slabMaintSetPaused(false);
    } else if (!strcasecmp(sub, "hold-sections") && c->argc == 3) {
        slabMaintSetSectionHold(true);
    } else if (!strcasecmp(sub, "release-sections") && c->argc == 3) {
        slabMaintSetSectionHold(false);
    } else {
        return false;
    }
    addReply(c, shared.ok);
    return true;
}

/* The INFO slab section, or the slab part of INFO debug. */
sds genSlabInfoString(sds info, bool debug) {
    if (!debug) {
        info = sdscatprintf(info,
                            "# Slab\r\n"
                            "slab_maintenance_threads:%d\r\n"
                            "slab_waiting_clients:%lu\r\n"
                            "slab_writes_rejected_busy:%lld\r\n",
                            slabMaintThreads(), slab_waiters ? listLength(slab_waiters) : 0,
                            server.stat_slab_writes_rejected_busy);
        return info;
    }
    uint64_t units, installs, fork_waits, fork_timeouts, reclaimed;
    long long fork_wait_total, fork_wait_max;
    slabMaintStats(&units, &installs, &fork_waits, &fork_timeouts, &reclaimed, &fork_wait_total, &fork_wait_max);
    info = sdscatprintf(info,
                        "slab_maintenance_units:%llu\r\n"
                        "slab_installs:%llu\r\n"
                        "slab_reclaim_queue_len:%llu\r\n"
                        "slab_reclaimed:%llu\r\n"
                        "slab_fork_waits:%llu\r\n"
                        "slab_fork_wait_us_total:%lld\r\n"
                        "slab_fork_wait_us_max:%lld\r\n"
                        "slab_fork_prepare_timeouts:%llu\r\n"
                        "slab_sections_held:%d\r\n"
                        "slab_admission_bypassed:%lld\r\n",
                        (unsigned long long)units, (unsigned long long)installs,
                        (unsigned long long)slabReclaimQueueLen(), (unsigned long long)reclaimed,
                        (unsigned long long)fork_waits, fork_wait_total, fork_wait_max, (unsigned long long)fork_timeouts,
                        slabMaintSectionsHeld(),
                        server.stat_slab_admission_bypassed);
    return info;
}

/* unblockClient() for BLOCKED_SLAB: also runs when a waiting client disconnects. */
void slabUnblockClient(client *c) {
    listIter li;
    listNode *ln;
    listRewind(slab_waiters, &li);
    while ((ln = listNext(&li))) {
        slabWaiter *w = listNodeValue(ln);
        if (w->c != c) continue;
        slabCollRelease(w->coll);
        zfree(w);
        listDelNode(slab_waiters, ln);
        return;
    }
}

/* Replies now if the target is reached, else blocks the client until it is or the
 * timeout passes (0: no timeout). */
static void slabWait(client *c, slabColl *coll, bool compaction, uint64_t target, long long reply, mstime_t timeout) {
    bool done = compaction ? slabCompactionDone(coll, target) : slabFlushDone(coll, target);
    if (done) {
        addReplyLongLong(c, reply);
        return;
    }
    if (c->flag.deny_blocking) {
        /* MULTI, scripts and module calls can't block: like a timeout. */
        addReplyNull(c);
        return;
    }
    slabWaiter *w = zmalloc(sizeof(*w));
    w->c = c;
    w->coll = coll;
    slabCollRetain(coll);
    w->compaction = compaction;
    w->target = target;
    w->reply = reply;
    listAddNodeTail(slab_waiters, w);
    initClientBlockingState(c);
    c->bstate->timeout = timeout;
    blockClient(c, BLOCKED_SLAB);
    /* The reply comes from slabBeforeSleep; the command is not executed again. */
    c->flag.pending_command = 0;
}

/* ---------------------------------------------------------------------------------------
 * Options
 * ------------------------------------------------------------------------------------- */

typedef enum {
    OPT_BLOCK_BYTES,
    OPT_FLUSH_ENTRIES,
    OPT_AUTO_COMPACT,
    OPT_COUNT
} slabOpt;

static_assert(OPT_COUNT == SLAB_CONFIG_PAIRS, "every option is persisted");

static const char *slab_opt_names[OPT_COUNT] = {"BLOCK_BYTES", "FLUSH_ENTRIES", "AUTO_COMPACT"};

/* A plain decimal unsigned integer. */
static int parseUnsigned(sds s, uint64_t max, uint64_t *out) {
    size_t n = sdslen(s);
    if (n == 0 || n > 20) return C_ERR;
    uint64_t v = 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] < '0' || s[i] > '9') return C_ERR;
        uint64_t d = (uint64_t)(s[i] - '0');
        if (v > (UINT64_MAX - d) / 10) return C_ERR;
        v = v * 10 + d;
    }
    if (v > max) return C_ERR;
    *out = v;
    return C_OK;
}

static int parseBool(sds s, bool *out) {
    if (!strcasecmp(s, "yes"))
        *out = true;
    else if (!strcasecmp(s, "no"))
        *out = false;
    else
        return C_ERR;
    return C_OK;
}

/* Parses option/value pairs from argv[from..] onto cfg. All or nothing: on error it
 * replies and returns C_ERR without changing cfg. */
static int parseOptions(client *c, int from, slabConfig *cfg) {
    slabConfig tmp = *cfg;
    bool seen[OPT_COUNT] = {0};
    if ((c->argc - from) % 2) {
        addReplyErrorObject(c, shared.syntaxerr);
        return C_ERR;
    }
    for (int j = from; j < c->argc; j += 2) {
        sds name = objectGetVal(c->argv[j]), val = objectGetVal(c->argv[j + 1]);
        int opt = -1;
        for (int k = 0; k < OPT_COUNT; k++)
            if (!strcasecmp(name, slab_opt_names[k])) opt = k;
        if (opt < 0) {
            addReplyErrorFormat(c, "unknown slab option '%s'", name);
            return C_ERR;
        }
        if (seen[opt]) {
            addReplyErrorFormat(c, "slab option '%s' given twice", slab_opt_names[opt]);
            return C_ERR;
        }
        seen[opt] = true;
        uint64_t u = 0;
        int ok = C_OK;
        switch (opt) {
        case OPT_BLOCK_BYTES:
            ok = parseUnsigned(val, UINT32_MAX, &u);
            tmp.block_bytes = (uint32_t)u;
            break;
        case OPT_FLUSH_ENTRIES: ok = parseUnsigned(val, UINT64_MAX, &tmp.flush_entries); break;
        case OPT_AUTO_COMPACT: ok = parseBool(val, &tmp.auto_compact); break;
        }
        if (ok != C_OK) {
            addReplyErrorFormat(c, "invalid value for slab option '%s'", slab_opt_names[opt]);
            return C_ERR;
        }
    }
    const char *err = slabConfigValidate(&tmp);
    if (err) {
        addReplyErrorFormat(c, "%s", err);
        return C_ERR;
    }
    *cfg = tmp;
    return C_OK;
}

/* The normalized option list of a config, for propagation and AOF rewrite. */
int slabConfigToArgv(const slabConfig *cfg, robj **argv) {
    char buf[64];
    int n = 0;
    for (int k = 0; k < OPT_COUNT; k++) {
        argv[n++] = createStringObject(slab_opt_names[k], strlen(slab_opt_names[k]));
        int len;
        switch (k) {
        case OPT_BLOCK_BYTES: len = snprintf(buf, sizeof(buf), "%u", cfg->block_bytes); break;
        case OPT_FLUSH_ENTRIES: len = snprintf(buf, sizeof(buf), "%llu", (unsigned long long)cfg->flush_entries); break;
        default: len = snprintf(buf, sizeof(buf), "%s", cfg->auto_compact ? "yes" : "no"); break;
        }
        argv[n++] = createStringObject(buf, (size_t)len);
    }
    return n;
}


/* Builds "SLCREATE key NX <every option>"; returns argc. The caller frees argv[i]. */
static int slabCreateArgv(robj *key, const slabConfig *cfg, robj **argv) {
    argv[0] = createStringObject("SLCREATE", 8);
    /* The key may be a static object (AOF rewrite), so copy rather than retain it. */
    argv[1] = createStringObject(objectGetVal(key), sdslen(objectGetVal(key)));
    argv[2] = createStringObject("NX", 2);
    return 3 + slabConfigToArgv(cfg, argv + 3);
}

/* ---------------------------------------------------------------------------------------
 * Lookup helpers
 * ------------------------------------------------------------------------------------- */

static slabColl *lookupCollRead(client *c, robj *key, robj **op) {
    robj *o = lookupKeyRead(c->db, key);
    if (op) *op = o;
    if (checkType(c, o, OBJ_SLAB)) return (slabColl *)-1;
    return o ? objectGetVal(o) : NULL;
}

static slabColl *lookupCollWrite(client *c, robj *key, robj **op) {
    robj *o = lookupKeyWrite(c->db, key);
    if (op) *op = o;
    if (checkType(c, o, OBJ_SLAB)) return (slabColl *)-1;
    return o ? objectGetVal(o) : NULL;
}

#define WRONGTYPE ((slabColl *)-1)

static slabOrigin writeOrigin(client *c) {
    if (c->id == CLIENT_ID_AOF) return SLAB_ORIGIN_LOADING;
    if (isReplicatedClient(c)) return SLAB_ORIGIN_REPLICATED;
    return SLAB_ORIGIN_CLIENT;
}

/* Creates a collection with 'cfg' under key and announces it. With 'propagate' (an
 * implicit create by a write), "SLCREATE key NX <every option>" is propagated first,
 * so replicas and the AOF create the same collection whatever their defaults. */
/* Adds the collection object 'o' (new if NULL) under 'key'. */
static slabColl *createColl(client *c, robj *key, const slabConfig *cfg, bool propagate, robj *o) {
    if (!o) o = createSlabObject(cfg);
    slabColl *coll = objectGetVal(o);
    if (propagate) {
        robj *argv[SLAB_CREATE_ARGV_MAX];
        int argc = slabCreateArgv(key, cfg, argv);
        alsoPropagate(c->db->id, argv, argc, PROPAGATE_AOF | PROPAGATE_REPL, c->slot);
        for (int j = 0; j < argc; j++) decrRefCount(argv[j]);
    }
    dbAdd(c->db, key, &o);
    notifyKeyspaceEvent(NOTIFY_SLAB, "slcreate", key, c->db->id);
    return coll;
}

/* Limits of a client write. Replicated and AOF writes are held only to the format
 * limits, like admission: the primary accepted them. */
#define SLAB_MAX_BATCH 10000
#define SLAB_CLIENT_MAX_KEY_BYTES 1024
#define SLAB_CLIENT_MAX_VALUE_BYTES (1024 * 1024)

/* Checks a write's size limits. Format limits apply to every origin (every node accepts
 * the same input); the client limits only to client writes. */
static int checkWriteLimits(client *c, int first, int step, int count, bool values) {
    bool client_origin = writeOrigin(c) == SLAB_ORIGIN_CLIENT;
    if (client_origin && count > SLAB_MAX_BATCH) {
        addReplyErrorFormat(c, "slab batch of %d entries exceeds %d", count, SLAB_MAX_BATCH);
        return C_ERR;
    }
    bool bypass = false;
    for (int j = 0; j < count; j++) {
        size_t kl = sdslen(objectGetVal(c->argv[first + j * step]));
        if (kl > SLAB_MAX_KEY_BYTES) {
            addReplyError(c, "slab key exceeds 65536 bytes");
            return C_ERR;
        }
        if (kl > SLAB_CLIENT_MAX_KEY_BYTES) {
            if (client_origin) {
                addReplyErrorFormat(c, "slab key exceeds %d bytes", SLAB_CLIENT_MAX_KEY_BYTES);
                return C_ERR;
            }
            bypass = true;
        }
        if (!values) continue;
        size_t vl = sdslen(objectGetVal(c->argv[first + j * step + 1]));
        if (vl > SLAB_MAX_VALUE_BYTES) {
            addReplyError(c, "slab value exceeds 67108864 bytes");
            return C_ERR;
        }
        if (vl > SLAB_CLIENT_MAX_VALUE_BYTES) {
            if (client_origin) {
                addReplyErrorFormat(c, "slab value exceeds %d bytes", SLAB_CLIENT_MAX_VALUE_BYTES);
                return C_ERR;
            }
            bypass = true;
        }
    }
    if (!client_origin && (bypass || count > SLAB_MAX_BATCH)) server.stat_slab_admission_bypassed++;
    return C_OK;
}

/* Backpressure; replies -SLABBUSY to a refused client write. */
static int admit(client *c, slabColl *coll, size_t n, size_t bytes) {
    int r = slabAdmit(coll, writeOrigin(c), n, bytes);
    if (r == SLAB_ADMIT_OK) return C_OK;
    if (writeOrigin(c) != SLAB_ORIGIN_CLIENT) {
        /* Replicated and AOF writes are never refused: the primary admitted them. */
        server.stat_slab_admission_bypassed++;
        return C_OK;
    }
    if (r == SLAB_ADMIT_TOO_LARGE) {
        addReplyErrorFormat(c, "slab write is larger than the pending cap of %llu bytes, split it",
                            (unsigned long long)slab_max_pending_bytes);
        return C_ERR;
    }
    server.stat_slab_writes_rejected_busy++;
    addReplyError(c, r == SLAB_ADMIT_BUSY_BYTES ? "-SLABBUSY pending writes exceed the collection's cap, retry later"
                                                : "-SLABBUSY too many frozen memtables, retry later");
    return C_ERR;
}

/* ---------------------------------------------------------------------------------------
 * Write commands
 * ------------------------------------------------------------------------------------- */

/* SLCREATE key [NX] [option value ...] */
void slcreateCommand(client *c) {
    int from = 2;
    bool nx = false;
    if (c->argc > 2 && !strcasecmp(objectGetVal(c->argv[2]), "nx")) {
        nx = true;
        from = 3;
    }
    slabConfig cfg;
    slabConfigDefaults(&cfg);
    if (parseOptions(c, from, &cfg) != C_OK) return;
    slabColl *coll = lookupCollWrite(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    if (coll) {
        if (nx)
            addReplyLongLong(c, 0);
        else
            addReplyError(c, "slab collection already exists");
        return;
    }
    createColl(c, c->argv[1], &cfg, false, NULL);
    signalModifiedKey(c, c->db, c->argv[1]);
    server.dirty++;
    /* Propagate the normalized form, so every option is explicit downstream. The
     * client takes ownership of the array and its references. */
    robj **argv = zmalloc(SLAB_CREATE_ARGV_MAX * sizeof(robj *));
    int argc = slabCreateArgv(c->argv[1], &cfg, argv);
    replaceClientCommandVector(c, argc, argv);
    addReplyLongLong(c, 1);
}

#define SLAB_STACK_KEYS 64

/* Applies argv[first], argv[first+step], ... as one write batch. */
static void applyArgv(slabColl *coll, client *c, int first, int step, int count, bool tombstones) {
    const unsigned char *kst[SLAB_STACK_KEYS], *vst[SLAB_STACK_KEYS];
    size_t klst[SLAB_STACK_KEYS], vlst[SLAB_STACK_KEYS];
    const unsigned char **keys = kst, **vals = vst;
    size_t *klens = klst, *vlens = vlst;
    serverAssert(count > 0); /* the arity guarantees it; tells GCC the arrays get written */
    if (count > SLAB_STACK_KEYS) {
        keys = zmalloc(count * sizeof(*keys));
        vals = zmalloc(count * sizeof(*vals));
        klens = zmalloc(count * sizeof(size_t));
        vlens = zmalloc(count * sizeof(size_t));
    }
    for (int j = 0; j < count; j++) {
        sds k = objectGetVal(c->argv[first + j * step]);
        keys[j] = (const unsigned char *)k;
        klens[j] = sdslen(k);
        if (!tombstones) {
            sds v = objectGetVal(c->argv[first + j * step + 1]);
            vals[j] = (const unsigned char *)v;
            vlens[j] = sdslen(v);
        } else {
            vals[j] = NULL;
            vlens[j] = 0;
        }
    }
    slabApply(coll, (size_t)count, keys, klens, vals, vlens, tombstones);
    if (keys != kst) {
        zfree(keys);
        zfree(vals);
        zfree(klens);
        zfree(vlens);
    }
}

static size_t argvBytes(client *c, int first, int step, int count, bool values) {
    size_t bytes = 0;
    for (int j = 0; j < count; j++) {
        bytes += sdslen(objectGetVal(c->argv[first + j * step]));
        if (values) bytes += sdslen(objectGetVal(c->argv[first + j * step + 1]));
    }
    return bytes;
}

/* SLMSET key k v [k v ...] */
void slmsetCommand(client *c) {
    if ((c->argc - 2) % 2) {
        addReplyErrorArity(c);
        return;
    }
    int pairs = (c->argc - 2) / 2;
    if (checkWriteLimits(c, 2, 2, pairs, true) != C_OK) return;
    slabColl *coll = lookupCollWrite(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    size_t bytes = argvBytes(c, 2, 2, pairs, true);
    if (!coll) {
        /* A write to a missing collection creates it with the default options, unless
         * the write itself is refused: then nothing is created. */
        slabConfig cfg;
        slabConfigDefaults(&cfg);
        robj *o = createSlabObject(&cfg);
        if (admit(c, objectGetVal(o), (size_t)pairs, bytes) != C_OK) {
            decrRefCount(o);
            return;
        }
        coll = createColl(c, c->argv[1], &cfg, true, o);
        server.dirty++;
    } else if (admit(c, coll, (size_t)pairs, bytes) != C_OK) {
        return;
    }
    applyArgv(coll, c, 2, 2, pairs, false);
    signalModifiedKey(c, c->db, c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_SLAB, "slmset", c->argv[1], c->db->id);
    server.dirty += pairs;
    addReplyLongLong(c, pairs);
}

/* SLDEL key k [k ...] */
void sldelCommand(client *c) {
    int count = c->argc - 2;
    if (checkWriteLimits(c, 2, 1, count, false) != C_OK) return;
    slabColl *coll = lookupCollWrite(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    if (!coll) {
        addReplyLongLong(c, 0);
        return;
    }
    if (admit(c, coll, (size_t)count, argvBytes(c, 2, 1, count, false)) != C_OK) return;
    applyArgv(coll, c, 2, 1, count, true);
    signalModifiedKey(c, c->db, c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_SLAB, "sldel", c->argv[1], c->db->id);
    server.dirty += count;
    addReplyLongLong(c, count);
}

/* ---------------------------------------------------------------------------------------
 * Read commands
 * ------------------------------------------------------------------------------------- */

/* SLGET key k */
void slgetCommand(client *c) {
    slabColl *coll = lookupCollRead(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    if (!coll) {
        addReplyNull(c);
        return;
    }
    slabSnapshot s;
    slabSnapshotTake(coll, &s, false);
    sds k = objectGetVal(c->argv[2]);
    const unsigned char *v;
    size_t vl;
    if (slabGet(&s, (const unsigned char *)k, sdslen(k), &v, &vl))
        addReplyBulkCBuffer(c, v, vl);
    else
        addReplyNull(c);
}

/* Replies to a batch read on the main thread. */
static void replyMultiGet(client *c, slabColl *coll, int first, int count) {
    if (!coll) {
        addReplyArrayLen(c, count);
        for (int j = 0; j < count; j++) addReplyNull(c);
        return;
    }
    const unsigned char *kst[SLAB_STACK_KEYS], *vst[SLAB_STACK_KEYS];
    size_t klst[SLAB_STACK_KEYS], vlst[SLAB_STACK_KEYS];
    const unsigned char **keys = kst, **vals = vst;
    size_t *klens = klst, *vlens = vlst;
    if (count > SLAB_STACK_KEYS) {
        keys = zmalloc(count * sizeof(*keys));
        vals = zmalloc(count * sizeof(*vals));
        klens = zmalloc(count * sizeof(size_t));
        vlens = zmalloc(count * sizeof(size_t));
    }
    for (int j = 0; j < count; j++) {
        sds k = objectGetVal(c->argv[first + j]);
        keys[j] = (const unsigned char *)k;
        klens[j] = sdslen(k);
    }
    slabSnapshot s;
    slabSnapshotTake(coll, &s, false);
    slabMultiGet(&s, (size_t)count, keys, klens, vals, vlens);
    addReplyArrayLen(c, count);
    for (int j = 0; j < count; j++) {
        if (vals[j])
            addReplyBulkCBuffer(c, vals[j], vlens[j]);
        else
            addReplyNull(c);
    }
    if (keys != kst) {
        zfree(keys);
        zfree(vals);
        zfree(klens);
        zfree(vlens);
    }
}

/* SLMGET key k [k ...] */
void slmgetCommand(client *c) {
    slabColl *coll = lookupCollRead(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    replyMultiGet(c, coll, 2, c->argc - 2);
}

#define SLAB_SCAN_DEFAULT_COUNT 100
#define SLAB_SCAN_FREEZE_MAX_FROZEN 4
#define SLAB_SCAN_FREEZE_MIN_ENTRIES 1024

/* Before an ordered read of the whole collection (SLSCAN, SLCOUNT EXACT): sorted, shared
 * runs serve later calls, while a large active memtable would be sorted privately on
 * every call. */
static void freezeForOrderedRead(slabColl *coll) {
    slabInfo info;
    slabCollInfo(coll, &info);
    if (info.pending_writes - info.pending_writes_draining >= SLAB_SCAN_FREEZE_MIN_ENTRIES &&
        info.frozen_memtables < SLAB_SCAN_FREEZE_MAX_FROZEN)
        slabFreeze(coll);
}

/* SLSCAN key cursor [PREFIX p] [COUNT n] [NOVALUES]
 *
 * The cursor is "0" to start and when done, otherwise "1" followed by the last key
 * visited. COUNT bounds the records a call visits. An entry present for the whole
 * iteration is returned exactly once, in key order. */
void slscanCommand(client *c) {
    sds cursor = objectGetVal(c->argv[2]);
    sds prefix = NULL;
    long count = SLAB_SCAN_DEFAULT_COUNT;
    bool novalues = false;
    for (int j = 3; j < c->argc; j++) {
        sds opt = objectGetVal(c->argv[j]);
        if (!strcasecmp(opt, "prefix") && j + 1 < c->argc) {
            prefix = objectGetVal(c->argv[++j]);
        } else if (!strcasecmp(opt, "count") && j + 1 < c->argc) {
            if (getRangeLongFromObjectOrReply(c, c->argv[++j], 1, LONG_MAX, &count, NULL) != C_OK) return;
        } else if (!strcasecmp(opt, "novalues")) {
            novalues = true;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }
    sds start = NULL;
    if (!strcmp(cursor, "0")) {
        start = sdsempty();
    } else if (sdslen(cursor) >= 1 && cursor[0] == '1') {
        /* Resume just after the last visited key. */
        start = sdsnewlen(cursor + 1, sdslen(cursor) - 1);
        start = sdscatlen(start, "\0", 1);
    } else {
        addReplyError(c, "invalid slab scan cursor");
        return;
    }
    if (prefix && sdscmp(start, prefix) < 0) {
        sdsfree(start);
        start = sdsdup(prefix);
    }
    slabColl *coll = lookupCollRead(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) {
        sdsfree(start);
        return;
    }
    if (!coll) {
        sdsfree(start);
        addReplyArrayLen(c, 2);
        addReplyBulkCBuffer(c, "0", 1);
        addReplyArrayLen(c, 0);
        return;
    }
    freezeForOrderedRead(coll);
    slabSnapshot s;
    slabSnapshotTake(coll, &s, false);
    slabIter *it = slabIterCreate(&s, (unsigned char *)start, sdslen(start), false);
    slabIterSetBudget(it, (uint64_t)count);
    void *replylen = NULL;
    addReplyArrayLen(c, 2);
    /* The cursor goes first, but is known only at the end: buffer the entries. */
    const unsigned char *k, *v;
    size_t kl, vl;
    list *items = listCreate();
    long long n = 0;
    bool done = true;
    while (slabIterNext(it, &k, &kl, &v, &vl)) {
        if (prefix && (kl < sdslen(prefix) || memcmp(k, prefix, sdslen(prefix)) != 0)) break;
        listAddNodeTail(items, sdsnewlen(k, kl));
        if (!novalues) listAddNodeTail(items, sdsnewlen(v, vl));
        n++;
    }
    if (slabIterStoppedOnBudget(it)) {
        const unsigned char *lk;
        size_t lkl;
        slabIterLastKey(it, &lk, &lkl);
        if (!prefix || (lkl >= sdslen(prefix) && memcmp(lk, prefix, sdslen(prefix)) == 0)) {
            sds next = sdsnewlen("1", 1);
            next = sdscatlen(next, lk, lkl);
            addReplyBulkSds(c, next);
            done = false;
        }
    }
    if (done) addReplyBulkCBuffer(c, "0", 1);
    (void)replylen;
    addReplyArrayLen(c, (long)listLength(items));
    listIter li;
    listNode *ln;
    listRewind(items, &li);
    while ((ln = listNext(&li))) {
        sds item = listNodeValue(ln);
        addReplyBulkCBuffer(c, item, sdslen(item));
        sdsfree(item);
    }
    listRelease(items);
    slabIterFree(it);
    sdsfree(start);
    (void)n;
}

/* SLCOUNT key [EXACT] */
void slcountCommand(client *c) {
    bool exact = false;
    if (c->argc > 3) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    if (c->argc == 3) {
        if (strcasecmp(objectGetVal(c->argv[2]), "exact")) {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
        exact = true;
    }
    slabColl *coll = lookupCollRead(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    if (!coll) {
        addReplyLongLong(c, 0);
        return;
    }
    if (!exact) {
        addReplyLongLong(c, (long long)slabCollCountEstimate(coll));
        return;
    }
    freezeForOrderedRead(coll);
    slabSnapshot s;
    slabSnapshotTake(coll, &s, false);
    addReplyLongLong(c, (long long)slabCountExact(&s));
}

/* SLINFO key [FULL]: the size and maintenance state; FULL adds the layout, the
 * maintenance counters and the options. */
void slinfoCommand(client *c) {
    bool full = false;
    if (c->argc > 3 || (c->argc == 3 && strcasecmp(objectGetVal(c->argv[2]), "full"))) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    full = c->argc == 3;
    slabColl *coll = lookupCollRead(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    if (!coll) {
        addReplyError(c, "no such slab collection");
        return;
    }
    slabInfo i;
    slabCollInfo(coll, &i);
    const slabStats *st = slabCollStats(coll);
    const slabConfig *cfg = slabCollConfig(coll);
#define LOAD(f) atomic_load_explicit(&st->f, memory_order_relaxed)
    uint64_t user = LOAD(user_bytes_written);
    uint64_t out = LOAD(maint_delta_record_bytes) + LOAD(maint_base_block_bytes) + LOAD(maint_rewrap_bytes);
    uint64_t structural = out + LOAD(maint_index_bytes);
    struct {
        const char *name;
        uint64_t val;
    } fields[] = {
        /* The short form. */
        {"entries", i.entries},
        {"memory_bytes", i.snapshot_bytes},
        {"pending_writes", i.pending_writes},
        {"pending_bytes", i.pending_bytes},
        {"frozen_memtables", i.frozen_memtables},
        {"delta_entries", i.delta_entries},
        {"delta_bytes", i.delta_bytes},
        {"flush_inflight", i.flush_inflight},
        {"compaction_requested", i.compaction_requested},
        {"compaction_inflight", i.compaction_inflight},
        {"maintenance_queue_depth", i.background_queue_depth},
        /* FULL */
        {"blocks", i.blocks},
        {"delta_blocks", i.delta_blocks},
        {"delta_puts", i.delta_puts},
        {"delta_tombstones", i.delta_tombstones},
        {"base_bytes", i.base_bytes},
        {"data_bytes", i.data_bytes},
        {"pending_writes_draining", i.pending_writes_draining},
        {"memtable_puts", i.memtable_puts},
        {"memtable_tombstones", i.memtable_tombstones},
        {"flushes", LOAD(flushes)},
        {"compactions", LOAD(compactions)},
        {"defragged_leaves", LOAD(defragged_leaves)},
        {"user_bytes_written", user},
        {"maint_delta_record_bytes", LOAD(maint_delta_record_bytes)},
        {"maint_base_block_bytes", LOAD(maint_base_block_bytes)},
        {"maint_rewrap_bytes", LOAD(maint_rewrap_bytes)},
        {"maint_index_bytes", LOAD(maint_index_bytes)},
        {"wa_out_bp", user ? out * 10000 / user : 0},
        {"wa_struct_bp", user ? structural * 10000 / user : 0},
        {"block_bytes", cfg->block_bytes},
        {"flush_entries", cfg->flush_entries},
        {"auto_compact", cfg->auto_compact},
    };
#undef LOAD
    size_t n = full ? sizeof(fields) / sizeof(fields[0]) : 11;
    addReplyMapLen(c, (long)n);
    for (size_t j = 0; j < n; j++) {
        addReplyBulkCString(c, fields[j].name);
        addReplyLongLong(c, (long long)fields[j].val);
    }
}

/* ---------------------------------------------------------------------------------------
 * Maintenance commands
 * ------------------------------------------------------------------------------------- */

/* Parses "WAIT timeout" at argv[j]; returns C_ERR after replying. */
static int parseWait(client *c, int j, mstime_t *timeout) {
    if (strcasecmp(objectGetVal(c->argv[j]), "wait") || j + 1 >= c->argc) {
        addReplyErrorObject(c, shared.syntaxerr);
        return C_ERR;
    }
    return getTimeoutFromObjectOrReply(c, c->argv[j + 1], timeout, UNIT_MILLISECONDS);
}

/* SLFLUSH key [WAIT timeout] */
void slflushCommand(client *c) {
    bool wait = c->argc > 2;
    mstime_t timeout = 0;
    if (c->argc > 4) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    if (wait && parseWait(c, 2, &timeout) != C_OK) return;
    slabColl *coll = lookupCollRead(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    if (!coll) {
        addReplyError(c, "no such slab collection");
        return;
    }
    uint64_t target;
    uint64_t n = slabRequestFlush(coll, &target);
    if (wait)
        slabWait(c, coll, false, target, (long long)n, timeout);
    else
        addReplyLongLong(c, (long long)n);
}

/* SLCOMPACT key [FULL] [WAIT timeout] */
void slcompactCommand(client *c) {
    bool full = false, wait = false;
    mstime_t timeout = 0;
    for (int j = 2; j < c->argc; j++) {
        if (!full && !strcasecmp(objectGetVal(c->argv[j]), "full")) {
            full = true;
        } else if (!wait) {
            if (parseWait(c, j, &timeout) != C_OK) return;
            wait = true;
            j++;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }
    slabColl *coll = lookupCollRead(c, c->argv[1], NULL);
    if (coll == WRONGTYPE) return;
    if (!coll) {
        addReplyError(c, "no such slab collection");
        return;
    }
    uint64_t target;
    uint64_t n = slabRequestCompaction(coll, full, &target);
    if (wait)
        slabWait(c, coll, true, target, (long long)n, timeout);
    else
        addReplyLongLong(c, (long long)n);
}
