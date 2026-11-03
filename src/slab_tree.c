/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Slab engine: the tree of leaves, the packer, maintenance units (flush, compaction,
 * the copy re-wrap), installs, the merge iterator, export, import and verification. */

#include "slab_int.h"
#include "slab_hash.h"
#include "serverassert.h"
#include "zmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---------------------------------------------------------------------------------------
 * Reference counting of shared tree parts
 * ------------------------------------------------------------------------------------- */

void slabBaseRetain(slabBase *b) {
    slabRefIncr(&b->refcount);
}

void slabBaseRelease(slabBase *b) {
    if (b && slabRefDecr(&b->refcount)) slabBaseFree(b);
}

void slabDeltaSegRelease(slabDeltaSeg *s) {
    if (s && slabRefDecr(&s->refcount)) slabDeltaSegFree(s);
}

static slabLeafDelta *leafDeltaCreate(uint64_t owner, slabDeltaSeg *older) {
    slabLeafDelta *d = zcalloc(sizeof(*d));
    atomic_init(&d->refcount, 1);
    atomic_init(&d->own, NULL);
    d->older = older;
    d->owner = owner;
    return d;
}

void slabLeafDeltaRetain(slabLeafDelta *d) {
    slabRefIncr(&d->refcount);
}

void slabLeafDeltaRelease(slabLeafDelta *d) {
    if (!d || !slabRefDecr(&d->refcount)) return;
    slabVlogFree(atomic_load_explicit(&d->own, memory_order_relaxed));
    slabDeltaSegRelease(d->older);
    zfree(d);
}

void slabLeafDeltaCounters(const slabLeafDelta *d, slabDeltaInfo *out) {
    memset(out, 0, sizeof(*out));
    if (!d) return;
    slabVlog *own = atomic_load_explicit(&((slabLeafDelta *)d)->own, memory_order_acquire);
    if (own) {
        out->entries += slabVlogEntries(own);
        out->puts += slabVlogPuts(own);
        out->tombstones += slabVlogTombstones(own);
        out->payload_bytes += slabVlogPayloadBytes(own);
        out->bytes += slabVlogBytes(own);
    }
    if (d->older) {
        out->entries += d->older->entries;
        out->puts += d->older->puts;
        out->tombstones += d->older->tombstones;
        out->payload_bytes += d->older->payload_bytes;
        out->bytes += d->older->alloc_bytes;
    }
    out->delta_leaves = out->entries ? 1 : 0;
}

static void deltaInfoAdd(slabDeltaInfo *a, const slabDeltaInfo *b, int sign) {
    a->delta_leaves += sign * (int64_t)b->delta_leaves;
    a->entries += sign * (int64_t)b->entries;
    a->puts += sign * (int64_t)b->puts;
    a->tombstones += sign * (int64_t)b->tombstones;
    a->payload_bytes += sign * (int64_t)b->payload_bytes;
    a->bytes += sign * (int64_t)b->bytes;
}

/* ---------------------------------------------------------------------------------------
 * Pages and trees
 * ------------------------------------------------------------------------------------- */

static void leafRetain(slabLeaf *l) {
    slabBaseRetain(l->base);
    if (l->delta) slabLeafDeltaRetain(l->delta);
}

static void leafRelease(slabLeaf *l) {
    slabBaseRelease(l->base);
    slabLeafDeltaRelease(l->delta);
}

/* Builds a page over n leaves, taking ownership of their references. */
static slabPage *pageCreate(size_t n, slabLeaf *leaves, const unsigned char *const *lowers, const size_t *lens) {
    size_t usable;
    slabPage *p = zmalloc_usable(sizeof(*p) + n * sizeof(slabLeaf), &usable);
    atomic_init(&p->refcount, 1);
    p->n = (uint32_t)n;
    memcpy(p->leaves, leaves, n * sizeof(slabLeaf));
    p->fence = slabFenceCreate(n, lowers, lens);
    memset(&p->info, 0, sizeof(p->info));
    for (size_t i = 0; i < n; i++) {
        const slabBase *b = leaves[i].base;
        p->info.leaves++;
        p->info.base_entries += b->entries;
        p->info.base_bytes += b->len;
        p->info.alloc_bytes += b->alloc_bytes + (leaves[i].delta ? zmalloc_size(leaves[i].delta) : 0);
    }
    p->alloc_bytes = usable + p->fence->alloc_bytes;
    p->info.alloc_bytes += p->alloc_bytes;
    return p;
}

static void pageRelease(slabPage *p) {
    if (!slabRefDecr(&p->refcount)) return;
    for (uint32_t i = 0; i < p->n; i++) leafRelease(&p->leaves[i]);
    slabFenceFree(p->fence);
    zfree(p);
}

/* A tree over the given pages (references transferred). */
static slabTree *treeCreate(uint64_t owner, uint32_t npages, slabPage **pages) {
    size_t usable;
    slabTree *t = zmalloc_usable(sizeof(*t) + npages * sizeof(slabPage *) + npages * sizeof(uint64_t), &usable);
    atomic_init(&t->refcount, 1);
    t->npages = npages;
    t->owner = owner;
    t->first = (uint64_t *)((char *)t + sizeof(*t) + npages * sizeof(slabPage *));
    memset(&t->info, 0, sizeof(t->info));
    uint64_t pos = 0;
    const unsigned char **lowers = zmalloc((npages + 1) * sizeof(*lowers));
    size_t *lens = zmalloc((npages + 1) * sizeof(size_t));
    for (uint32_t i = 0; i < npages; i++) {
        t->pages[i] = pages[i];
        t->first[i] = pos;
        pos += pages[i]->n;
        lowers[i] = slabFenceKey(pages[i]->fence, 0, &lens[i]);
        t->info.leaves += pages[i]->info.leaves;
        t->info.base_entries += pages[i]->info.base_entries;
        t->info.base_bytes += pages[i]->info.base_bytes;
        t->info.alloc_bytes += pages[i]->info.alloc_bytes;
    }
    t->nleaves = pos;
    t->fence = slabFenceCreate(npages, lowers, lens);
    zfree(lowers);
    zfree(lens);
    t->alloc_bytes = usable + t->fence->alloc_bytes;
    return t;
}

slabTree *slabTreeEmpty(uint64_t owner) {
    return treeCreate(owner, 0, NULL);
}

void slabTreeRetain(slabTree *t) {
    slabRefIncr(&t->refcount);
}

void slabTreeRelease(slabTree *t) {
    if (!t || !slabRefDecr(&t->refcount)) return;
    for (uint32_t i = 0; i < t->npages; i++) pageRelease(t->pages[i]);
    slabFenceFree(t->fence);
    zfree(t);
}

uint64_t slabTreeRoute(const slabTree *t, const unsigned char *key, size_t klen) {
    size_t pr = slabFenceRank(t->fence, key, klen);
    uint32_t page = pr ? (uint32_t)pr - 1 : 0;
    size_t lr = slabFenceRank(t->pages[page]->fence, key, klen);
    return (uint64_t)page << 16 | (lr ? lr - 1 : 0);
}

void slabTreeRouteBatch(const slabTree *t, size_t n, const unsigned char *const *keys, const size_t *klens, uint64_t *out) {
    size_t pages[64];
    size_t local[SLAB_FENCE_LANES];
    for (size_t start = 0; start < n; start += 64) {
        size_t m = n - start < 64 ? n - start : 64;
        slabFenceRankBatch(t->fence, m, keys + start, klens + start, pages);
        for (size_t j = 0; j < m; j += SLAB_FENCE_LANES) {
            size_t lanes = m - j < SLAB_FENCE_LANES ? m - j : SLAB_FENCE_LANES;
            const slabFence *idx[SLAB_FENCE_LANES];
            for (size_t l = 0; l < lanes; l++) {
                size_t p = pages[j + l] ? pages[j + l] - 1 : 0;
                pages[j + l] = p;
                idx[l] = t->pages[p]->fence;
            }
            slabFenceRankLanes(idx, lanes, keys + start + j, klens + start + j, local);
            for (size_t l = 0; l < lanes; l++)
                out[start + j + l] = (uint64_t)pages[j + l] << 16 | (local[l] ? local[l] - 1 : 0);
        }
    }
}

/* Global position <-> routed id. */

static inline uint64_t posOfId(const slabTree *t, uint64_t id) {
    return t->first[id >> 16] + (id & 0xffff);
}

void slabTreeLower(const slabTree *t, uint64_t pos, const unsigned char **key, size_t *klen) {
    uint32_t page, local;
    slabTreeLeaf(t, pos, &page, &local);
    *key = slabFenceKey(t->pages[page]->fence, local, klen);
}

/* Splits n leaves into pages of at most SLAB_PAGE_MAX leaves, of equal size. */
static uint32_t paginate(size_t n, slabLeaf *leaves, const unsigned char *const *lowers, const size_t *lens, slabPage ***out) {
    uint32_t np = (uint32_t)((n + SLAB_PAGE_MAX - 1) / SLAB_PAGE_MAX);
    slabPage **pages = zmalloc((np ? np : 1) * sizeof(slabPage *));
    size_t off = 0;
    for (uint32_t i = 0; i < np; i++) {
        size_t cnt = n / np + (i < n % np);
        pages[i] = pageCreate(cnt, leaves + off, lowers + off, lens + off);
        off += cnt;
    }
    *out = pages;
    return np;
}

slabTree *slabTreeFromLeaves(uint64_t owner, size_t n, slabLeaf *leaves, unsigned char **lowers, size_t *lower_lens) {
    slabPage **pages;
    uint32_t np = paginate(n, leaves, (const unsigned char *const *)lowers, lower_lens, &pages);
    slabTree *t = treeCreate(owner, np, pages);
    zfree(pages);
    return t;
}

/* Copy-on-write replacement of leaves [from, to) by n new leaves (references
 * transferred; lowers copied). Only the touched pages are rebuilt; the others are
 * shared. A run below SLAB_PAGE_MIN leaves absorbs a neighbouring page. If the
 * replacement is empty, the range merges into its predecessor; when it starts the tree,
 * the next leaf inherits the empty lower bound. */
slabTree *slabTreeReplace(const slabTree *t, uint64_t from, uint64_t to, size_t n, slabLeaf *leaves, unsigned char **lowers, size_t *lower_lens) {
    assert(from < to && to <= t->nleaves);
    uint32_t p0, p1, l0, l1;
    slabTreeLeaf(t, from, &p0, &l0);
    slabTreeLeaf(t, to - 1, &p1, &l1);
    /* Absorb neighbours while the run would be undersized. */
    size_t run = (size_t)(t->first[p1] + t->pages[p1]->n - t->first[p0]) - (to - from) + n;
    while (run < SLAB_PAGE_MIN && (p0 > 0 || p1 + 1 < t->npages)) {
        if (p1 + 1 < t->npages) {
            p1++;
            run += t->pages[p1]->n;
        } else {
            p0--;
            run += t->pages[p0]->n;
        }
    }
    uint64_t run_from = t->first[p0];
    uint64_t run_to = t->first[p1] + t->pages[p1]->n;
    size_t total = (size_t)(run_to - run_from) - (size_t)(to - from) + n;
    slabLeaf *rl = zmalloc((total ? total : 1) * sizeof(slabLeaf));
    const unsigned char **rk = zmalloc((total ? total : 1) * sizeof(*rk));
    size_t *rkl = zmalloc((total ? total : 1) * sizeof(size_t));
    size_t m = 0;
    for (uint64_t pos = run_from; pos < run_to; pos++) {
        if (pos == from) {
            for (size_t i = 0; i < n; i++) {
                rl[m] = leaves[i];
                rk[m] = lowers[i];
                rkl[m] = lower_lens[i];
                m++;
            }
        }
        if (pos >= from && pos < to) continue;
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
        rl[m] = *l;
        leafRetain(&rl[m]);
        rk[m] = slabFenceKey(t->pages[pg]->fence, lc, &rkl[m]);
        m++;
    }
    assert(m == total);
    /* The first leaf of the tree always has the empty lower bound. */
    if (run_from == 0 && m > 0) rkl[0] = 0;
    uint32_t keep = t->npages - (p1 - p0 + 1);
    slabPage **mid;
    uint32_t nmid = paginate(m, rl, rk, rkl, &mid);
    slabPage **pages = zmalloc((keep + nmid + 1) * sizeof(slabPage *));
    uint32_t np = 0;
    for (uint32_t i = 0; i < p0; i++) {
        slabRefIncr(&t->pages[i]->refcount);
        pages[np++] = t->pages[i];
    }
    for (uint32_t i = 0; i < nmid; i++) pages[np++] = mid[i];
    for (uint32_t i = p1 + 1; i < t->npages; i++) {
        slabRefIncr(&t->pages[i]->refcount);
        pages[np++] = t->pages[i];
    }
    slabTree *nt = treeCreate(t->owner, np, pages);
    zfree(pages);
    zfree(mid);
    zfree(rl);
    zfree(rk);
    zfree(rkl);
    return nt;
}

/* ---------------------------------------------------------------------------------------
 * Delta records of a leaf, merged newest-wins in key order
 * ------------------------------------------------------------------------------------- */

typedef struct deltaRec {
    const unsigned char *key;
    size_t klen;
    const unsigned char *val; /* NULL = tombstone */
    size_t vlen;
} deltaRec;

typedef struct deltaList {
    deltaRec *recs;
    size_t n, cap;
} deltaList;

static void deltaListPush(deltaList *dl, const unsigned char *k, size_t kl, const unsigned char *v, size_t vl) {
    if (dl->n == dl->cap) {
        dl->cap = dl->cap ? dl->cap * 2 : 16;
        dl->recs = zrealloc(dl->recs, dl->cap * sizeof(deltaRec));
    }
    dl->recs[dl->n++] = (deltaRec){k, kl, v, vl};
}

static int deltaRecCmp(const void *a, const void *b) {
    const deltaRec *x = a, *y = b;
    return slabKeyCmp(x->key, x->klen, y->key, y->klen);
}

/* The leaf's visible delta at 'bound': 'own' records at or below it, over 'older'. */
static void leafDeltaCollect(const slabLeafDelta *d, uint64_t bound, deltaList *out) {
    out->n = 0;
    if (!d) return;
    deltaList own = {0};
    slabVlog *log = atomic_load_explicit(&((slabLeafDelta *)d)->own, memory_order_acquire);
    if (log) {
        slabVlogIter it;
        slabVlogIterInit(&it, log, bound);
        const slabVlogRec *r;
        while ((r = slabVlogIterNext(&it)))
            deltaListPush(&own, r->data, r->klen, slabVlogRecIsTombstone(r) ? NULL : r->data + r->klen,
                          slabVlogRecIsTombstone(r) ? 0 : r->vlen);
        qsort(own.recs, own.n, sizeof(deltaRec), deltaRecCmp);
    }
    size_t oi = 0;
    size_t spos = 0;
    const unsigned char *sk = NULL, *sv = NULL;
    size_t skl = 0, svl = 0;
    bool shas = d->older && slabDeltaSegNext(d->older, &spos, &sk, &skl, &sv, &svl);
    while (oi < own.n || shas) {
        int c = !shas ? -1 : oi == own.n ? 1
                                         : slabKeyCmp(own.recs[oi].key, own.recs[oi].klen, sk, skl);
        if (c <= 0) {
            deltaListPush(out, own.recs[oi].key, own.recs[oi].klen, own.recs[oi].val, own.recs[oi].vlen);
            oi++;
            if (c == 0) shas = slabDeltaSegNext(d->older, &spos, &sk, &skl, &sv, &svl);
        } else {
            deltaListPush(out, sk, skl, sv, svl);
            shas = slabDeltaSegNext(d->older, &spos, &sk, &skl, &sv, &svl);
        }
    }
    zfree(own.recs);
}

/* Sorts records by key, keeping only the last of equal keys (the newest, when the
 * input is in write order). An index tiebreak makes the sort stable. */
typedef struct {
    deltaRec r;
    size_t idx;
} indexedRec;

static int indexedRecCmp(const void *a, const void *b) {
    const indexedRec *x = a, *y = b;
    int c = slabKeyCmp(x->r.key, x->r.klen, y->r.key, y->r.klen);
    if (c) return c;
    return (x->idx > y->idx) - (x->idx < y->idx);
}

static void stableSortLastWins(deltaList *dl) {
    if (dl->n < 2) return;
    indexedRec *t = zmalloc(dl->n * sizeof(indexedRec));
    for (size_t i = 0; i < dl->n; i++) t[i] = (indexedRec){dl->recs[i], i};
    qsort(t, dl->n, sizeof(indexedRec), indexedRecCmp);
    size_t m = 0;
    for (size_t i = 0; i < dl->n; i++) {
        if (i + 1 < dl->n && slabKeyCmp(t[i].r.key, t[i].r.klen, t[i + 1].r.key, t[i + 1].r.klen) == 0) continue;
        dl->recs[m++] = t[i].r;
    }
    dl->n = m;
    zfree(t);
}

/* ---------------------------------------------------------------------------------------
 * Packer: sorted live entries -> leaves of blocks closed at the block size class
 * ------------------------------------------------------------------------------------- */

typedef struct packer {
    const slabConfig *cfg;
    uint64_t owner;
    size_t cap; /* the largest size class within BLOCK_BYTES */
    slabBaseBuilder prev, cur;
    bool has_prev;
    /* output */
    slabLeaf *leaves;
    unsigned char **lowers;
    size_t *lower_lens;
    size_t n, ncap;
    /* the lower bound of the first output leaf */
    const unsigned char *first_lower;
    size_t first_lower_len;
    uint64_t bytes_written;
} packer;

static void packerInit(packer *p, const slabConfig *cfg, uint64_t owner, const unsigned char *first_lower, size_t fll) {
    memset(p, 0, sizeof(*p));
    p->cfg = cfg;
    p->owner = owner;
    p->cap = slabSizeClassFloor(cfg->block_bytes);
    slabBaseBuilderInit(&p->prev, cfg->key_restart, cfg->value_restart);
    slabBaseBuilderInit(&p->cur, cfg->key_restart, cfg->value_restart);
    p->first_lower = first_lower;
    p->first_lower_len = fll;
}

static void packerEmit(packer *p, slabBaseBuilder *bb) {
    if (bb->entries == 0) return;
    if (p->n == p->ncap) {
        p->ncap = p->ncap ? p->ncap * 2 : 8;
        p->leaves = zrealloc(p->leaves, p->ncap * sizeof(slabLeaf));
        p->lowers = zrealloc(p->lowers, p->ncap * sizeof(unsigned char *));
        p->lower_lens = zrealloc(p->lower_lens, p->ncap * sizeof(size_t));
    }
    const unsigned char *lk;
    size_t ll;
    if (p->n == 0) {
        lk = p->first_lower;
        ll = p->first_lower_len;
    } else {
        lk = bb->keys;
        ll = bb->key_off[1];
    }
    unsigned char *lower = zmalloc(ll ? ll : 1);
    memcpy(lower, lk, ll);
    slabBase *b = slabBaseBuilderFinish(bb);
    p->bytes_written += b->len;
    p->leaves[p->n] = (slabLeaf){b, leafDeltaCreate(p->owner, NULL), 0};
    p->lowers[p->n] = lower;
    p->lower_lens[p->n] = ll;
    p->n++;
}

static void packerAdd(packer *p, const unsigned char *k, size_t kl, const unsigned char *v, size_t vl) {
    size_t shared = slabBaseBuilderShared(&p->cur, k, kl);
    if (p->cur.entries > 0 && slabBaseBuilderBytesWith(&p->cur, kl, vl, shared) > p->cap) {
        if (p->has_prev) packerEmit(p, &p->prev);
        slabBaseBuilder tmp = p->prev;
        p->prev = p->cur;
        p->cur = tmp;
        slabBaseBuilderReset(&p->cur);
        p->has_prev = true;
    }
    slabBaseBuilderAdd(&p->cur, k, kl, v, vl);
}

/* Closes the packer. A tail block under a quarter of the cap is evened out with the
 * block before it, so a rewrite never leaves a sliver. */
static void packerFinish(packer *p) {
    if (p->has_prev && p->cur.entries > 0 && slabBaseBuilderBytes(&p->cur) < p->cap / 4) {
        size_t total = slabBaseBuilderBytes(&p->prev) + slabBaseBuilderBytes(&p->cur);
        slabBaseBuilder a, b;
        slabBaseBuilderInit(&a, p->cfg->key_restart, p->cfg->value_restart);
        slabBaseBuilderInit(&b, p->cfg->key_restart, p->cfg->value_restart);
        slabBaseBuilder *srcs[2] = {&p->prev, &p->cur};
        for (int s = 0; s < 2; s++) {
            slabBaseBuilder *src = srcs[s];
            for (uint32_t i = 0; i < src->entries; i++) {
                const unsigned char *k = src->keys + src->key_off[i];
                size_t kl = src->key_off[i + 1] - src->key_off[i];
                const unsigned char *v = src->vals + src->val_off[i];
                size_t vl = src->val_off[i + 1] - src->val_off[i];
                if (b.entries == 0 && slabBaseBuilderBytes(&a) < total / 2)
                    slabBaseBuilderAdd(&a, k, kl, v, vl);
                else
                    slabBaseBuilderAdd(&b, k, kl, v, vl);
            }
        }
        packerEmit(p, &a);
        packerEmit(p, &b);
        slabBaseBuilderRelease(&a);
        slabBaseBuilderRelease(&b);
    } else {
        if (p->has_prev) packerEmit(p, &p->prev);
        packerEmit(p, &p->cur);
    }
    slabBaseBuilderRelease(&p->prev);
    slabBaseBuilderRelease(&p->cur);
}

static void packerFreeOutput(packer *p) {
    for (size_t i = 0; i < p->n; i++) zfree(p->lowers[i]);
    zfree(p->leaves);
    zfree(p->lowers);
    zfree(p->lower_lens);
}

/* Merges a base block with delta records (sorted, one per key; val NULL = tombstone)
 * and feeds the live entries to a packer. */
static void packBaseWithDelta(packer *p, const slabBase *b, const deltaList *dl) {
    slabBaseIter it;
    slabBaseIterInit(&it, b);
    const unsigned char *bk = NULL, *bv = NULL;
    size_t bkl = 0, bvl = 0;
    bool bhas = slabBaseIterNext(&it, &bk, &bkl, &bv, &bvl);
    size_t di = 0;
    while (bhas || di < dl->n) {
        int c = !bhas ? 1 : di == dl->n ? -1
                                        : slabKeyCmp(bk, bkl, dl->recs[di].key, dl->recs[di].klen);
        if (c < 0) {
            packerAdd(p, bk, bkl, bv, bvl);
            bhas = slabBaseIterNext(&it, &bk, &bkl, &bv, &bvl);
        } else {
            if (dl->recs[di].val) packerAdd(p, dl->recs[di].key, dl->recs[di].klen, dl->recs[di].val, dl->recs[di].vlen);
            if (c == 0) bhas = slabBaseIterNext(&it, &bk, &bkl, &bv, &bvl);
            di++;
        }
    }
    slabBaseIterRelease(&it);
}

/* Merges a leaf's base with its visible delta at 'bound' into a packer. */
static void packLeaf(packer *p, const slabLeaf *l, uint64_t bound) {
    deltaList dl = {0};
    leafDeltaCollect(l->delta, bound, &dl);
    packBaseWithDelta(p, l->base, &dl);
    zfree(dl.recs);
}

/* Overlays 'newer' (sorted, one per key) on 'older' (sorted, one per key). */
static void deltaListOverlay(const deltaList *older, const deltaList *newer, deltaList *out) {
    size_t a = 0, b = 0;
    out->n = 0;
    while (a < older->n || b < newer->n) {
        int c = a == older->n ? 1 : b == newer->n ? -1
                                                  : slabKeyCmp(older->recs[a].key, older->recs[a].klen, newer->recs[b].key, newer->recs[b].klen);
        if (c < 0) {
            deltaListPush(out, older->recs[a].key, older->recs[a].klen, older->recs[a].val, older->recs[a].vlen);
            a++;
        } else {
            deltaListPush(out, newer->recs[b].key, newer->recs[b].klen, newer->recs[b].val, newer->recs[b].vlen);
            b++;
            if (c == 0) a++;
        }
    }
}

/* ---------------------------------------------------------------------------------------
 * Compaction triggers
 * ------------------------------------------------------------------------------------- */

/* value * SLAB_RATIO_BP >= ratio_bp * base, without overflow: with base = q * SLAB_RATIO_BP + rem,
 * the threshold is q * ratio_bp + ceil(ratio_bp * rem / SLAB_RATIO_BP). */
static inline bool reachesRatio(uint64_t value, uint32_t ratio_bp, uint64_t base) {
    uint64_t q = base / SLAB_RATIO_BP, rem = base % SLAB_RATIO_BP;
    uint64_t need = ((uint64_t)ratio_bp * rem + SLAB_RATIO_BP - 1) / SLAB_RATIO_BP;
    if (q != 0 && ratio_bp > (UINT64_MAX - need) / q) return false;
    return value >= q * ratio_bp + need;
}

/* max(4 x base, 64 MiB), but below the format's block limit with room for the segment
 * envelope: a leaf's visible delta is saved as one segment, and a larger one would
 * produce an RDB file that cannot be loaded. */
static uint64_t hardCap(const slabBase *b) {
    uint64_t cap = SLAB_HARD_CAP_BASE_FACTOR * b->len;
    if (cap < SLAB_HARD_CAP_FLOOR) cap = SLAB_HARD_CAP_FLOOR;
    uint64_t limit = SLAB_MAX_BLOCK_BYTES - SLAB_HARD_CAP_SLACK;
    return cap < limit ? cap : limit;
}

/* Whether a leaf should be compacted: its delta reached r of the base (entries or
 * payload), or its allocation is mostly overhead, or it passed the hard cap. */
static bool leafNeedsCompaction(const slabConfig *cfg, const slabLeaf *l, bool explicit_req) {
    slabDeltaInfo d;
    slabLeafDeltaCounters(l->delta, &d);
    if (d.entries == 0 && d.bytes == 0) return false;
    if (d.bytes >= hardCap(l->base)) return true;
    if (!cfg->auto_compact && !explicit_req) return false;
    uint32_t r = cfg->compact_ratio_bp;
    if (reachesRatio(d.entries, r, l->base->entries) || reachesRatio(d.payload_bytes, r, l->base->len)) return true;
    slabVlog *own = l->delta ? atomic_load_explicit(&l->delta->own, memory_order_acquire) : NULL;
    uint64_t records = own ? slabVlogRecords(own) : 0;
    uint64_t dense = d.payload_bytes + SLAB_DENSE_RECORD_OVERHEAD * records;
    return reachesRatio(d.bytes, r, l->base->len) && d.bytes >= SLAB_WASTE_FACTOR * dense;
}

/* Candidates are kept by lower bound, so tree rewrites never invalidate them. */
static void candidateAdd(slabMaint *m, const unsigned char *lower, size_t len) {
    for (size_t i = 0; i < m->ncandidates; i++) {
        unsigned char *c = (unsigned char *)(uintptr_t)m->candidates[i];
        uint32_t cl;
        memcpy(&cl, c, 4);
        if (cl == len && memcmp(c + 4, lower, len) == 0) return;
    }
    if (m->ncandidates == m->candidates_cap) {
        m->candidates_cap = m->candidates_cap ? m->candidates_cap * 2 : 16;
        m->candidates = zrealloc(m->candidates, m->candidates_cap * sizeof(uint64_t));
    }
    unsigned char *c = zmalloc(4 + len);
    uint32_t l32 = (uint32_t)len;
    memcpy(c, &l32, 4);
    memcpy(c + 4, lower, len);
    m->candidates[m->ncandidates++] = (uint64_t)(uintptr_t)c;
}

static void candidatesClear(slabMaint *m) {
    for (size_t i = 0; i < m->ncandidates; i++) zfree((void *)(uintptr_t)m->candidates[i]);
    m->ncandidates = 0;
}

void slabMaintRelease(slabMaint *m) {
    candidatesClear(m);
    zfree(m->candidates);
    zfree(m->compact_cursor);
    zfree(m->defrag_bases);
    slabTreeRelease(m->builder_tree);
    m->builder_tree = NULL;
}

/* ---------------------------------------------------------------------------------------
 * Install requests
 * ------------------------------------------------------------------------------------- */

void slabInstallFree(slabInstall *in) {
    slabTreeRelease(in->base);
    slabTreeRelease(in->new_tree);
    zfree(in);
}

/* Applies an install request (main thread). An install is never stale: units of a
 * collection run one at a time, each starts from the tree and flushed_through the
 * previous one installs, installs apply in order, and nothing else replaces a live
 * collection's tree. A unit built on anything else would already have appended into the
 * live deltas below their newest versions, so it is not dropped and retried: it stops
 * the server. */
void slabInstallApply(slabColl *c, slabInstall *in) {
    slabView *cur = c->view;
    assert(cur->tree == in->base && cur->flushed_through == in->base_ft);
    if (in->kind == SLAB_INSTALL_FLUSH) {
        uint32_t k = in->consumed;
        assert(k > 0 && k <= cur->nfrozen && cur->frozen[cur->nfrozen - 1]->id == cur->flushed_through + 1 &&
               cur->frozen[cur->nfrozen - k]->id == in->consumed_through);
    }
    uint32_t keep = in->kind == SLAB_INSTALL_FLUSH ? cur->nfrozen - in->consumed : cur->nfrozen;
    slabView *v = slabViewCreate(keep);
    v->active = cur->active;
    slabMemtableRetain(v->active);
    for (uint32_t i = 0; i < keep; i++) {
        v->frozen[i] = cur->frozen[i];
        slabMemtableRetain(v->frozen[i]);
    }
    if (in->kind == SLAB_INSTALL_FLUSH) {
        for (uint32_t i = keep; i < cur->nfrozen; i++) {
            c->frozen_count--;
            c->frozen_bytes -= cur->frozen[i]->charged_bytes;
        }
        v->flushed_through = in->consumed_through;
    } else {
        v->flushed_through = cur->flushed_through;
    }
    v->tree = in->new_tree ? in->new_tree : cur->tree;
    slabTreeRetain(v->tree);
    v->generation = cur->generation + 1;
    v->delta = in->delta;
    v->config = cur->config;
    slabView *old = c->view;
    c->view = v;
    slabViewRelease(old);
    if (in->done_full_seq > c->compact_done) c->compact_done = in->done_full_seq;
    if (in->done_scan_seq > c->compact_done) c->compact_done = in->done_scan_seq;
    slabInstallFree(in);
}

void slabMaintDropInstalls(slabColl *c) {
    pthread_mutex_lock(&c->install_lock);
    slabInstall *in = c->installs_head;
    c->installs_head = c->installs_tail = NULL;
    pthread_mutex_unlock(&c->install_lock);
    while (in) {
        slabInstall *next = in->next;
        slabInstallFree(in);
        atomic_fetch_sub_explicit(&c->installs_outstanding, 1, memory_order_relaxed);
        in = next;
    }
}

/* ---------------------------------------------------------------------------------------
 * Maintenance units (run by one thread per collection at a time)
 * ------------------------------------------------------------------------------------- */

/* The unit's view of the collection, handed over by the main thread:
 * the frozen memtables still to flush, oldest first, and the config. */
typedef struct unitInput {
    slabMemtable *mems[SLAB_FLUSH_UNIT_MAX_MEMTABLES];
    uint32_t nmems;
    slabConfig config;
} unitInput;

/* Starts the builder from the View of a collection the main thread just built
 * (loaded or copied), before any unit runs. */
static void builderStart(slabColl *c, slabView *v) {
    slabMaint *m = &c->maint;
    slabTreeRelease(m->builder_tree);
    m->builder_tree = v->tree;
    slabTreeRetain(m->builder_tree);
    m->builder_ft = v->flushed_through;
    m->builder_delta = v->delta;
}

/* A flush record routed to its leaf. */
typedef struct flushRouted {
    uint64_t leaf;
    uint32_t mem;
    const slabVlogRec *rec;
} flushRouted;

/* One chunk of a flush's appends: whole leaf groups of the sorted records, and what the
 * chunk found (merged by the unit thread in chunk order). */
typedef struct flushChunk {
    size_t from, to; /* records */
    uint64_t rec_bytes;
    slabDeltaInfo delta_change; /* after minus before, modulo 2^64 */
    uint64_t *cand, *capped;    /* leaf positions: compaction candidates, over the hard cap */
    size_t ncand, ncapped;
} flushChunk;

typedef struct flushCtx {
    slabColl *c;
    const slabTree *t;
    const slabConfig *cfg;
    const flushRouted *sorted;
    uint64_t c_ver;
    flushChunk *chunks;
    size_t nchunks;
} flushCtx;

/* Chunks for 'items' units of work: up to four per maintenance thread, none smaller
 * than a few thousand records' worth, one when running inline. */
static size_t parChunkCount(size_t items) {
    size_t threads = (size_t)slabMaintThreads();
    if (threads < 2 || items < 2 * SLAB_PAR_MIN_CHUNK) return 1;
    size_t n = items / SLAB_PAR_MIN_CHUNK;
    return n < 4 * threads ? n : 4 * threads;
}

static void pushPos(uint64_t **a, size_t *n, uint64_t pos) {
    *a = zrealloc(*a, (*n + 1) * sizeof(uint64_t));
    (*a)[(*n)++] = pos;
}

/* Appends one chunk's leaf groups to their delta logs. Each leaf belongs to one chunk,
 * so every delta log still has one writer. Runs in its own fork section. */
static void flushChunkRun(void *ctx, size_t idx) {
    flushCtx *fx = ctx;
    flushChunk *ch = &fx->chunks[idx];
    slabColl *c = fx->c;
    const flushRouted *sorted = fx->sorted;
    /* In-place appends: a fork child must never see a slot without its record. */
    slabSectionEnter();
    size_t i = ch->from;
    while (i < ch->to) {
        slabSectionYield();
        size_t j = i;
        uint64_t group_bytes = 0;
        while (j < ch->to && sorted[j].leaf == sorted[i].leaf) {
            group_bytes += slabVlogRecordBytes(sorted[j].rec->klen,
                                               slabVlogRecIsTombstone(sorted[j].rec) ? 0 : sorted[j].rec->vlen);
            j++;
        }
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(fx->t, sorted[i].leaf, &pg, &lc);
        assert(l->delta && l->delta->owner == c->id);
        slabDeltaInfo before;
        slabLeafDeltaCounters(l->delta, &before);
        if (before.bytes + group_bytes >= hardCap(l->base)) {
            pushPos(&ch->capped, &ch->ncapped, sorted[i].leaf);
            i = j;
            continue;
        }
        slabVlog *own = atomic_load_explicit(&l->delta->own, memory_order_relaxed);
        if (!own) {
            own = slabVlogCreate(2 * (j - i), group_bytes);
            atomic_fetch_add_explicit(&c->stats.maint_index_bytes, slabVlogCapacity(own) * 8, memory_order_relaxed);
            /* Release: the empty log is complete before a reader can load it. */
            atomic_store_explicit(&l->delta->own, own, memory_order_release);
            atomic_store_explicit(&l->has_delta, 1, memory_order_relaxed);
        }
        uint64_t cap_before = slabVlogCapacity(own);
        slabVlogBegin(own, fx->c_ver, group_bytes);
        for (size_t x = i; x < j; x++) {
            /* A pending fork may proceed between records: every published slot already
             * has its record, and the child never appends to this batch. */
            if (((x - i) & (SLAB_FORK_YIELD_RECORDS - 1)) == SLAB_FORK_YIELD_RECORDS - 1) slabSectionYield();
            const slabVlogRec *r = sorted[x].rec;
            bool tomb = slabVlogRecIsTombstone(r);
            slabVlogAppend(own, r->data, r->klen, slabHash(r->data, r->klen), tomb ? NULL : r->data + r->klen,
                           tomb ? 0 : r->vlen, tomb);
        }
        slabVlogCommit(own);
        if (slabVlogCapacity(own) != cap_before)
            atomic_fetch_add_explicit(&c->stats.maint_index_bytes, slabVlogCapacity(own) * 8, memory_order_relaxed);
        ch->rec_bytes += group_bytes;
        slabDeltaInfo after;
        slabLeafDeltaCounters(l->delta, &after);
        deltaInfoAdd(&ch->delta_change, &before, -1);
        deltaInfoAdd(&ch->delta_change, &after, 1);
        if (leafNeedsCompaction(fx->cfg, l, false)) pushPos(&ch->cand, &ch->ncand, sorted[i].leaf);
        i = j;
    }
    slabSectionLeave();
}

/* Flush: the oldest frozen memtables not yet consumed by the builder. */
static slabInstall *runFlush(slabColl *c, unitInput *in) {
    slabMaint *m = &c->maint;
    slabTree *t = m->builder_tree;
    uint64_t c_ver = in->mems[in->nmems - 1]->id;
    slabInstall *req = zcalloc(sizeof(*req));
    req->kind = SLAB_INSTALL_FLUSH;
    req->base = t;
    slabTreeRetain(t);
    req->base_ft = m->builder_ft;
    req->consumed_through = c_ver;
    req->consumed = in->nmems;
    slabDeltaInfo delta = m->builder_delta;

    if (t->nleaves == 0) {
        /* First flush: merge the memtables' sorted runs (newest wins) and pack. */
        packer p;
        packerInit(&p, &in->config, c->id, (const unsigned char *)"", 0);
        size_t pos[SLAB_FLUSH_UNIT_MAX_MEMTABLES] = {0};
        const slabRun *runs[SLAB_FLUSH_UNIT_MAX_MEMTABLES];
        for (uint32_t i = 0; i < in->nmems; i++) runs[i] = slabMemtableRun(in->mems[i], true);
        for (;;) {
            int best = -1;
            for (int i = (int)in->nmems - 1; i >= 0; i--) {
                if (pos[i] >= runs[i]->n) continue;
                if (best < 0) {
                    best = i;
                    continue;
                }
                const slabVlogRec *a = runs[i]->recs[pos[i]], *b = runs[best]->recs[pos[best]];
                if (slabKeyCmp(a->data, a->klen, b->data, b->klen) < 0) best = i;
            }
            if (best < 0) break;
            const slabVlogRec *r = runs[best]->recs[pos[best]];
            /* Skip the same key in older memtables. */
            for (uint32_t i = 0; i < in->nmems; i++) {
                if ((int)i == best || pos[i] >= runs[i]->n) continue;
                const slabVlogRec *o = runs[i]->recs[pos[i]];
                if (o->klen == r->klen && memcmp(o->data, r->data, r->klen) == 0) pos[i]++;
            }
            if (!slabVlogRecIsTombstone(r)) packerAdd(&p, r->data, r->klen, r->data + r->klen, r->vlen);
            pos[best]++;
        }
        packerFinish(&p);
        if (p.n) req->new_tree = slabTreeFromLeaves(c->id, p.n, p.leaves, p.lowers, p.lower_lens);
        atomic_fetch_add_explicit(&c->stats.maint_base_block_bytes, p.bytes_written, memory_order_relaxed);
        packerFreeOutput(&p);
    } else {
        /* Route each memtable's records to their leaves and append each leaf's group to
         * its delta log as one batch at version c, oldest memtable first so the newest
         * version of a key becomes its head. Readers skip version c until the install
         * raises flushed_through. */
        size_t total = 0;
        for (uint32_t i = 0; i < in->nmems; i++) total += slabVlogEntries(in->mems[i]->log);
        flushRouted *rs = zmalloc((total ? total : 1) * sizeof(flushRouted));
        size_t nr = 0;
        for (uint32_t i = 0; i < in->nmems; i++) {
            slabVlogIter it;
            slabVlogIterInit(&it, in->mems[i]->log, UINT64_MAX);
            const slabVlogRec *r;
            while ((r = slabVlogIterNext(&it))) {
                if (nr == total) {
                    total = total ? total * 2 : 16;
                    rs = zrealloc(rs, total * sizeof(flushRouted));
                }
                rs[nr++] = (flushRouted){posOfId(t, slabTreeRoute(t, r->data, r->klen)), i, r};
            }
        }
        /* Counting sort by leaf, stable in memtable order. */
        uint64_t *counts = zcalloc((t->nleaves + 1) * sizeof(uint64_t));
        for (size_t i = 0; i < nr; i++) counts[rs[i].leaf + 1]++;
        for (uint64_t i = 0; i < t->nleaves; i++) counts[i + 1] += counts[i];
        flushRouted *sorted = zmalloc((nr ? nr : 1) * sizeof(flushRouted));
        for (uint32_t mem = 0; mem < in->nmems; mem++)
            for (size_t i = 0; i < nr; i++)
                if (rs[i].mem == mem) sorted[counts[rs[i].leaf]++] = rs[i];
        zfree(counts);
        zfree(rs);

        /* Chunks of whole leaf groups, about equal in records, appended in parallel. */
        flushCtx fx = {.c = c, .t = t, .cfg = &in->config, .sorted = sorted, .c_ver = c_ver};
        size_t want = parChunkCount(nr);
        fx.chunks = zcalloc(want * sizeof(flushChunk));
        size_t i = 0;
        while (i < nr) {
            size_t end_at = nr * (fx.nchunks + 1) / want;
            size_t j = i + 1;
            while (j < nr && (j < end_at || sorted[j].leaf == sorted[j - 1].leaf)) j++;
            fx.chunks[fx.nchunks++] = (flushChunk){.from = i, .to = j};
            i = j;
        }
        slabParRun(flushChunkRun, &fx, fx.nchunks);

        slabTree *nt = NULL;
        uint64_t rec_bytes = 0;
        uint64_t *capped = NULL;
        size_t ncapped = 0;
        for (size_t k = 0; k < fx.nchunks; k++) {
            flushChunk *ch = &fx.chunks[k];
            rec_bytes += ch->rec_bytes;
            deltaInfoAdd(&delta, &ch->delta_change, 1);
            for (size_t x = 0; x < ch->ncand; x++) {
                uint32_t pg, lc;
                slabTreeLeaf(t, ch->cand[x], &pg, &lc);
                size_t lkl;
                const unsigned char *lk = slabFenceKey(t->pages[pg]->fence, lc, &lkl);
                candidateAdd(m, lk, lkl);
            }
            if (ch->ncapped) {
                capped = zrealloc(capped, (ncapped + ch->ncapped) * sizeof(uint64_t));
                memcpy(capped + ncapped, ch->capped, ch->ncapped * sizeof(uint64_t));
                ncapped += ch->ncapped;
            }
            zfree(ch->cand);
            zfree(ch->capped);
        }
        zfree(fx.chunks);
        atomic_fetch_add_explicit(&c->stats.maint_delta_record_bytes, rec_bytes, memory_order_relaxed);
        /* Rebuild the capped leaves with their pending records, highest position first
         * so earlier positions stay valid. */
        slabTree *cur = t;
        slabTreeRetain(cur);
        for (size_t k = ncapped; k-- > 0;) {
            uint64_t pos = capped[k];
            slabLeaf *l = slabTreeLeaf(cur, pos, &(uint32_t){0}, &(uint32_t){0});
            const unsigned char *lower;
            size_t lower_len;
            slabTreeLower(cur, pos, &lower, &lower_len);
            unsigned char *lcopy = zmalloc(lower_len ? lower_len : 1);
            memcpy(lcopy, lower, lower_len);
            /* Merge base, visible delta and this unit's records for the leaf. The
             * unit's records arrive in memtable order; the last one per key wins. */
            deltaList dl = {0}, pend = {0}, merged = {0};
            leafDeltaCollect(l->delta, m->builder_ft, &dl);
            for (size_t x = 0; x < nr; x++) {
                if (sorted[x].leaf != pos) continue;
                const slabVlogRec *r = sorted[x].rec;
                bool tomb = slabVlogRecIsTombstone(r);
                deltaListPush(&pend, r->data, r->klen, tomb ? NULL : r->data + r->klen, tomb ? 0 : r->vlen);
            }
            stableSortLastWins(&pend);
            deltaListOverlay(&dl, &pend, &merged);
            packer p;
            packerInit(&p, &in->config, c->id, lcopy, lower_len);
            packBaseWithDelta(&p, l->base, &merged);
            packerFinish(&p);
            zfree(merged.recs);
            slabDeltaInfo before;
            slabLeafDeltaCounters(l->delta, &before);
            deltaInfoAdd(&delta, &before, -1);
            slabTree *next = slabTreeReplace(cur, pos, pos + 1, p.n, p.leaves, p.lowers, p.lower_lens);
            atomic_fetch_add_explicit(&c->stats.maint_base_block_bytes, p.bytes_written, memory_order_relaxed);
            packerFreeOutput(&p);
            zfree(lcopy);
            zfree(dl.recs);
            zfree(pend.recs);
            slabTreeRelease(cur);
            cur = next;
        }
        if (ncapped)
            nt = cur;
        else
            slabTreeRelease(cur);
        zfree(capped);
        zfree(sorted);
        req->new_tree = nt;
    }
    req->delta = delta;
    /* Advance the builder: the next unit chains on this one, installed or not. */
    if (req->new_tree) {
        slabTreeRelease(m->builder_tree);
        m->builder_tree = req->new_tree;
        slabTreeRetain(req->new_tree);
    }
    m->builder_ft = c_ver;
    m->builder_delta = delta;
    atomic_fetch_add_explicit(&c->stats.flushes, 1, memory_order_relaxed);
    return req;
}

typedef struct candPick {
    double priority;
    uint64_t pos;
    unsigned char *cand;
} candPick;

static int candCmpDesc(const void *a, const void *b) {
    const candPick *x = a, *y = b;
    return (x->priority < y->priority) - (x->priority > y->priority);
}

/* One run of contiguous leaves a compaction rewrites, packed by any worker. */
typedef struct compactRun {
    uint64_t from, to;
    unsigned char *lower;
    packer p;
    slabDeltaInfo delta_removed;
} compactRun;

typedef struct compactCtx {
    slabColl *c;
    const slabTree *t;
    const slabConfig *cfg;
    uint64_t ft;
    compactRun *runs;
    size_t nruns;
} compactCtx;

/* Packs one run from the builder tree; writes nothing shared, so no fork section. */
static void compactRunPack(void *ctx, size_t idx) {
    compactCtx *cx = ctx;
    compactRun *run = &cx->runs[idx];
    const unsigned char *lower;
    size_t lower_len;
    slabTreeLower(cx->t, run->from, &lower, &lower_len);
    run->lower = zmalloc(lower_len ? lower_len : 1);
    memcpy(run->lower, lower, lower_len);
    packerInit(&run->p, cx->cfg, cx->c->id, run->lower, lower_len);
    for (uint64_t x = run->from; x < run->to; x++) {
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(cx->t, x, &pg, &lc);
        slabDeltaInfo d;
        slabLeafDeltaCounters(l->delta, &d);
        deltaInfoAdd(&run->delta_removed, &d, 1);
        /* A delta record above the builder's bound belongs to an uninstalled flush and
         * would be lost; such leaves are never chosen while a flush is pending because
         * units are serialized and chain on the builder. */
        packLeaf(&run->p, l, cx->ft);
    }
    packerFinish(&run->p);
}

/* Compaction: rewrites candidate leaves (or, in a FULL pass, every leaf with a delta and
 * runs of small leaves), merging each base with its delta as of the builder's
 * flushed_through, in runs of contiguous leaves. */
static slabInstall *runCompaction(slabColl *c, unitInput *in) {
    slabMaint *m = &c->maint;
    slabTree *t = m->builder_tree;
    uint64_t ft = m->builder_ft;
    /* Pick leaves: positions with a priority. */
    size_t npick = 0, cap = 64;
    double *pick = zmalloc(cap * 2 * sizeof(double)); /* (priority, position) pairs */
    bool full = m->full_compact;
    uint64_t input_bytes = 0;
    if (full) {
        uint64_t pos = 0;
        if (m->compact_cursor && t->nleaves) pos = posOfId(t, slabTreeRoute(t, m->compact_cursor, m->compact_cursor_len));
        uint64_t small = in->config.block_bytes / 4;
        while (pos < t->nleaves && npick < SLAB_COMPACT_UNIT_MAX_LEAVES && input_bytes < SLAB_COMPACT_UNIT_MAX_BYTES) {
            uint32_t pg, lc;
            slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
            slabDeltaInfo d;
            slabLeafDeltaCounters(l->delta, &d);
            bool small_pair = false;
            if (l->base->len < small && pos + 1 < t->nleaves) {
                slabLeaf *nx = slabTreeLeaf(t, pos + 1, &pg, &lc);
                small_pair = nx->base->len < small;
            }
            if (d.entries || small_pair) {
                if (npick == cap) {
                    cap *= 2;
                    pick = zrealloc(pick, cap * 2 * sizeof(double));
                }
                pick[2 * npick] = 0;
                pick[2 * npick + 1] = (double)pos;
                npick++;
                input_bytes += l->base->len + d.bytes;
                if (small_pair && npick < SLAB_COMPACT_UNIT_MAX_LEAVES) {
                    pick[2 * npick] = 0;
                    pick[2 * npick + 1] = (double)(pos + 1);
                    npick++;
                    pos++;
                }
            }
            pos++;
        }
        zfree(m->compact_cursor);
        m->compact_cursor = NULL;
        if (pos >= t->nleaves) {
            m->full_compact = false;
        } else {
            const unsigned char *k;
            size_t kl;
            slabTreeLower(t, pos, &k, &kl);
            m->compact_cursor = zmalloc(kl ? kl : 1);
            memcpy(m->compact_cursor, k, kl);
            m->compact_cursor_len = kl;
        }
    } else {
        /* Every candidate still over its triggers, by priority (delta bytes per base
         * byte). The unit takes the highest within its bounds; the rest stay candidates,
         * so leaves late in key order are not starved by earlier ones. */
        candPick *cp = zmalloc((m->ncandidates ? m->ncandidates : 1) * sizeof(candPick));
        size_t ncp = 0;
        for (size_t i = 0; i < m->ncandidates; i++) {
            unsigned char *cd = (unsigned char *)(uintptr_t)m->candidates[i];
            uint32_t cl;
            memcpy(&cl, cd, 4);
            uint64_t p = t->nleaves ? posOfId(t, slabTreeRoute(t, cd + 4, cl)) : 0;
            uint32_t pg, lc;
            slabLeaf *l = t->nleaves ? slabTreeLeaf(t, p, &pg, &lc) : NULL;
            if (!l || !leafNeedsCompaction(&in->config, l, m->explicit_cands)) {
                zfree(cd);
                continue;
            }
            slabDeltaInfo d;
            slabLeafDeltaCounters(l->delta, &d);
            cp[ncp++] = (candPick){(double)d.bytes / (double)(l->base->len ? l->base->len : 1), p, cd};
        }
        m->ncandidates = 0;
        qsort(cp, ncp, sizeof(candPick), candCmpDesc);
        size_t take = 0;
        while (take < ncp && take < SLAB_COMPACT_UNIT_MAX_LEAVES) {
            if (take > 0 && input_bytes >= SLAB_COMPACT_UNIT_MAX_BYTES) break;
            uint32_t pg, lc;
            input_bytes += slabTreeLeaf(t, cp[take].pos, &pg, &lc)->base->len;
            take++;
        }
        if (take > cap) pick = zrealloc(pick, take * 2 * sizeof(double));
        for (size_t i = 0; i < ncp; i++) {
            if (i < take) {
                pick[2 * npick] = cp[i].priority;
                pick[2 * npick + 1] = (double)cp[i].pos;
                npick++;
                zfree(cp[i].cand);
            } else {
                m->candidates[m->ncandidates++] = (uint64_t)(uintptr_t)cp[i].cand;
            }
        }
        zfree(cp);
    }
    /* Positions ascending, deduplicated. */
    uint64_t *pos = zmalloc((npick ? npick : 1) * sizeof(uint64_t));
    for (size_t i = 0; i < npick; i++) pos[i] = (uint64_t)pick[2 * i + 1];
    zfree(pick);
    for (size_t i = 1; i < npick; i++)
        for (size_t j = i; j > 0 && pos[j - 1] > pos[j]; j--) {
            uint64_t tmp = pos[j];
            pos[j] = pos[j - 1];
            pos[j - 1] = tmp;
        }
    /* Two stored lower bounds can route to the same leaf (it split or merged since). */
    size_t uniq = 0;
    for (size_t i = 0; i < npick; i++)
        if (uniq == 0 || pos[uniq - 1] != pos[i]) pos[uniq++] = pos[i];
    npick = uniq;

    slabInstall *req = zcalloc(sizeof(*req));
    req->kind = SLAB_INSTALL_COMPACTION;
    req->base = t;
    slabTreeRetain(t);
    req->base_ft = ft;
    slabDeltaInfo delta = m->builder_delta;
    /* Runs of contiguous positions (split in non-FULL units so they spread over
     * threads), packed in parallel from the builder tree, then put in place from the
     * highest so lower positions stay valid in the intermediate trees. */
    compactCtx cx = {.c = c, .t = t, .cfg = &in->config, .ft = ft};
    cx.runs = zmalloc((npick ? npick : 1) * sizeof(compactRun));
    for (size_t r = 0; r < npick;) {
        size_t e = r + 1;
        while (e < npick && pos[e - 1] + 1 == pos[e] && (full || e - r < SLAB_COMPACT_RUN_SPLIT)) e++;
        cx.runs[cx.nruns++] = (compactRun){.from = pos[r], .to = pos[e - 1] + 1};
        r = e;
    }
    slabParRun(compactRunPack, &cx, cx.nruns);
    slabTree *cur = t;
    slabTreeRetain(cur);
    for (size_t r = cx.nruns; r-- > 0;) {
        compactRun *run = &cx.runs[r];
        deltaInfoAdd(&delta, &run->delta_removed, -1);
        slabTree *next;
        if (run->p.n == 0 && run->from == 0 && run->to == cur->nleaves) {
            /* Everything was deleted: an empty tree. */
            next = slabTreeEmpty(c->id);
        } else {
            next = slabTreeReplace(cur, run->from, run->to, run->p.n, run->p.leaves, run->p.lowers, run->p.lower_lens);
        }
        atomic_fetch_add_explicit(&c->stats.maint_base_block_bytes, run->p.bytes_written, memory_order_relaxed);
        packerFreeOutput(&run->p);
        zfree(run->lower);
        slabTreeRelease(cur);
        cur = next;
    }
    zfree(cx.runs);
    zfree(pos);
    req->new_tree = cur;
    req->delta = delta;
    slabTreeRelease(m->builder_tree);
    m->builder_tree = cur;
    slabTreeRetain(cur);
    m->builder_delta = delta;
    atomic_fetch_add_explicit(&c->stats.compactions, 1, memory_order_relaxed);
    return req;
}

/* The copy re-wrap: gives every leaf of a copied tree a delta of its own, holding the
 * shared delta visible at flushed_through as an immutable segment, so the copy never
 * appends to the source's logs. */
static slabInstall *runRewrap(slabColl *c) {
    slabMaint *m = &c->maint;
    slabTree *t = m->builder_tree;
    size_t n = t->nleaves;
    slabLeaf *leaves = zmalloc((n ? n : 1) * sizeof(slabLeaf));
    unsigned char **lowers = zmalloc((n ? n : 1) * sizeof(unsigned char *));
    size_t *lens = zmalloc((n ? n : 1) * sizeof(size_t));
    slabDeltaInfo delta = {0};
    uint64_t rewrap_bytes = 0;
    for (uint64_t pos = 0; pos < n; pos++) {
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
        const unsigned char *lk = slabFenceKey(t->pages[pg]->fence, lc, &lens[pos]);
        lowers[pos] = zmalloc(lens[pos] ? lens[pos] : 1);
        memcpy(lowers[pos], lk, lens[pos]);
        deltaList dl = {0};
        leafDeltaCollect(l->delta, m->builder_ft, &dl);
        slabDeltaSeg *seg = NULL;
        if (dl.n) {
            slabSegWriter w;
            slabSegWriterInit(&w);
            for (size_t i = 0; i < dl.n; i++)
                slabSegWriterAdd(&w, dl.recs[i].key, dl.recs[i].klen, dl.recs[i].val, dl.recs[i].vlen, dl.recs[i].val == NULL);
            size_t len;
            unsigned char *buf = slabSegWriterFinish(&w, &len);
            const char *err;
            seg = slabDeltaSegLoad(buf, len, &err);
            assert(seg);
            rewrap_bytes += len;
        }
        zfree(dl.recs);
        slabBaseRetain(l->base);
        leaves[pos] = (slabLeaf){l->base, leafDeltaCreate(c->id, seg), seg != NULL};
        slabDeltaInfo d;
        slabLeafDeltaCounters(leaves[pos].delta, &d);
        deltaInfoAdd(&delta, &d, 1);
    }
    slabTree *nt = slabTreeFromLeaves(c->id, n, leaves, lowers, lens);
    for (size_t i = 0; i < n; i++) zfree(lowers[i]);
    zfree(leaves);
    zfree(lowers);
    zfree(lens);
    slabInstall *req = zcalloc(sizeof(*req));
    req->kind = SLAB_INSTALL_REWRAP;
    req->base = t;
    slabTreeRetain(t);
    req->base_ft = m->builder_ft;
    req->new_tree = nt;
    req->delta = delta;
    slabTreeRelease(m->builder_tree);
    m->builder_tree = nt;
    slabTreeRetain(nt);
    m->builder_delta = delta;
    m->foreign = false;
    m->scan_leaves = true;
    atomic_fetch_add_explicit(&c->stats.maint_rewrap_bytes, rewrap_bytes, memory_order_relaxed);
    return req;
}

/* Active defrag: rebuilds up to SLAB_COMPACT_UNIT_MAX_LEAVES leaves whose base block
 * the main thread asked to move, as copies in new allocations with the same content and
 * delta. The install replaces the leaves (and the touched pages, reallocated by the
 * copy-on-write) like a compaction; the old blocks are freed when their last reader drops
 * them. A requested base that is gone by now (compacted, or freed and its address reused)
 * costs at most one needless copy. The copies use zmalloc, not allocatorDefragAlloc: the
 * old block is freed later through zfree, so both sides must be counted in used_memory. */
static int ptrCmp(const void *a, const void *b) {
    uintptr_t x = (uintptr_t) * (void *const *)a, y = (uintptr_t) * (void *const *)b;
    return x < y ? -1 : x > y;
}

static slabInstall *runDefrag(slabColl *c) {
    slabMaint *m = &c->maint;
    slabTree *cur = m->builder_tree;
    slabTreeRetain(cur);
    uint64_t pos = m->defrag_cursor, moved = 0;
    while (pos < cur->nleaves && moved < SLAB_COMPACT_UNIT_MAX_LEAVES) {
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(cur, pos, &pg, &lc);
        void *key = l->base;
        if (bsearch(&key, m->defrag_bases, m->ndefrag_bases, sizeof(void *), ptrCmp)) {
            const unsigned char *lk;
            size_t lkl;
            slabTreeLower(cur, pos, &lk, &lkl);
            unsigned char *lcopy = zmalloc(lkl ? lkl : 1);
            memcpy(lcopy, lk, lkl);
            slabLeaf nl = {slabBaseMove(l->base), l->delta, atomic_load(&l->has_delta)};
            if (nl.delta) slabLeafDeltaRetain(nl.delta);
            slabTree *next = slabTreeReplace(cur, pos, pos + 1, 1, &nl, &lcopy, &lkl);
            zfree(lcopy);
            slabTreeRelease(cur);
            cur = next;
            moved++;
        }
        pos++;
    }
    m->defrag_cursor = pos;
    if (pos >= cur->nleaves) {
        zfree(m->defrag_bases);
        m->defrag_bases = NULL;
        m->ndefrag_bases = 0;
        m->defrag_cursor = 0;
    }
    if (!moved) {
        slabTreeRelease(cur);
        return NULL;
    }
    slabInstall *req = zcalloc(sizeof(*req));
    req->kind = SLAB_INSTALL_COMPACTION;
    req->base = m->builder_tree;
    slabTreeRetain(req->base);
    req->base_ft = m->builder_ft;
    req->new_tree = cur;
    req->delta = m->builder_delta;
    slabTreeRelease(m->builder_tree);
    m->builder_tree = cur;
    slabTreeRetain(cur);
    atomic_fetch_add_explicit(&c->stats.defragged_leaves, moved, memory_order_relaxed);
    return req;
}

/* Moves the bases the main thread queued into the pass, unless one is running. */
static void takeDefragRequest(slabColl *c) {
    slabMaint *m = &c->maint;
    if (m->defrag_bases) return;
    pthread_mutex_lock(&c->install_lock);
    m->defrag_bases = c->defrag_req;
    m->ndefrag_bases = c->ndefrag_req;
    c->defrag_req = NULL;
    c->ndefrag_req = c->defrag_req_cap = 0;
    pthread_mutex_unlock(&c->install_lock);
    if (m->defrag_bases) qsort(m->defrag_bases, m->ndefrag_bases, sizeof(void *), ptrCmp);
    m->defrag_cursor = 0;
}

void slabRequestDefrag(slabColl *c, int (*should_move)(void *ptr)) {
    const slabTree *t = c->view->tree;
    if (t->nleaves == 0) return;
    if (c->defrag_scan >= t->nleaves) c->defrag_scan = 0;
    uint64_t end = c->defrag_scan + SLAB_DEFRAG_SCAN_LEAVES;
    if (end > t->nleaves) end = t->nleaves;
    bool queued = false;
    for (uint64_t pos = c->defrag_scan; pos < end; pos++) {
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
        if (!should_move(l->base) && !should_move((void *)l->base->buf)) continue;
        pthread_mutex_lock(&c->install_lock);
        if (c->ndefrag_req == c->defrag_req_cap) {
            c->defrag_req_cap = c->defrag_req_cap ? c->defrag_req_cap * 2 : 64;
            c->defrag_req = zrealloc(c->defrag_req, c->defrag_req_cap * sizeof(void *));
        }
        c->defrag_req[c->ndefrag_req++] = l->base;
        pthread_mutex_unlock(&c->install_lock);
        queued = true;
    }
    c->defrag_scan = end;
    if (queued) slabMaintNotify(c);
}

/* After an RDB load or a copy, evaluates every leaf's triggers once. */
static void scanLeaves(slabColl *c, const slabConfig *cfg) {
    slabMaint *m = &c->maint;
    slabTree *t = m->builder_tree;
    for (uint64_t pos = 0; pos < t->nleaves; pos++) {
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
        if (leafNeedsCompaction(cfg, l, false)) {
            size_t kl;
            const unsigned char *k = slabFenceKey(t->pages[pg]->fence, lc, &kl);
            candidateAdd(m, k, kl);
        }
    }
    m->scan_leaves = false;
}

/* ---------------------------------------------------------------------------------------
 * Unit selection (any thread; slab_maint.c runs it)
 * ------------------------------------------------------------------------------------- */

/* Whether a leaf scan for compaction candidates should evaluate explicit requests. */
static void scanForRequest(slabColl *c, const slabConfig *cfg) {
    slabMaint *m = &c->maint;
    slabTree *t = m->builder_tree;
    for (uint64_t pos = 0; pos < t->nleaves; pos++) {
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
        if (leafNeedsCompaction(cfg, l, true)) {
            size_t kl;
            const unsigned char *k = slabFenceKey(t->pages[pg]->fence, lc, &kl);
            candidateAdd(m, k, kl);
        }
    }
}

/* Chooses and runs one bounded unit on the View the main thread handed over; returns
 * its install request, or NULL when there is nothing to do. Flushes go first, but a
 * ready compaction gets every fourth unit. With 'allow_compaction' false (a fork child
 * exists) only flushes run. */
slabInstall *slabUnitRun(slabColl *c, slabView *v, bool allow_compaction) {
    slabMaint *m = &c->maint;
    if (!m->builder_tree) {
        m->builder_tree = v->tree;
        slabTreeRetain(m->builder_tree);
    }
    unitInput in;
    in.config = v->config;
    in.nmems = 0;
    if (m->foreign) return runRewrap(c);
    uint64_t seq = atomic_exchange(&c->req_full_seq, 0);
    if (seq) {
        /* A new request restarts the pass: it must see what was written before it. */
        m->full_compact = true;
        zfree(m->compact_cursor);
        m->compact_cursor = NULL;
        if (seq > m->full_seq) m->full_seq = seq;
    }
    seq = atomic_exchange(&c->req_scan_seq, 0);
    if (seq) {
        scanForRequest(c, &in.config);
        m->explicit_cands = true;
        if (seq > m->scan_seq) m->scan_seq = seq;
    }
    takeDefragRequest(c);
    if (m->scan_leaves) scanLeaves(c, &in.config);
    /* Frozen memtables beyond the builder, oldest first, bounded per unit. */
    uint64_t bytes = 0;
    for (uint32_t i = v->nfrozen; i-- > 0;) {
        slabMemtable *mt = v->frozen[i];
        if (mt->id <= m->builder_ft) continue;
        if (in.nmems == SLAB_FLUSH_UNIT_MAX_MEMTABLES || (in.nmems && bytes >= SLAB_FLUSH_UNIT_MAX_BYTES)) break;
        in.mems[in.nmems++] = mt;
        bytes += slabMemtableBytes(mt);
    }
    bool compact_ready = allow_compaction && (m->full_compact || m->ncandidates > 0);
    slabInstall *req = NULL;
    if (in.nmems && !(compact_ready && m->flush_streak >= 3)) {
        m->flush_streak++;
        atomic_store_explicit(&c->flush_inflight, 1, memory_order_relaxed);
        req = runFlush(c, &in);
        atomic_store_explicit(&c->flush_inflight, 0, memory_order_relaxed);
    } else if (compact_ready) {
        m->flush_streak = 0;
        atomic_store_explicit(&c->compaction_inflight, 1, memory_order_relaxed);
        req = runCompaction(c, &in);
        atomic_store_explicit(&c->compaction_inflight, 0, memory_order_relaxed);
    } else if (allow_compaction && m->defrag_bases) {
        /* Last priority: a pass that finds nothing to move installs nothing. */
        while (m->defrag_bases && !req) req = runDefrag(c);
    }
    /* Explicit requests this unit completes travel with its install (an empty one if
     * the unit had nothing to do), so a WAIT returns once the work is published. */
    uint64_t done_full = 0, done_scan = 0;
    if (m->full_seq && !m->full_compact) {
        done_full = m->full_seq;
        m->full_seq = 0;
    }
    if (m->scan_seq && m->ncandidates == 0) {
        done_scan = m->scan_seq;
        m->scan_seq = 0;
        m->explicit_cands = false;
    }
    if ((done_full || done_scan) && !req) {
        req = zcalloc(sizeof(*req));
        req->kind = SLAB_INSTALL_COMPACTION;
        req->base = m->builder_tree;
        slabTreeRetain(req->base);
        req->base_ft = m->builder_ft;
        req->delta = m->builder_delta;
    }
    if (req) {
        req->done_full_seq = done_full;
        req->done_scan_seq = done_scan;
    }
    atomic_store_explicit(&c->compact_pending, m->full_compact || m->ncandidates > 0, memory_order_relaxed);
    return req;
}

uint64_t slabRequestFlush(slabColl *c, uint64_t *target) {
    uint64_t n = slabFreeze(c);
    slabView *v = c->view;
    *target = v->nfrozen ? v->frozen[0]->id : v->flushed_through;
    slabMaintNotify(c);
    return n;
}

bool slabFlushDone(const slabColl *c, uint64_t target) {
    return c->view->flushed_through >= target;
}

/* SLCOMPACT (main thread). FULL starts a pass over every leaf with a delta and runs of
 * small leaves; without FULL every leaf over its triggers is scheduled now. The count
 * returned is taken from the published tree. */
uint64_t slabRequestCompaction(slabColl *c, bool full, uint64_t *target) {
    uint64_t scheduled = 0;
    *target = ++c->compact_requested;
    if (full) {
        atomic_store(&c->req_full_seq, *target);
    } else {
        slabTree *t = c->view->tree;
        for (uint64_t pos = 0; pos < t->nleaves; pos++) {
            uint32_t pg, lc;
            slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
            if (leafNeedsCompaction(&c->view->config, l, true)) scheduled++;
        }
        atomic_store(&c->req_scan_seq, *target);
    }
    atomic_store_explicit(&c->compact_pending, 1, memory_order_relaxed);
    slabMaintNotify(c);
    return scheduled;
}

bool slabCompactionDone(const slabColl *c, uint64_t target) {
    return c->compact_done >= target;
}

/* ---------------------------------------------------------------------------------------
 * COPY
 * ------------------------------------------------------------------------------------- */

slabColl *slabCollCopy(slabColl *src) {
    slabFreeze(src);
    slabView *sv = src->view;
    slabColl *c = slabCollCreate(&sv->config);
    slabView *v = slabViewCreate(sv->nfrozen);
    v->active = c->view->active;
    for (uint32_t i = 0; i < sv->nfrozen; i++) {
        v->frozen[i] = sv->frozen[i];
        slabMemtableRetain(v->frozen[i]);
    }
    v->tree = sv->tree;
    slabTreeRetain(v->tree);
    v->flushed_through = sv->flushed_through;
    v->delta = sv->delta;
    v->config = sv->config;
    v->generation = 1;
    slabViewRelease(c->view);
    c->view = v;
    c->next_seq = src->next_seq;
    c->next_memtable_id = src->next_memtable_id;
    c->frozen_count = src->frozen_count;
    c->frozen_bytes = src->frozen_bytes;
    builderStart(c, v);
    c->maint.foreign = v->tree->nleaves > 0;
    slabMaintNotify(c);
    return c;
}

/* ---------------------------------------------------------------------------------------
 * Merge iterator
 * ------------------------------------------------------------------------------------- */

typedef struct memLayer {
    const slabRun *run;
    bool owned;
    size_t pos;
} memLayer;

struct slabIter {
    slabSnapshot snap;
    bool fork_safe;
    memLayer *mem; /* the active memtable and every frozen one */
    uint32_t nmem;
    /* tree stream */
    const slabTree *t;
    uint64_t leaf;
    bool leaf_open;
    slabBaseIter base;
    bool base_has;
    const unsigned char *bk, *bv;
    size_t bkl, bvl;
    deltaList dl;
    size_t di;
    const unsigned char *start;
    size_t start_len;
    bool first_leaf;
    /* budget */
    uint64_t budget, visited;
    bool stopped;
    unsigned char *last;
    size_t last_len, last_cap;
    /* the key returned last (copied: layers advance before the next call) */
    unsigned char *out;
    size_t out_cap;
};

static size_t runLowerBound(const slabRun *r, const unsigned char *k, size_t kl) {
    size_t lo = 0, hi = r->n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (slabKeyCmp(r->recs[mid]->data, r->recs[mid]->klen, k, kl) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

static void iterOpenLeaf(slabIter *it) {
    const slabTree *t = it->t;
    uint32_t pg, lc;
    slabLeaf *l = slabTreeLeaf(t, it->leaf, &pg, &lc);
    if (it->leaf_open) slabBaseIterRelease(&it->base);
    slabBaseIterInit(&it->base, l->base);
    leafDeltaCollect(l->delta, it->snap.view->flushed_through, &it->dl);
    it->di = 0;
    if (it->first_leaf && it->start_len) {
        slabBaseIterSeek(&it->base, it->start, it->start_len);
        size_t lo = 0, hi = it->dl.n;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (slabKeyCmp(it->dl.recs[mid].key, it->dl.recs[mid].klen, it->start, it->start_len) < 0)
                lo = mid + 1;
            else
                hi = mid;
        }
        it->di = lo;
    }
    it->first_leaf = false;
    it->leaf_open = true;
    it->base_has = slabBaseIterNext(&it->base, &it->bk, &it->bkl, &it->bv, &it->bvl);
}

/* The tree layer's head: the smaller of the leaf's base and delta heads, moving to the
 * next leaf when both are exhausted. */
static bool treeHead(slabIter *it, const unsigned char **k, size_t *kl) {
    while (it->leaf < it->t->nleaves) {
        if (!it->leaf_open) iterOpenLeaf(it);
        bool dh = it->di < it->dl.n;
        if (it->base_has || dh) {
            if (it->base_has && (!dh || slabKeyCmp(it->bk, it->bkl, it->dl.recs[it->di].key, it->dl.recs[it->di].klen) <= 0)) {
                *k = it->bk;
                *kl = it->bkl;
            } else {
                *k = it->dl.recs[it->di].key;
                *kl = it->dl.recs[it->di].klen;
            }
            return true;
        }
        it->leaf++;
        slabBaseIterRelease(&it->base);
        it->leaf_open = false;
    }
    return false;
}

bool slabSnapshotPrepareRun(const slabSnapshot *s, uint32_t i) {
    if (i >= s->view->nfrozen) return false;
    slabMemtableRun(s->view->frozen[i], true);
    return true;
}

slabIter *slabIterCreate(const slabSnapshot *s, const unsigned char *start, size_t start_len, bool fork_safe) {
    slabIter *it = zcalloc(sizeof(*it));
    it->snap = *s;
    it->snap.owned = false;
    it->fork_safe = fork_safe;
    const slabView *v = s->view;
    it->mem = zcalloc((1 + v->nfrozen) * sizeof(memLayer));
    if (v->active->log && slabVlogEntries(v->active->log)) {
        /* The active memtable is sorted privately, always under the snapshot's bound:
         * it may have been frozen since, and its shared run would hold later writes. */
        it->mem[it->nmem].run = slabRunBuild(v->active->log, s->bound);
        it->mem[it->nmem].owned = true;
        it->nmem++;
    }
    for (uint32_t i = 0; i < v->nfrozen; i++) {
        slabMemtable *m = v->frozen[i];
        slabRun *r = atomic_load_explicit(&m->run, memory_order_acquire);
        if (r) {
            it->mem[it->nmem].run = r;
            it->mem[it->nmem].owned = false;
        } else {
            it->mem[it->nmem].run = slabMemtableRun(m, !fork_safe);
            it->mem[it->nmem].owned = fork_safe;
        }
        it->nmem++;
    }
    for (uint32_t i = 0; i < it->nmem; i++) it->mem[i].pos = runLowerBound(it->mem[i].run, start, start_len);
    it->t = v->tree;
    it->start = start;
    it->start_len = start_len;
    it->first_leaf = true;
    it->leaf = v->tree->nleaves && start_len ? posOfId(v->tree, slabTreeRoute(v->tree, start, start_len)) : 0;
    return it;
}

void slabIterSetBudget(slabIter *it, uint64_t visits) {
    it->budget = visits;
}

bool slabIterStoppedOnBudget(const slabIter *it) {
    return it->stopped;
}

void slabIterLastKey(const slabIter *it, const unsigned char **key, size_t *klen) {
    *key = it->last;
    *klen = it->last_len;
}

static void copyKey(unsigned char **buf, size_t *cap, size_t *len, const unsigned char *k, size_t kl) {
    if (kl > *cap || !*buf) {
        *cap = kl > 64 ? kl : 64;
        *buf = zrealloc(*buf, *cap);
    }
    memcpy(*buf, k, kl);
    *len = kl;
}

bool slabIterNext(slabIter *it, const unsigned char **key, size_t *klen, const unsigned char **val, size_t *vlen) {
    for (;;) {
        const unsigned char *mk = NULL;
        size_t mkl = 0;
        for (uint32_t i = 0; i < it->nmem; i++) {
            memLayer *ml = &it->mem[i];
            if (ml->pos >= ml->run->n) continue;
            const slabVlogRec *r = ml->run->recs[ml->pos];
            if (!mk || slabKeyCmp(r->data, r->klen, mk, mkl) < 0) mk = r->data, mkl = r->klen;
        }
        const unsigned char *tk;
        size_t tkl;
        bool th = treeHead(it, &tk, &tkl);
        if (th && (!mk || slabKeyCmp(tk, tkl, mk, mkl) < 0)) mk = tk, mkl = tkl;
        if (!mk) return false;
        if (it->budget && it->visited >= it->budget) {
            it->stopped = true;
            return false;
        }
        /* Copy the key: advancing the layers may move the buffer it lives in. */
        size_t outlen;
        copyKey(&it->out, &it->out_cap, &outlen, mk, mkl);
        mk = it->out;
        /* The newest layer holding the key wins; every layer holding it advances. */
        const unsigned char *wv = NULL;
        size_t wvl = 0;
        bool found = false, tomb = false;
        for (uint32_t i = 0; i < it->nmem; i++) {
            memLayer *ml = &it->mem[i];
            if (ml->pos >= ml->run->n) continue;
            const slabVlogRec *r = ml->run->recs[ml->pos];
            if (slabKeyCmp(r->data, r->klen, mk, mkl) != 0) continue;
            if (!found) {
                found = true;
                tomb = slabVlogRecIsTombstone(r);
                wv = r->data + r->klen;
                wvl = r->vlen;
            }
            ml->pos++;
            it->visited++;
        }
        if (th && slabKeyCmp(tk, tkl, mk, mkl) == 0) {
            bool dmatch = it->di < it->dl.n && slabKeyCmp(it->dl.recs[it->di].key, it->dl.recs[it->di].klen, mk, mkl) == 0;
            bool bmatch = it->base_has && slabKeyCmp(it->bk, it->bkl, mk, mkl) == 0;
            if (!found) {
                found = true;
                if (dmatch) {
                    tomb = it->dl.recs[it->di].val == NULL;
                    wv = it->dl.recs[it->di].val;
                    wvl = it->dl.recs[it->di].vlen;
                } else {
                    tomb = false;
                    wv = it->bv;
                    wvl = it->bvl;
                }
            }
            /* The value may live in the base iterator's current entry; it stays valid
             * (the block is immutable), only the key buffer is reused. */
            if (dmatch) {
                it->di++;
                it->visited++;
            }
            if (bmatch) {
                it->base_has = slabBaseIterNext(&it->base, &it->bk, &it->bkl, &it->bv, &it->bvl);
                it->visited++;
            }
        }
        copyKey(&it->last, &it->last_cap, &it->last_len, mk, mkl);
        if (tomb) continue;
        *key = it->out;
        *klen = mkl;
        *val = wv;
        *vlen = wvl;
        return true;
    }
}

void slabIterFree(slabIter *it) {
    for (uint32_t i = 0; i < it->nmem; i++)
        if (it->mem[i].owned) slabRunFree((slabRun *)it->mem[i].run);
    zfree(it->mem);
    if (it->leaf_open) slabBaseIterRelease(&it->base);
    zfree(it->dl.recs);
    zfree(it->last);
    zfree(it->out);
    zfree(it);
}

/* ---------------------------------------------------------------------------------------
 * Export and import
 * ------------------------------------------------------------------------------------- */

struct slabExport {
    slabSnapshot snap;
    memLayer *mem; /* the active memtable and every frozen one */
    uint32_t nmem;
    uint64_t pending;
    bool counted;
};

slabExport *slabExportBegin(const slabSnapshot *s) {
    slabExport *e = zcalloc(sizeof(*e));
    e->snap = *s;
    e->snap.owned = false;
    const slabView *v = s->view;
    e->mem = zcalloc((1 + v->nfrozen) * sizeof(memLayer));
    /* Fork-safe: runs are built privately unless already published. */
    if (v->active->log) {
        e->mem[e->nmem].run = slabRunBuild(v->active->log, s->bound);
        e->mem[e->nmem].owned = true;
        e->nmem++;
    }
    for (uint32_t i = 0; i < v->nfrozen; i++) {
        slabRun *r = atomic_load_explicit(&v->frozen[i]->run, memory_order_acquire);
        e->mem[e->nmem].run = r ? r : slabRunBuild(v->frozen[i]->log, UINT64_MAX);
        e->mem[e->nmem].owned = r == NULL;
        e->nmem++;
    }
    return e;
}

uint64_t slabExportLeafCount(const slabExport *e) {
    return e->snap.view->tree->nleaves;
}

void slabExportLeaf(slabExport *e, uint64_t i, const unsigned char **lower, size_t *lower_len, const unsigned char **base, size_t *base_len, unsigned char **delta, size_t *delta_len) {
    const slabTree *t = e->snap.view->tree;
    uint32_t pg, lc;
    slabLeaf *l = slabTreeLeaf(t, i, &pg, &lc);
    *lower = slabFenceKey(t->pages[pg]->fence, lc, lower_len);
    *base = l->base->buf;
    *base_len = l->base->len;
    deltaList dl = {0};
    leafDeltaCollect(l->delta, e->snap.view->flushed_through, &dl);
    *delta = NULL;
    *delta_len = 0;
    if (dl.n) {
        slabSegWriter w;
        slabSegWriterInit(&w);
        for (size_t k = 0; k < dl.n; k++)
            slabSegWriterAdd(&w, dl.recs[k].key, dl.recs[k].klen, dl.recs[k].val, dl.recs[k].vlen, dl.recs[k].val == NULL);
        *delta = slabSegWriterFinish(&w, delta_len);
    }
    zfree(dl.recs);
}

/* The next pending entry across memtables, newest wins (tombstones included). */
static bool pendingNext(memLayer *mem, uint32_t nmem, const slabVlogRec **out) {
    const slabVlogRec *best = NULL;
    for (uint32_t i = 0; i < nmem; i++) {
        if (mem[i].pos >= mem[i].run->n) continue;
        const slabVlogRec *r = mem[i].run->recs[mem[i].pos];
        if (!best || slabKeyCmp(r->data, r->klen, best->data, best->klen) < 0) best = r;
    }
    if (!best) return false;
    const slabVlogRec *win = NULL;
    for (uint32_t i = 0; i < nmem; i++) {
        if (mem[i].pos >= mem[i].run->n) continue;
        const slabVlogRec *r = mem[i].run->recs[mem[i].pos];
        if (slabKeyCmp(r->data, r->klen, best->data, best->klen) != 0) continue;
        if (!win) win = r;
        mem[i].pos++;
    }
    *out = win;
    return true;
}

uint64_t slabExportPendingCount(slabExport *e) {
    if (!e->counted) {
        const slabVlogRec *r;
        uint64_t n = 0;
        while (pendingNext(e->mem, e->nmem, &r)) n++;
        for (uint32_t i = 0; i < e->nmem; i++) e->mem[i].pos = 0;
        e->pending = n;
        e->counted = true;
    }
    return e->pending;
}

bool slabExportPendingNext(slabExport *e, const unsigned char **key, size_t *klen, const unsigned char **val, size_t *vlen) {
    const slabVlogRec *r;
    if (!pendingNext(e->mem, e->nmem, &r)) return false;
    *key = r->data;
    *klen = r->klen;
    if (slabVlogRecIsTombstone(r)) {
        *val = NULL;
        *vlen = 0;
    } else {
        *val = r->data + r->klen;
        *vlen = r->vlen;
    }
    return true;
}

void slabExportEnd(slabExport *e) {
    for (uint32_t i = 0; i < e->nmem; i++)
        if (e->mem[i].owned) slabRunFree((slabRun *)e->mem[i].run);
    zfree(e->mem);
    zfree(e);
}

struct slabImport {
    slabColl *c;
    slabLeaf *leaves;
    unsigned char **lowers;
    size_t *lower_lens;
    size_t n, cap;
    unsigned char *last_key; /* the previous leaf's largest key */
    size_t last_len;
    bool has_last;
    slabMemtable *pending;
    unsigned char *pend_last;
    size_t pend_last_len;
    bool has_pend;
};

slabImport *slabImportBegin(const slabConfig *cfg) {
    slabImport *im = zcalloc(sizeof(*im));
    im->c = slabCollCreate(cfg);
    return im;
}

const char *slabImportLeaf(slabImport *im, const unsigned char *lower, size_t lower_len, unsigned char *base, size_t base_len, unsigned char *delta, size_t delta_len) {
    if (lower_len > SLAB_MAX_KEY_BYTES) return "leaf lower bound too long";
    if (im->n == 0 && lower_len != 0) return "the first leaf must have the empty lower bound";
    if (im->n > 0 && slabKeyCmp(im->lowers[im->n - 1], im->lower_lens[im->n - 1], lower, lower_len) >= 0)
        return "leaf lower bounds not strictly increasing";
    if (im->has_last && slabKeyCmp(im->last_key, im->last_len, lower, lower_len) >= 0)
        return "a key lies beyond its leaf";
    const char *err = NULL;
    slabBase *b = slabBaseLoad(base, base_len, &err);
    if (!b) return err;
    if (b->entries == 0) {
        b->buf = NULL; /* the caller keeps 'base' on failure */
        slabBaseFree(b);
        return "empty base block";
    }
    unsigned char *first, *last;
    size_t fl, ll;
    slabBaseKeyBounds(b, &first, &fl, &last, &ll);
    bool below = slabKeyCmp(first, fl, lower, lower_len) < 0;
    zfree(first);
    slabDeltaSeg *seg = NULL;
    if (!below && delta_len) {
        seg = slabDeltaSegLoad(delta, delta_len, &err);
        if (seg && seg->entries) {
            size_t pos = 0;
            const unsigned char *k, *v, *dfirst = NULL, *dlast = NULL;
            size_t kl, vl, dfl = 0, dll = 0;
            while (slabDeltaSegNext(seg, &pos, &k, &kl, &v, &vl)) {
                if (!dfirst) dfirst = k, dfl = kl;
                dlast = k, dll = kl;
            }
            if (slabKeyCmp(dfirst, dfl, lower, lower_len) < 0) {
                seg->buf = NULL;
                slabDeltaSegFree(seg);
                seg = NULL;
                err = "a delta key lies below its leaf";
            } else if (slabKeyCmp(dlast, dll, last, ll) > 0) {
                zfree(last);
                last = zmalloc(dll ? dll : 1);
                memcpy(last, dlast, dll);
                ll = dll;
            }
        }
        if (!seg) {
            b->buf = NULL;
            slabBaseFree(b);
            zfree(last);
            return err ? err : "invalid delta segment";
        }
    }
    if (below) {
        b->buf = NULL;
        slabBaseFree(b);
        zfree(last);
        return "a key lies below its leaf";
    }
    if (delta_len == 0 && delta) zfree(delta);
    if (im->n == im->cap) {
        im->cap = im->cap ? im->cap * 2 : 16;
        im->leaves = zrealloc(im->leaves, im->cap * sizeof(slabLeaf));
        im->lowers = zrealloc(im->lowers, im->cap * sizeof(unsigned char *));
        im->lower_lens = zrealloc(im->lower_lens, im->cap * sizeof(size_t));
    }
    im->leaves[im->n] = (slabLeaf){b, leafDeltaCreate(im->c->id, seg), seg != NULL};
    im->lowers[im->n] = zmalloc(lower_len ? lower_len : 1);
    memcpy(im->lowers[im->n], lower, lower_len);
    im->lower_lens[im->n] = lower_len;
    im->n++;
    zfree(im->last_key);
    im->last_key = last;
    im->last_len = ll;
    im->has_last = true;
    return NULL;
}

const char *slabImportPending(slabImport *im, const unsigned char *key, size_t klen, const unsigned char *val, size_t vlen, bool tombstone) {
    if (klen > SLAB_MAX_KEY_BYTES || (!tombstone && vlen > SLAB_MAX_VALUE_BYTES)) return "pending entry too large";
    if (im->has_pend && slabKeyCmp(im->pend_last, im->pend_last_len, key, klen) >= 0)
        return "pending keys not strictly increasing";
    if (!im->pending) {
        im->pending = slabMemtableCreate(1, &im->c->view->config, 0, false);
        slabVlogBegin(im->pending->log, 1, 0);
    }
    slabVlogAppend(im->pending->log, key, klen, slabHash(key, klen), val, vlen, tombstone);
    im->pend_last = zrealloc(im->pend_last, klen ? klen : 1);
    memcpy(im->pend_last, key, klen);
    im->pend_last_len = klen;
    im->has_pend = true;
    return NULL;
}

slabColl *slabImportFinish(slabImport *im, const char **err) {
    slabColl *c = im->c;
    *err = NULL;
    slabTree *t = im->n ? slabTreeFromLeaves(c->id, im->n, im->leaves, im->lowers, im->lower_lens) : slabTreeEmpty(c->id);
    for (size_t i = 0; i < im->n; i++) zfree(im->lowers[i]);
    zfree(im->leaves);
    zfree(im->lowers);
    zfree(im->lower_lens);
    zfree(im->last_key);
    zfree(im->pend_last);
    slabView *old = c->view;
    slabView *v = slabViewCreate(im->pending ? 1 : 0);
    v->active = old->active;
    v->tree = t;
    v->config = old->config;
    v->generation = 1;
    v->flushed_through = 0;
    for (uint64_t pos = 0; pos < t->nleaves; pos++) {
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
        slabDeltaInfo d;
        slabLeafDeltaCounters(l->delta, &d);
        deltaInfoAdd(&v->delta, &d, 1);
    }
    if (im->pending) {
        slabVlogCommit(im->pending->log);
        atomic_store_explicit(&im->pending->published_seq, 1, memory_order_relaxed);
        v->frozen[0] = im->pending;
        c->frozen_count = 1;
        im->pending->charged_bytes = slabMemtableBytes(im->pending) + 8 * slabVlogEntries(im->pending->log);
        c->frozen_bytes = im->pending->charged_bytes;
        c->next_memtable_id = 2;
        c->next_seq = 2;
    }
    c->view = v;
    slabViewRelease(old);
    builderStart(c, v);
    c->maint.scan_leaves = true;
    zfree(im);
    slabMaintNotify(c);
    return c;
}

void slabImportAbort(slabImport *im) {
    for (size_t i = 0; i < im->n; i++) {
        leafRelease(&im->leaves[i]);
        zfree(im->lowers[i]);
    }
    zfree(im->leaves);
    zfree(im->lowers);
    zfree(im->lower_lens);
    zfree(im->last_key);
    zfree(im->pend_last);
    if (im->pending) {
        slabVlogAbort(im->pending->log);
        slabMemtableRelease(im->pending);
    }
    slabCollDetach(im->c);
    zfree(im);
}

/* ---------------------------------------------------------------------------------------
 * Verification
 * ------------------------------------------------------------------------------------- */

static char *verr(const char *msg, uint64_t pos) {
    char buf[160];
    snprintf(buf, sizeof(buf), "%s (leaf %llu)", msg, (unsigned long long)pos);
    size_t n = strlen(buf) + 1;
    char *s = zmalloc(n);
    memcpy(s, buf, n);
    return s;
}

char *slabVerify(const slabSnapshot *s) {
    const slabView *v = s->view;
    const slabTree *t = v->tree;
    /* Frozen ids: exactly flushed_through + 1 .. in order, newest first. */
    for (uint32_t i = 0; i < v->nfrozen; i++)
        if (v->frozen[i]->id != v->flushed_through + (v->nfrozen - i)) return verr("frozen memtable ids not contiguous", i);
    for (uint32_t p = 0; p < t->npages; p++) {
        const slabPage *pg = t->pages[p];
        if (pg->n == 0 || pg->n > SLAB_PAGE_MAX) return verr("bad page size", t->first[p]);
    }
    for (uint64_t pos = 0; pos < t->nleaves; pos++) {
        uint32_t pg, lc;
        slabLeaf *l = slabTreeLeaf(t, pos, &pg, &lc);
        size_t lkl, ukl = 0;
        const unsigned char *lk = slabFenceKey(t->pages[pg]->fence, lc, &lkl), *uk = NULL;
        if (pos == 0 && lkl != 0) return verr("first lower bound not empty", pos);
        if (pos + 1 < t->nleaves) {
            uint32_t npg, nlc;
            slabTreeLeaf(t, pos + 1, &npg, &nlc);
            uk = slabFenceKey(t->pages[npg]->fence, nlc, &ukl);
            if (slabKeyCmp(lk, lkl, uk, ukl) >= 0) return verr("lower bounds not increasing", pos);
        }
        if (l->base->entries) {
            unsigned char *first, *last;
            size_t fl, ll;
            slabBaseKeyBounds(l->base, &first, &fl, &last, &ll);
            bool bad = slabKeyCmp(first, fl, lk, lkl) < 0 || (uk && slabKeyCmp(last, ll, uk, ukl) >= 0);
            zfree(first);
            zfree(last);
            if (bad) return verr("base key outside its leaf", pos);
        }
        deltaList dl = {0};
        leafDeltaCollect(l->delta, v->flushed_through, &dl);
        for (size_t i = 0; i < dl.n; i++) {
            if (slabKeyCmp(dl.recs[i].key, dl.recs[i].klen, lk, lkl) < 0 ||
                (uk && slabKeyCmp(dl.recs[i].key, dl.recs[i].klen, uk, ukl) >= 0)) {
                zfree(dl.recs);
                return verr("delta key outside its leaf", pos);
            }
        }
        zfree(dl.recs);
    }
    return NULL;
}
