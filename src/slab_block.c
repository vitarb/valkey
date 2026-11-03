/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Base blocks, their cursor and builder, and persisted delta segments. See
 * slab_block.h for the formats. */

#include "slab_block.h"
#include "slab_hash.h"
#include "serverassert.h"
#include "zmalloc.h"

#include <string.h>

#define SLAB_BASE_MAGIC 0x31424c53U /* "SLB1" */
#define SLAB_SEG_MAGIC 0x31444c53U  /* "SLD1" */
#define SLAB_TOMBSTONE UINT32_MAX
/* Blocks with at most this many restart groups are sought by comparing the restart keys
 * directly: the rpfx array would cost a cache miss and save nothing. */
#define SLAB_SEEK_DIRECT_MAX 4
/* A gallop that lands at most this many entries ahead is walked instead. */
#define SLAB_NEAR_ENTRIES 8

uint64_t slab_hash_seed;

/* ---------------------------------------------------------------------------------------
 * Little-endian fields and varints
 * ------------------------------------------------------------------------------------- */

static inline uint32_t rd32(const unsigned char *p) {
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

static inline uint64_t rd64(const unsigned char *p) {
    return (uint64_t)rd32(p) | (uint64_t)rd32(p + 4) << 32;
}

static inline void wr32(unsigned char *p, uint32_t v) {
    p[0] = v;
    p[1] = v >> 8;
    p[2] = v >> 16;
    p[3] = v >> 24;
}

static inline void wr64(unsigned char *p, uint64_t v) {
    wr32(p, (uint32_t)v);
    wr32(p + 4, (uint32_t)(v >> 32));
}

size_t slabVarintLen(uint32_t v) {
    if (v < (1U << 7)) return 1;
    if (v < (1U << 14)) return 2;
    if (v < (1U << 21)) return 3;
    if (v < (1U << 28)) return 4;
    return 5;
}

static inline size_t varintPut(unsigned char *p, uint32_t v) {
    size_t n = 0;
    while (v >= 0x80) {
        p[n++] = (unsigned char)(v | 0x80);
        v >>= 7;
    }
    p[n++] = (unsigned char)v;
    return n;
}

/* Checked, canonical decode: at most 5 bytes, no value above UINT32_MAX and no
 * redundant trailing zero group. Used by validation only. */
static bool varintGetChecked(const unsigned char *p, size_t len, size_t *off, uint32_t *out) {
    uint64_t v = 0;
    for (int i = 0; i < 5; i++) {
        if (*off >= len) return false;
        unsigned char byte = p[(*off)++];
        v |= (uint64_t)(byte & 0x7f) << (7 * i);
        if (!(byte & 0x80)) {
            if (i > 0 && byte == 0) return false;
            if (v > UINT32_MAX) return false;
            *out = (uint32_t)v;
            return true;
        }
    }
    return false;
}

/* Unchecked decode for validated blocks. */
static inline uint32_t varintGet(const unsigned char *p, uint32_t *off) {
    uint32_t v = p[*off];
    (*off)++;
    if (v < 0x80) return v;
    v &= 0x7f;
    int shift = 7;
    for (;;) {
        uint32_t byte = p[(*off)++];
        v |= (byte & 0x7f) << shift;
        if (byte < 0x80) return v;
        shift += 7;
    }
}

static inline uint64_t align4(uint64_t v) {
    return (v + 3) & ~(uint64_t)3;
}

/* Bytes a and b share at their start. Compares 8 bytes at a time. */
static inline size_t commonPrefix(const unsigned char *a, size_t alen, const unsigned char *b, size_t blen) {
    size_t n = alen < blen ? alen : blen, i = 0;
    while (i + 8 <= n) {
        uint64_t x, y;
        memcpy(&x, a + i, 8);
        memcpy(&y, b + i, 8);
        if (x != y) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
            return i + (__builtin_clzll(x ^ y) >> 3);
#else
            return i + (__builtin_ctzll(x ^ y) >> 3);
#endif
        }
        i += 8;
    }
    while (i < n && a[i] == b[i]) i++;
    return i;
}

/* Compares a with b; *common receives the bytes they share. */
static inline int compareWithCommon(const unsigned char *a, size_t alen, const unsigned char *b, size_t blen, size_t *common) {
    size_t m = commonPrefix(a, alen, b, blen);
    *common = m;
    if (m < alen && m < blen) return a[m] < b[m] ? -1 : 1;
    return (alen > m) - (blen > m);
}

/* ---------------------------------------------------------------------------------------
 * Base block: validation
 * ------------------------------------------------------------------------------------- */

#define ERR(msg)      \
    do {              \
        *err = (msg); \
        goto fail;    \
    } while (0)

slabBase *slabBaseLoad(unsigned char *buf, size_t len, const char **err) {
    slabBase *b = NULL;
    unsigned char *key = NULL;
    if (len < SLAB_BASE_HEADER_BYTES || len > SLAB_MAX_BLOCK_BYTES) {
        *err = "base block length out of range";
        return NULL;
    }
    if (rd32(buf) != SLAB_BASE_MAGIC) {
        *err = "bad base block magic";
        return NULL;
    }
    uint32_t entries = rd32(buf + 4);
    uint32_t kr = (uint32_t)buf[8] | (uint32_t)buf[9] << 8;
    uint32_t vr = (uint32_t)buf[10] | (uint32_t)buf[11] << 8;
    uint32_t prefix_len = rd32(buf + 12), kdat_len = rd32(buf + 16), vblb_len = rd32(buf + 20),
             vlen_len = rd32(buf + 24);
    if (rd32(buf + 28) != 0 || rd64(buf + 32) != len) {
        *err = "bad base block header";
        return NULL;
    }
    if (kr < 1 || kr > SLAB_MAX_RESTART || vr < 1 || vr > SLAB_MAX_RESTART) {
        *err = "bad restart interval";
        return NULL;
    }
    if (prefix_len > SLAB_MAX_KEY_BYTES || (entries == 0 && (prefix_len || kdat_len || vblb_len || vlen_len))) {
        *err = "bad block prefix";
        return NULL;
    }
    uint64_t restarts = entries ? ((uint64_t)entries + kr - 1) / kr : 0;
    uint64_t vgroups = entries ? ((uint64_t)entries + vr - 1) / vr : 0;
    uint64_t off = SLAB_BASE_HEADER_BYTES;
    uint64_t prefix_off = off;
    off += prefix_len;
    uint64_t kdat_off = off;
    off += kdat_len;
    uint64_t kdat_end = off;
    off = align4(off);
    uint64_t krst_off = off;
    off += restarts * 4;
    uint64_t vblb_off = off;
    off += vblb_len;
    uint64_t vlen_off = off;
    off += vlen_len;
    uint64_t vlen_end = off;
    off = align4(off);
    uint64_t vors_off = off;
    off += vgroups * 4;
    if (off != len) {
        *err = "base block sections do not tile the block";
        return NULL;
    }
    for (uint64_t i = kdat_end; i < krst_off; i++)
        if (buf[i]) {
            *err = "nonzero base block padding";
            return NULL;
        }
    for (uint64_t i = vlen_end; i < vors_off; i++)
        if (buf[i]) {
            *err = "nonzero base block padding";
            return NULL;
        }

    /* One allocation for the struct and its derived arrays; rpfx first, 8-aligned. */
    size_t struct_bytes = (sizeof(slabBase) + 7) & ~(size_t)7;
    size_t usable;
    b = zmalloc_usable(struct_bytes + restarts * 8 + vgroups * 4, &usable);
    memset(b, 0, sizeof(*b));
    atomic_init(&b->refcount, 1);
    b->rpfx = (uint64_t *)((char *)b + struct_bytes);
    b->vlen_group_off = (uint32_t *)((char *)b->rpfx + restarts * 8);
    b->buf = buf;
    b->len = len;
    b->entries = entries;
    b->key_restart = kr;
    b->value_restart = vr;
    b->restarts = (uint32_t)restarts;
    b->vgroups = (uint32_t)vgroups;
    b->prefix_len = prefix_len;
    b->kdat_len = kdat_len;
    b->vblb_len = vblb_len;
    b->vlen_len = vlen_len;
    b->prefix = buf + prefix_off;
    b->kdat = buf + kdat_off;
    b->krst = buf + krst_off;
    b->vblb = buf + vblb_off;
    b->vlen = buf + vlen_off;
    b->vors = buf + vors_off;

    /* The key stream: restart offsets, maximal shared lengths, strictly increasing keys. */
    size_t max_suffix = SLAB_MAX_KEY_BYTES - prefix_len;
    key = zmalloc(max_suffix ? max_suffix : 1);
    size_t key_len = 0, koff = 0;
    for (uint64_t g = 0; g < restarts; g++) {
        if (rd32(b->krst + g * 4) != koff) ERR("restart table does not match the keys");
        uint32_t slen;
        if (!varintGetChecked(b->kdat, kdat_len, &koff, &slen)) ERR("bad key varint");
        if (slen > max_suffix || slen > kdat_len - koff) ERR("bad key length");
        const unsigned char *suffix = b->kdat + koff;
        if (g > 0 && slabKeyCmp(suffix, slen, key, key_len) <= 0) ERR("keys not strictly increasing");
        memcpy(key, suffix, slen);
        key_len = slen;
        koff += slen;
        b->rpfx[g] = slabKeyHead(key, key_len);
        uint64_t in_group = entries - g * kr < kr ? entries - g * kr : kr;
        for (uint64_t i = 1; i < in_group; i++) {
            uint32_t shared, unshared;
            if (!varintGetChecked(b->kdat, kdat_len, &koff, &shared) ||
                !varintGetChecked(b->kdat, kdat_len, &koff, &unshared))
                ERR("bad key varint");
            if (unshared > kdat_len - koff) ERR("bad key length");
            const unsigned char *tail = b->kdat + koff;
            /* Maximal shared length and strict increase in one test: the first unshared
             * byte must exist and exceed the previous key's byte at that position, or the
             * previous key must end exactly at the shared length. */
            if (shared < key_len) {
                if (unshared == 0 || tail[0] <= key[shared]) ERR("keys not strictly increasing");
            } else if (shared == key_len) {
                if (unshared == 0) ERR("keys not strictly increasing");
            } else {
                ERR("bad shared key length");
            }
            if ((uint64_t)shared + unshared > max_suffix) ERR("bad key length");
            memcpy(key + shared, tail, unshared);
            key_len = shared + unshared;
            koff += unshared;
        }
    }
    if (koff != kdat_len) ERR("trailing key bytes");

    /* Values: one length per entry, group offsets matching VORS, lengths tiling VBLB. */
    size_t voff = 0;
    uint64_t blob = 0;
    for (uint64_t i = 0; i < entries; i++) {
        if (i % vr == 0) {
            uint64_t g = i / vr;
            if (rd32(b->vors + g * 4) != blob) ERR("value restart table does not match the values");
            b->vlen_group_off[g] = (uint32_t)voff;
        }
        uint32_t vl;
        if (!varintGetChecked(b->vlen, vlen_len, &voff, &vl)) ERR("bad value varint");
        if (vl > SLAB_MAX_VALUE_BYTES) ERR("value too long");
        blob += vl;
        if (blob > vblb_len) ERR("value lengths exceed the value blob");
    }
    if (voff != vlen_len || blob != vblb_len) ERR("value lengths do not match the value blob");

    zfree(key);
    b->alloc_bytes = usable + zmalloc_size(buf);
    return b;

fail:
    zfree(key);
    zfree(b);
    return NULL;
}

#undef ERR

slabBase *slabBaseMove(const slabBase *b) {
    size_t struct_bytes = (sizeof(slabBase) + 7) & ~(size_t)7;
    size_t total = struct_bytes + (size_t)b->restarts * 8 + (size_t)b->vgroups * 4;
    slabBase *n = zmalloc(total);
    unsigned char *buf = zmalloc(b->len);
    memcpy(n, b, total);
    memcpy(buf, b->buf, b->len);
    atomic_init(&n->refcount, 1);
    ptrdiff_t d = buf - b->buf;
    n->buf = buf;
    n->prefix = b->prefix + d;
    n->kdat = b->kdat + d;
    n->krst = b->krst + d;
    n->vblb = b->vblb + d;
    n->vlen = b->vlen + d;
    n->vors = b->vors + d;
    n->rpfx = (uint64_t *)((char *)n + struct_bytes);
    n->vlen_group_off = (uint32_t *)((char *)n->rpfx + (size_t)b->restarts * 8);
    n->alloc_bytes = zmalloc_size(n) + zmalloc_size(buf);
    return n;
}

void slabBaseFree(slabBase *b) {
    if (!b) return;
    zfree((void *)b->buf);
    zfree(b);
}

void slabBasePrefetch(const slabBase *b) {
    __builtin_prefetch(b->prefix, 0, 3);
    __builtin_prefetch(b->kdat, 0, 3);
    __builtin_prefetch(b->vlen, 0, 3);
}

/* ---------------------------------------------------------------------------------------
 * Base block: seek and cursor
 * ------------------------------------------------------------------------------------- */

static inline const unsigned char *restartSuffix(const slabBase *b, uint32_t g, uint32_t *len) {
    uint32_t off = rd32(b->krst + (size_t)g * 4);
    *len = varintGet(b->kdat, &off);
    return b->kdat + off;
}

/* Whether restart key g is <= the suffix. */
static inline bool restartLe(const slabBase *b, uint32_t g, const unsigned char *s, size_t slen, uint64_t head) {
    uint64_t h = b->rpfx[g];
    if (h != head) return h < head;
    uint32_t rlen;
    const unsigned char *rk = restartSuffix(b, g, &rlen);
    return slabKeyCmp(rk, rlen, s, slen) <= 0;
}

/* The restart group a suffix belongs to: the last group whose restart key is <= it
 * (group 0 if every restart key is greater). */
static uint32_t baseSeek(const slabBase *b, const unsigned char *s, size_t slen) {
    uint32_t restarts = b->restarts;
    if (restarts <= SLAB_SEEK_DIRECT_MAX) {
        uint32_t g = 0;
        while (g + 1 < restarts) {
            uint32_t rlen;
            const unsigned char *rk = restartSuffix(b, g + 1, &rlen);
            if (slabKeyCmp(rk, rlen, s, slen) > 0) break;
            g++;
        }
        return g;
    }
    uint64_t head = slabKeyHead(s, slen);
    /* Integer search over the heads: [lo, hi) is the run of heads equal to the target's,
     * the only groups that need a full comparison. */
    uint32_t lo = 0, hi = restarts;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (b->rpfx[mid] < head)
            lo = mid + 1;
        else
            hi = mid;
    }
    uint32_t eq_lo = lo;
    hi = restarts;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (b->rpfx[mid] <= head)
            lo = mid + 1;
        else
            hi = mid;
    }
    hi = lo;
    lo = eq_lo;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (restartLe(b, mid, s, slen, head))
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo ? lo - 1 : 0;
}

void slabBaseCursorInit(slabBaseCursor *c, const slabBase *b) {
    memset(c, 0, sizeof(*c));
    c->b = b;
}

static inline uint32_t groupEnd(const slabBaseCursor *c) {
    uint64_t end = ((uint64_t)c->group + 1) * c->b->key_restart;
    return end < c->b->entries ? (uint32_t)end : c->b->entries;
}

/* Positions the cursor on the restart entry of group g and compares it with target. */
static void loadRestart(slabBaseCursor *c, uint32_t g, const unsigned char *t, size_t tlen) {
    const slabBase *b = c->b;
    c->group = g;
    c->index = g * b->key_restart;
    uint32_t off = g == 0 ? 0 : rd32(b->krst + (size_t)g * 4);
    uint32_t len = varintGet(b->kdat, &off);
    c->shared = 0;
    c->tail = b->kdat + off;
    c->tail_len = len;
    c->next_kdat = off + len;
    uint32_t vg = c->index / b->value_restart;
    uint32_t voff = 0, val_off = 0;
    if (vg) {
        voff = b->vlen_group_off[vg];
        val_off = rd32(b->vors + (size_t)vg * 4);
    }
    for (uint32_t i = vg * b->value_restart; i < c->index; i++) val_off += varintGet(b->vlen, &voff);
    c->val_off = val_off;
    c->val_len = varintGet(b->vlen, &voff);
    c->next_vlen = voff;
    size_t common;
    c->order = compareWithCommon(c->tail, len, t, tlen, &common);
    c->common = (uint32_t)common;
}

/* Steps from the last entry of a group to the next group's restart entry. */
static void crossGroup(slabBaseCursor *c, const unsigned char *t, size_t tlen) {
    const slabBase *b = c->b;
    uint32_t voff = c->next_vlen;
    c->val_off += c->val_len;
    c->val_len = varintGet(b->vlen, &voff);
    c->next_vlen = voff;
    uint32_t off = c->next_kdat;
    uint32_t len = varintGet(b->kdat, &off);
    c->index++;
    c->group++;
    c->shared = 0;
    c->tail = b->kdat + off;
    c->tail_len = len;
    c->next_kdat = off + len;
    size_t common;
    c->order = compareWithCommon(c->tail, len, t, tlen, &common);
    c->common = (uint32_t)common;
}

/* Steps to the next entry inside a group, comparing in encoded form: the entry shares
 * 'shared' bytes with its predecessor, which shared 'common' bytes with the target. */
static void advance(slabBaseCursor *c, const unsigned char *t, size_t tlen) {
    const slabBase *b = c->b;
    uint32_t voff = c->next_vlen;
    c->val_off += c->val_len;
    c->val_len = varintGet(b->vlen, &voff);
    c->next_vlen = voff;
    uint32_t off = c->next_kdat;
    uint32_t shared = varintGet(b->kdat, &off);
    uint32_t unshared = varintGet(b->kdat, &off);
    c->index++;
    c->tail = b->kdat + off;
    c->tail_len = unshared;
    c->next_kdat = off + unshared;
    if (shared > c->common) {
        /* Same bytes as the predecessor where it was below the target. */
        c->order = -1;
    } else if (shared < c->common) {
        /* The predecessor matched the target further; this key's byte at 'shared' is
         * greater than the predecessor's, which equals the target's. */
        c->order = 1;
        c->common = shared;
    } else {
        size_t matched;
        c->order = compareWithCommon(c->tail, unshared, t + shared, tlen - shared, &matched);
        c->common = shared + (uint32_t)matched;
    }
    c->shared = shared;
}

/* Re-relates the current entry, known relative to the last target, to a new target that
 * is >= the last one and shares c bytes with it. */
static void relate(slabBaseCursor *cur, const unsigned char *t, size_t tlen, size_t c) {
    uint32_t m = cur->common;
    if (cur->order < 0) {
        cur->common = (uint32_t)(c < m ? c : m);
        return;
    }
    if (c < m) {
        cur->order = -1;
        cur->common = (uint32_t)c;
    } else if (c > m) {
        /* The entry and the target first differ where the entry and the last target did. */
    } else {
        /* An entry at or above the last target shares at least its own shared bytes with
         * it, so the comparison can resume inside its tail. */
        assert(m >= cur->shared);
        size_t matched;
        cur->order = compareWithCommon(cur->tail + (m - cur->shared), cur->tail_len - (m - cur->shared), t + m, tlen - m, &matched);
        cur->common = m + (uint32_t)matched;
    }
}

/* The last restart group <= target, galloping forward from 'from'. */
static uint32_t gallop(const slabBaseCursor *c, uint32_t from, const unsigned char *t, size_t tlen) {
    const slabBase *b = c->b;
    uint32_t restarts = b->restarts;
    uint64_t head = slabKeyHead(t, tlen);
    uint32_t lo = from, hi, step = 1;
    for (;;) {
        uint64_t probe = (uint64_t)lo + step;
        if (probe >= restarts) {
            hi = restarts;
            break;
        }
        if (!restartLe(b, (uint32_t)probe, t, tlen, head)) {
            hi = (uint32_t)probe;
            break;
        }
        lo = (uint32_t)probe;
        step = step < (1U << 30) ? step * 2 : step;
    }
    while (hi - lo > 1) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (restartLe(b, mid, t, tlen, head))
            lo = mid;
        else
            hi = mid;
    }
    return lo;
}

bool slabBaseCursorGet(slabBaseCursor *c, const unsigned char *t, size_t tlen, const unsigned char **val, size_t *vlen) {
    const slabBase *b = c->b;
    if (b->entries == 0) return false;
    bool limited;
    uint32_t limit = 0;
    if (c->positioned) {
        size_t cp = commonPrefix(c->last, c->last_len, t, tlen);
        bool forward = cp == c->last_len || (cp < tlen && t[cp] > c->last[cp]);
        if (forward) {
            relate(c, t, tlen, cp);
            limited = false;
        } else {
            limit = baseSeek(b, t, tlen);
            loadRestart(c, limit, t, tlen);
            limited = true;
        }
    } else {
        limit = baseSeek(b, t, tlen);
        loadRestart(c, limit, t, tlen);
        limited = true;
    }
    uint32_t group_end = groupEnd(c);
    uint32_t steps = 0;
    while (c->order < 0) {
        if (!limited && steps > 0) {
            /* The target is past the next entry: find its group before walking on. */
            limit = gallop(c, c->group, t, tlen);
            limited = true;
            if ((uint64_t)(limit - c->group) * b->key_restart > SLAB_NEAR_ENTRIES) {
                loadRestart(c, limit, t, tlen);
                group_end = groupEnd(c);
                continue;
            }
        }
        if (c->index + 1 >= group_end) {
            if ((limited && c->group >= limit) || group_end >= b->entries) break;
            crossGroup(c, t, tlen);
            group_end = groupEnd(c);
            steps++;
            continue;
        }
        advance(c, t, tlen);
        steps++;
    }
    c->positioned = true;
    c->last = t;
    c->last_len = tlen;
    if (c->order != 0) return false;
    *val = b->vblb + c->val_off;
    *vlen = c->val_len;
    return true;
}

bool slabBaseGet(const slabBase *b, const unsigned char *key, size_t klen, const unsigned char **val, size_t *vlen) {
    if (b->entries == 0 || klen < b->prefix_len || memcmp(key, b->prefix, b->prefix_len) != 0) return false;
    slabBaseCursor c;
    slabBaseCursorInit(&c, b);
    return slabBaseCursorGet(&c, key + b->prefix_len, klen - b->prefix_len, val, vlen);
}

/* ---------------------------------------------------------------------------------------
 * Base block: iteration
 * ------------------------------------------------------------------------------------- */

static void iterReserve(slabBaseIter *it, size_t need) {
    if (need <= it->key_cap) return;
    size_t cap = it->key_cap ? it->key_cap : 64;
    while (cap < need) cap *= 2;
    it->key = zrealloc(it->key, cap);
    it->key_cap = cap;
}

void slabBaseIterInit(slabBaseIter *it, const slabBase *b) {
    memset(it, 0, sizeof(*it));
    it->b = b;
    iterReserve(it, b->prefix_len + 64);
    memcpy(it->key, b->prefix, b->prefix_len);
    it->key_len = b->prefix_len;
}

void slabBaseIterRelease(slabBaseIter *it) {
    zfree(it->key);
    it->key = NULL;
}

/* Decodes entry it->index (whose KDAT and VLEN offsets the iterator holds) into the key
 * buffer and returns its value. */
static void iterDecode(slabBaseIter *it, const unsigned char **val, size_t *vlen) {
    const slabBase *b = it->b;
    size_t plen = b->prefix_len;
    if (it->index % b->key_restart == 0) {
        uint32_t len = varintGet(b->kdat, &it->kdat_off);
        iterReserve(it, plen + len);
        memcpy(it->key + plen, b->kdat + it->kdat_off, len);
        it->key_len = plen + len;
        it->kdat_off += len;
    } else {
        uint32_t shared = varintGet(b->kdat, &it->kdat_off);
        uint32_t unshared = varintGet(b->kdat, &it->kdat_off);
        iterReserve(it, plen + shared + unshared);
        memcpy(it->key + plen + shared, b->kdat + it->kdat_off, unshared);
        it->key_len = plen + shared + unshared;
        it->kdat_off += unshared;
    }
    uint32_t len = varintGet(b->vlen, &it->vlen_off);
    *val = b->vblb + it->vblb_off;
    *vlen = len;
    it->vblb_off += len;
}

bool slabBaseIterNext(slabBaseIter *it, const unsigned char **key, size_t *klen, const unsigned char **val, size_t *vlen) {
    if (it->pending) {
        it->pending = false;
        *val = it->pending_val;
        *vlen = it->pending_vlen;
    } else {
        if (it->index >= it->b->entries) return false;
        iterDecode(it, val, vlen);
        it->index++;
    }
    *key = it->key;
    *klen = it->key_len;
    return true;
}

/* Moves the iterator to the start of restart group g. */
static void iterToGroup(slabBaseIter *it, uint32_t g) {
    const slabBase *b = it->b;
    it->index = g * b->key_restart;
    it->kdat_off = g == 0 ? 0 : rd32(b->krst + (size_t)g * 4);
    uint32_t vg = it->index / b->value_restart;
    uint32_t voff = 0, val_off = 0;
    if (vg) {
        voff = b->vlen_group_off[vg];
        val_off = rd32(b->vors + (size_t)vg * 4);
    }
    for (uint32_t i = vg * b->value_restart; i < it->index; i++) val_off += varintGet(b->vlen, &voff);
    it->vlen_off = voff;
    it->vblb_off = val_off;
}

void slabBaseIterSeek(slabBaseIter *it, const unsigned char *key, size_t klen) {
    it->pending = false;
    const slabBase *b = it->b;
    if (b->entries == 0) return;
    size_t plen = b->prefix_len;
    size_t cp = commonPrefix(key, klen, b->prefix, plen);
    if (cp < plen) {
        if (cp < klen && key[cp] > b->prefix[cp]) {
            it->index = b->entries; /* every key of the block is smaller */
        } else {
            iterToGroup(it, 0); /* every key of the block is greater */
        }
        return;
    }
    const unsigned char *s = key + plen;
    size_t slen = klen - plen;
    iterToGroup(it, baseSeek(b, s, slen));
    /* Walk the group to the first entry >= key and keep it for the next call. */
    while (it->index < b->entries) {
        iterDecode(it, &it->pending_val, &it->pending_vlen);
        it->index++;
        if (slabKeyCmp(it->key + plen, it->key_len - plen, s, slen) >= 0) {
            it->pending = true;
            return;
        }
    }
}

void slabBaseKeyBounds(const slabBase *b, unsigned char **first, size_t *first_len, unsigned char **last, size_t *last_len) {
    assert(b->entries > 0);
    uint32_t rlen;
    const unsigned char *rk = restartSuffix(b, 0, &rlen);
    *first_len = b->prefix_len + rlen;
    *first = zmalloc(*first_len ? *first_len : 1);
    memcpy(*first, b->prefix, b->prefix_len);
    memcpy(*first + b->prefix_len, rk, rlen);
    slabBaseIter it;
    slabBaseIterInit(&it, b);
    iterToGroup(&it, b->restarts - 1);
    const unsigned char *k, *v;
    size_t kl, vl;
    while (slabBaseIterNext(&it, &k, &kl, &v, &vl)) {
    }
    *last_len = it.key_len;
    *last = zmalloc(*last_len ? *last_len : 1);
    memcpy(*last, it.key, it.key_len);
    slabBaseIterRelease(&it);
}

/* ---------------------------------------------------------------------------------------
 * Base block: builder
 * ------------------------------------------------------------------------------------- */

static uint32_t clampRestart(uint32_t v) {
    return v < 1 ? 1 : v > SLAB_MAX_RESTART ? SLAB_MAX_RESTART
                                            : v;
}

void slabBaseBuilderInit(slabBaseBuilder *bb, uint32_t key_restart, uint32_t value_restart) {
    memset(bb, 0, sizeof(*bb));
    bb->key_restart = clampRestart(key_restart);
    bb->value_restart = clampRestart(value_restart);
    bb->key_off_cap = 64;
    bb->key_off = zmalloc(bb->key_off_cap * sizeof(uint32_t));
    bb->val_off = zmalloc(bb->key_off_cap * sizeof(uint32_t));
    bb->key_off[0] = bb->val_off[0] = 0;
}

void slabBaseBuilderReset(slabBaseBuilder *bb) {
    bb->entries = 0;
    bb->keys_len = bb->vals_len = 0;
    bb->prefix = bb->key_bytes = bb->restart_key_bytes = bb->key_restarts = 0;
    bb->value_bytes = bb->value_restarts = 0;
}

void slabBaseBuilderRelease(slabBaseBuilder *bb) {
    zfree(bb->keys);
    zfree(bb->vals);
    zfree(bb->key_off);
    zfree(bb->val_off);
    memset(bb, 0, sizeof(*bb));
}

static void growBytes(unsigned char **p, size_t *cap, size_t need) {
    if (need <= *cap) return;
    size_t c = *cap ? *cap : 256;
    while (c < need) c *= 2;
    *p = zrealloc(*p, c);
    *cap = c;
}

size_t slabBaseBuilderShared(const slabBaseBuilder *bb, const unsigned char *key, size_t klen) {
    if (bb->entries == 0) return 0;
    const unsigned char *prev = bb->keys + bb->key_off[bb->entries - 1];
    size_t plen = bb->key_off[bb->entries] - bb->key_off[bb->entries - 1];
    return commonPrefix(prev, plen, key, klen);
}

static size_t layoutBytes(size_t prefix, size_t kdat, size_t key_restarts, size_t value_bytes, size_t value_restarts) {
    size_t raw = SLAB_BASE_HEADER_BYTES + prefix + kdat;
    raw = align4(raw) + 4 * key_restarts;
    raw += value_bytes;
    return align4(raw) + 4 * value_restarts;
}

size_t slabBaseBuilderBytesWith(const slabBaseBuilder *bb, size_t klen, size_t vlen, size_t shared) {
    size_t n = bb->entries;
    size_t prefix = n == 0 ? klen : (bb->prefix < shared ? bb->prefix : shared);
    size_t key_bytes = bb->key_bytes, restart_key_bytes = bb->restart_key_bytes;
    size_t key_restarts = bb->key_restarts, value_restarts = bb->value_restarts;
    if (n % bb->key_restart == 0) {
        key_restarts++;
        restart_key_bytes += klen;
        key_bytes += slabVarintLen((uint32_t)klen);
    } else {
        size_t unshared = klen - shared;
        key_bytes += slabVarintLen((uint32_t)shared) + slabVarintLen((uint32_t)unshared) + unshared;
    }
    if (n % bb->value_restart == 0) value_restarts++;
    size_t value_bytes = bb->value_bytes + slabVarintLen((uint32_t)vlen) + vlen;
    return layoutBytes(prefix, key_bytes + restart_key_bytes - key_restarts * prefix, key_restarts, value_bytes, value_restarts);
}

size_t slabBaseBuilderBytes(const slabBaseBuilder *bb) {
    if (bb->entries == 0) return SLAB_BASE_HEADER_BYTES;
    return layoutBytes(bb->prefix, bb->key_bytes + bb->restart_key_bytes - bb->key_restarts * bb->prefix,
                       bb->key_restarts, bb->value_bytes, bb->value_restarts);
}

void slabBaseBuilderAdd(slabBaseBuilder *bb, const unsigned char *key, size_t klen, const unsigned char *val, size_t vlen) {
    assert(klen <= SLAB_MAX_KEY_BYTES && vlen <= SLAB_MAX_VALUE_BYTES);
    size_t shared = slabBaseBuilderShared(bb, key, klen);
    size_t n = bb->entries;
    if (n > 0) {
        const unsigned char *prev = bb->keys + bb->key_off[n - 1];
        size_t plen = bb->key_off[n] - bb->key_off[n - 1];
        assert(slabKeyCmp(prev, plen, key, klen) < 0);
    }
    bb->prefix = n == 0 ? klen : (bb->prefix < shared ? bb->prefix : shared);
    if (n % bb->key_restart == 0) {
        bb->key_restarts++;
        bb->restart_key_bytes += klen;
        bb->key_bytes += slabVarintLen((uint32_t)klen);
    } else {
        size_t unshared = klen - shared;
        bb->key_bytes += slabVarintLen((uint32_t)shared) + slabVarintLen((uint32_t)unshared) + unshared;
    }
    if (n % bb->value_restart == 0) bb->value_restarts++;
    bb->value_bytes += slabVarintLen((uint32_t)vlen) + vlen;

    if (n + 2 > bb->key_off_cap) {
        bb->key_off_cap *= 2;
        bb->key_off = zrealloc(bb->key_off, bb->key_off_cap * sizeof(uint32_t));
        bb->val_off = zrealloc(bb->val_off, bb->key_off_cap * sizeof(uint32_t));
    }
    growBytes(&bb->keys, &bb->keys_cap, bb->keys_len + klen);
    if (klen) memcpy(bb->keys + bb->keys_len, key, klen);
    bb->keys_len += klen;
    growBytes(&bb->vals, &bb->vals_cap, bb->vals_len + vlen);
    if (vlen) memcpy(bb->vals + bb->vals_len, val, vlen);
    bb->vals_len += vlen;
    bb->entries++;
    bb->key_off[bb->entries] = (uint32_t)bb->keys_len;
    bb->val_off[bb->entries] = (uint32_t)bb->vals_len;
}

slabBase *slabBaseBuilderFinish(slabBaseBuilder *bb) {
    uint32_t n = bb->entries, kr = bb->key_restart, vr = bb->value_restart;
    size_t prefix = n ? bb->prefix : 0;
    /* Exact section sizes: the accounting above is an upper bound once the prefix is
     * known, so recount the key stream against the final prefix. */
    size_t kdat = 0;
    for (uint32_t i = 0; i < n; i++) {
        const unsigned char *k = bb->keys + bb->key_off[i];
        size_t klen = bb->key_off[i + 1] - bb->key_off[i];
        size_t slen = klen - prefix;
        if (i % kr == 0) {
            kdat += slabVarintLen((uint32_t)slen) + slen;
        } else {
            const unsigned char *p = bb->keys + bb->key_off[i - 1];
            size_t plen = bb->key_off[i] - bb->key_off[i - 1];
            size_t shared = commonPrefix(p + prefix, plen - prefix, k + prefix, slen);
            size_t unshared = slen - shared;
            kdat += slabVarintLen((uint32_t)shared) + slabVarintLen((uint32_t)unshared) + unshared;
        }
    }
    size_t vlen_bytes = 0;
    for (uint32_t i = 0; i < n; i++) vlen_bytes += slabVarintLen(bb->val_off[i + 1] - bb->val_off[i]);
    size_t restarts = n ? (n + kr - 1) / kr : 0, vgroups = n ? (n + vr - 1) / vr : 0;
    size_t total = layoutBytes(prefix, kdat, restarts, bb->vals_len + vlen_bytes, vgroups);
    assert(total <= SLAB_MAX_BLOCK_BYTES);

    unsigned char *buf = zcalloc(total);
    wr32(buf, SLAB_BASE_MAGIC);
    wr32(buf + 4, n);
    buf[8] = kr & 0xff;
    buf[9] = kr >> 8;
    buf[10] = vr & 0xff;
    buf[11] = vr >> 8;
    wr32(buf + 12, (uint32_t)prefix);
    wr32(buf + 16, (uint32_t)kdat);
    wr32(buf + 20, (uint32_t)bb->vals_len);
    wr32(buf + 24, (uint32_t)vlen_bytes);
    wr32(buf + 28, 0);
    wr64(buf + 32, total);
    unsigned char *p = buf + SLAB_BASE_HEADER_BYTES;
    if (n) memcpy(p, bb->keys, prefix);
    unsigned char *kd = p + prefix;
    unsigned char *krst = buf + align4(SLAB_BASE_HEADER_BYTES + prefix + kdat);
    size_t ko = 0;
    for (uint32_t i = 0; i < n; i++) {
        const unsigned char *k = bb->keys + bb->key_off[i] + prefix;
        size_t slen = bb->key_off[i + 1] - bb->key_off[i] - prefix;
        if (i % kr == 0) {
            wr32(krst + (i / kr) * 4, (uint32_t)ko);
            ko += varintPut(kd + ko, (uint32_t)slen);
            memcpy(kd + ko, k, slen);
            ko += slen;
        } else {
            const unsigned char *pk = bb->keys + bb->key_off[i - 1] + prefix;
            size_t plen = bb->key_off[i] - bb->key_off[i - 1] - prefix;
            size_t shared = commonPrefix(pk, plen, k, slen);
            ko += varintPut(kd + ko, (uint32_t)shared);
            ko += varintPut(kd + ko, (uint32_t)(slen - shared));
            memcpy(kd + ko, k + shared, slen - shared);
            ko += slen - shared;
        }
    }
    assert(ko == kdat);
    unsigned char *vblb = krst + restarts * 4;
    if (bb->vals_len) memcpy(vblb, bb->vals, bb->vals_len);
    unsigned char *vl = vblb + bb->vals_len;
    unsigned char *vors = buf + align4((size_t)(vl - buf) + vlen_bytes);
    size_t vo = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (i % vr == 0) wr32(vors + (i / vr) * 4, bb->val_off[i]);
        vo += varintPut(vl + vo, bb->val_off[i + 1] - bb->val_off[i]);
    }
    assert(vo == vlen_bytes);

    const char *err = NULL;
    slabBase *b = slabBaseLoad(buf, total, &err);
    assert(b != NULL);
    slabBaseBuilderReset(bb);
    return b;
}

slabBase *slabBaseBuild(uint32_t key_restart, uint32_t value_restart, size_t n, const unsigned char *const *keys, const size_t *klens, const unsigned char *const *vals, const size_t *vlens) {
    slabBaseBuilder bb;
    slabBaseBuilderInit(&bb, key_restart, value_restart);
    for (size_t i = 0; i < n; i++) slabBaseBuilderAdd(&bb, keys[i], klens[i], vals[i], vlens[i]);
    slabBase *b = slabBaseBuilderFinish(&bb);
    slabBaseBuilderRelease(&bb);
    return b;
}

size_t slabSizeClassFloor(size_t bytes) {
    size_t usable;
    void *p = zmalloc_usable(bytes, &usable);
    zfree(p);
    if (usable == bytes) return bytes;
    /* Binary search for the largest request whose class still fits in 'bytes'. Below
     * the smallest class nothing fits, and 'bytes' itself is returned. */
    size_t lo = 1, hi = bytes, best = bytes;
    while (lo <= hi) {
        size_t mid = lo + (hi - lo) / 2;
        p = zmalloc_usable(mid, &usable);
        zfree(p);
        if (usable <= bytes) {
            best = usable;
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    return best;
}

/* ---------------------------------------------------------------------------------------
 * Persisted delta segments
 * ------------------------------------------------------------------------------------- */

#define SEG_HEADER 8
#define SEG_RECORD_HEADER 8

slabDeltaSeg *slabDeltaSegLoad(unsigned char *buf, size_t len, const char **err) {
    if (len < SEG_HEADER || len > SLAB_MAX_BLOCK_BYTES || rd32(buf) != SLAB_SEG_MAGIC) {
        *err = "bad delta segment header";
        return NULL;
    }
    uint32_t entries = rd32(buf + 4);
    if ((uint64_t)entries * SEG_RECORD_HEADER > len - SEG_HEADER) {
        *err = "bad delta segment entry count";
        return NULL;
    }
    uint64_t cap = 2;
    while (cap < (uint64_t)entries * 2) cap <<= 1;
    size_t usable_s, usable_t;
    slabDeltaSeg *s = zcalloc_usable(sizeof(*s), &usable_s);
    s->slots = zcalloc_usable(cap * sizeof(uint64_t), &usable_t);
    s->mask = cap - 1;
    size_t off = SEG_HEADER;
    const unsigned char *prev = NULL;
    size_t prev_len = 0;
    for (uint32_t i = 0; i < entries; i++) {
        if (len - off < SEG_RECORD_HEADER) goto bad;
        uint32_t klen = rd32(buf + off), vlen = rd32(buf + off + 4);
        bool tomb = vlen == SLAB_TOMBSTONE;
        if (klen > SLAB_MAX_KEY_BYTES || (!tomb && vlen > SLAB_MAX_VALUE_BYTES)) goto bad;
        uint64_t need = (uint64_t)klen + (tomb ? 0 : vlen);
        if (len - off - SEG_RECORD_HEADER < need) goto bad;
        const unsigned char *key = buf + off + SEG_RECORD_HEADER;
        if (prev && slabKeyCmp(prev, prev_len, key, klen) >= 0) goto bad;
        uint64_t h = slabHash(key, klen);
        uint64_t idx = h & s->mask;
        while (s->slots[idx]) idx = (idx + 1) & s->mask;
        s->slots[idx] = (h >> 32) << 32 | (uint64_t)(off + 1);
        if (tomb)
            s->tombstones++;
        else
            s->puts++;
        s->payload_bytes += need;
        prev = key;
        prev_len = klen;
        off += SEG_RECORD_HEADER + need;
    }
    if (off != len) goto bad;
    atomic_init(&s->refcount, 1);
    s->buf = buf;
    s->len = len;
    s->entries = entries;
    s->alloc_bytes = usable_s + usable_t + zmalloc_size(buf);
    return s;
bad:
    *err = "invalid delta segment record";
    zfree(s->slots);
    zfree(s);
    return NULL;
}

void slabDeltaSegFree(slabDeltaSeg *s) {
    if (!s) return;
    zfree((void *)s->buf);
    zfree(s->slots);
    zfree(s);
}

int slabDeltaSegLookup(const slabDeltaSeg *s, const unsigned char *key, size_t klen, uint64_t hash, const unsigned char **val, size_t *vlen) {
    uint64_t tag = hash >> 32;
    uint64_t idx = hash & s->mask;
    for (;;) {
        uint64_t w = s->slots[idx];
        if (w == 0) return SLAB_SEG_MISS;
        if (w >> 32 == tag) {
            size_t off = (size_t)(w & 0xffffffffULL) - 1;
            const unsigned char *r = s->buf + off;
            uint32_t rk = rd32(r), rv = rd32(r + 4);
            if (rk == klen && memcmp(r + SEG_RECORD_HEADER, key, klen) == 0) {
                if (rv == SLAB_TOMBSTONE) return SLAB_SEG_TOMBSTONE;
                *val = r + SEG_RECORD_HEADER + rk;
                *vlen = rv;
                return SLAB_SEG_PUT;
            }
        }
        idx = (idx + 1) & s->mask;
    }
}

bool slabDeltaSegNext(const slabDeltaSeg *s, size_t *pos, const unsigned char **key, size_t *klen, const unsigned char **val, size_t *vlen) {
    size_t off = *pos ? *pos : SEG_HEADER;
    if (off >= s->len) return false;
    uint32_t rk = rd32(s->buf + off), rv = rd32(s->buf + off + 4);
    *key = s->buf + off + SEG_RECORD_HEADER;
    *klen = rk;
    if (rv == SLAB_TOMBSTONE) {
        *val = NULL;
        *vlen = 0;
        *pos = off + SEG_RECORD_HEADER + rk;
    } else {
        *val = *key + rk;
        *vlen = rv;
        *pos = off + SEG_RECORD_HEADER + rk + rv;
    }
    return true;
}

void slabSegWriterInit(slabSegWriter *w) {
    memset(w, 0, sizeof(*w));
    growBytes(&w->buf, &w->cap, SEG_HEADER);
    w->len = SEG_HEADER;
}

void slabSegWriterAdd(slabSegWriter *w, const unsigned char *key, size_t klen, const unsigned char *val, size_t vlen, bool tombstone) {
    size_t need = SEG_RECORD_HEADER + klen + (tombstone ? 0 : vlen);
    growBytes(&w->buf, &w->cap, w->len + need);
    unsigned char *p = w->buf + w->len;
    wr32(p, (uint32_t)klen);
    wr32(p + 4, tombstone ? SLAB_TOMBSTONE : (uint32_t)vlen);
    memcpy(p + SEG_RECORD_HEADER, key, klen);
    if (!tombstone && vlen) memcpy(p + SEG_RECORD_HEADER + klen, val, vlen);
    w->len += need;
    w->entries++;
}

unsigned char *slabSegWriterFinish(slabSegWriter *w, size_t *len) {
    wr32(w->buf, SLAB_SEG_MAGIC);
    wr32(w->buf + 4, w->entries);
    unsigned char *out = zrealloc(w->buf, w->len);
    *len = w->len;
    memset(w, 0, sizeof(*w));
    return out;
}
