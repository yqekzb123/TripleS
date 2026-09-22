#include "row_sdmvcc.h"

#include "catalog.h"
#include "helper.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "txn_table.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cstddef>
#include <set>

#if CC_ALG == SDMVCC

namespace {
// Statistics are striped by thread. The old single global atomic cache line
// was touched several times for every intent and version operation.
class StripedCounter {
    struct alignas(64) Slot { std::atomic<uint64_t> value; };
    static const unsigned kSlots = 128;
    Slot slots_[kSlots];
    static unsigned slot() {
        static thread_local unsigned value = static_cast<unsigned>(
            ((uintptr_t)pthread_self() >> 4) & (kSlots - 1));
        return value;
    }
public:
    StripedCounter() {
        for (unsigned i = 0; i < kSlots; ++i)
            slots_[i].value.store(0, std::memory_order_relaxed);
    }
    uint64_t fetch_add(uint64_t v,
                       std::memory_order = std::memory_order_relaxed) {
        return slots_[slot()].value.fetch_add(v, std::memory_order_relaxed);
    }
    uint64_t fetch_sub(uint64_t v,
                       std::memory_order = std::memory_order_relaxed) {
        return slots_[slot()].value.fetch_sub(v, std::memory_order_relaxed);
    }
    uint64_t load(std::memory_order = std::memory_order_relaxed) const {
        uint64_t result = 0;
        for (unsigned i = 0; i < kSlots; ++i)
            result += slots_[i].value.load(std::memory_order_relaxed);
        return result;
    }
};

// Gauges may be incremented by a scheduler and decremented by a worker. Their
// individual shards can therefore be negative even though the aggregate is
// non-negative; signed shards avoid unsigned underflow on cross-thread frees.
class StripedGauge {
    struct alignas(64) Slot { std::atomic<int64_t> value; };
    static const unsigned kSlots = 128;
    Slot slots_[kSlots];
    static unsigned slot() {
        static thread_local unsigned value = static_cast<unsigned>(
            ((uintptr_t)pthread_self() >> 4) & (kSlots - 1));
        return value;
    }
public:
    StripedGauge() {
        for (unsigned i = 0; i < kSlots; ++i)
            slots_[i].value.store(0, std::memory_order_relaxed);
    }
    void fetch_add(uint64_t v,
                   std::memory_order = std::memory_order_relaxed) {
        slots_[slot()].value.fetch_add(static_cast<int64_t>(v),
                                       std::memory_order_relaxed);
    }
    void fetch_sub(uint64_t v,
                   std::memory_order = std::memory_order_relaxed) {
        slots_[slot()].value.fetch_sub(static_cast<int64_t>(v),
                                       std::memory_order_relaxed);
    }
    uint64_t load(std::memory_order = std::memory_order_relaxed) const {
        int64_t result = 0;
        for (unsigned i = 0; i < kSlots; ++i)
            result += slots_[i].value.load(std::memory_order_relaxed);
        return result > 0 ? static_cast<uint64_t>(result) : 0;
    }
};

StripedCounter g_intents_registered;
StripedCounter g_intents_released;
StripedCounter g_intent_waits;
StripedCounter g_intent_notifications;
StripedCounter g_versions_created;
StripedCounter g_versions_reclaimed;
StripedGauge g_version_bytes;
StripedCounter g_gc_calls;
StripedCounter g_gc_disabled_calls;
std::atomic<uint64_t> g_lazy_registration_skips(0);
std::atomic<uint64_t> g_lazy_ready_reads(0);
std::atomic<uint64_t> g_lazy_waits(0);
std::atomic<uint64_t> g_long_guards_registered(0);
std::atomic<uint64_t> g_long_guards_released(0);
std::atomic<uint64_t> g_long_guard_keys(0);
std::atomic<uint64_t> g_long_guard_build_ns(0);
std::atomic<uint64_t> g_long_guard_gc_probes(0);
std::atomic<uint64_t> g_long_guard_gc_protected(0);
std::atomic<uint64_t> g_long_guard_active(0);
std::atomic<uint64_t> g_long_guard_peak_active(0);
std::atomic<uint64_t> g_unsafe_intent_skips(0);
std::atomic<uint64_t> g_unsafe_reads(0);
std::atomic<uint64_t> g_unsafe_fallback_reads(0);
std::atomic<uint64_t> g_blind_writes_registered(0);
StripedGauge g_active_read_intents;
std::atomic<uint64_t> g_peak_active_read_intents(0);
StripedGauge g_live_versions;
StripedCounter g_tracked_rows;
std::atomic<uint64_t> g_peak_version_chain(0);
StripedCounter g_gc_candidates_processed;
StripedCounter g_gc_local_checks;
struct DeferredShard {
    pthread_mutex_t latch;
    SDMVCCIndex rows;
    std::atomic<uint64_t> first_threshold;
    DeferredShard() : first_threshold(UINT64_MAX) {
        pthread_mutex_init(&latch, nullptr);
    }
};
static const unsigned kGcShards = 16;
DeferredShard g_deferred[kGcShards];
unsigned row_shard(const Row_sdmvcc *row) {
    return (reinterpret_cast<uintptr_t>(row) >> 6) % kGcShards;
}
SDMVCCEntry *from_version_index(SDMVCCIndexNode *node) {
    return node ? reinterpret_cast<SDMVCCEntry *>(
        reinterpret_cast<char *>(node) - offsetof(SDMVCCEntry, versionIndex)) : nullptr;
}
SDMVCCEntry *from_gc_index(SDMVCCIndexNode *node) {
    return node ? reinterpret_cast<SDMVCCEntry *>(
        reinterpret_cast<char *>(node) - offsetof(SDMVCCEntry, gcIndex)) : nullptr;
}
pthread_mutex_t g_snapshot_pin_latch = PTHREAD_MUTEX_INITIALIZER;
std::multiset<uint64_t> g_snapshot_pins;
std::atomic<uint64_t> g_oldest_pinned_snapshot(UINT64_MAX);
// GC is the overwhelmingly common operation and only reads the registry.
// A rwlock lets independent row GC paths probe the single L1 guard without
// serializing on one process-wide mutex; begin/finalize/remove are rare.
pthread_rwlock_t g_long_guard_latch = PTHREAD_RWLOCK_INITIALIZER;
std::vector<SDMVCCLongReadGuard *> g_long_guards;

uint64_t mix64(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

void long_guard_add(SDMVCCLongReadGuard *guard, row_t *row) {
    assert(guard != NULL && guard->building);
    const uint64_t bit_count = guard->bloom.size() * 64;
    assert(bit_count > 0);
    const uint64_t key = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(row));
    const uint64_t h1 = mix64(key);
    const uint64_t h2 = mix64(key ^ 0xd6e8feb86659fd93ULL) | 1ULL;
    for (uint64_t i = 0; i < SDMVCC_LONG_READ_GUARD_HASHES; ++i) {
        const uint64_t bit = (h1 + i * h2) % bit_count;
        guard->bloom[bit >> 6] |= 1ULL << (bit & 63);
    }
}

bool long_guard_maybe_contains(const SDMVCCLongReadGuard *guard, row_t *row) {
    const uint64_t bit_count = guard->bloom.size() * 64;
    const uint64_t key = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(row));
    const uint64_t h1 = mix64(key);
    const uint64_t h2 = mix64(key ^ 0xd6e8feb86659fd93ULL) | 1ULL;
    for (uint64_t i = 0; i < SDMVCC_LONG_READ_GUARD_HASHES; ++i) {
        const uint64_t bit = (h1 + i * h2) % bit_count;
        if ((guard->bloom[bit >> 6] & (1ULL << (bit & 63))) == 0)
            return false;
    }
    return true;
}

void update_peak(std::atomic<uint64_t> &peak, uint64_t value) {
    uint64_t current = peak.load(std::memory_order_relaxed);
    while (value > current &&
           !peak.compare_exchange_weak(current, value,
                                       std::memory_order_relaxed)) {}
}

// Schedulers allocate eager intents while workers release them. Per-thread
// caches transfer batches through one central list, reducing malloc/free and
// central-lock traffic. No pool lock is taken while a row latch is held.
struct EntryPool {
    pthread_mutex_t latch;
    SDMVCCEntry *head;
    EntryPool() : head(nullptr) { pthread_mutex_init(&latch, nullptr); }
} g_entry_pool;

struct LocalEntryCache {
    SDMVCCEntry *head;
    unsigned count;
    LocalEntryCache() : head(nullptr), count(0) {}
};
thread_local LocalEntryCache g_entry_cache;

void refill_entry_cache() {
    pthread_mutex_lock(&g_entry_pool.latch);
    while (g_entry_pool.head && g_entry_cache.count < 64) {
        SDMVCCEntry *e = g_entry_pool.head;
        g_entry_pool.head = e->retiredNext;
        e->retiredNext = g_entry_cache.head;
        g_entry_cache.head = e;
        ++g_entry_cache.count;
    }
    pthread_mutex_unlock(&g_entry_pool.latch);
    if (g_entry_cache.head) return;
    static const unsigned kBatch = 64;
    SDMVCCEntry *batch = static_cast<SDMVCCEntry *>(
        mem_allocator.alloc(sizeof(SDMVCCEntry) * kBatch));
    for (unsigned i = 0; i < kBatch; ++i) {
        batch[i].retiredNext = g_entry_cache.head;
        g_entry_cache.head = &batch[i];
        ++g_entry_cache.count;
    }
}

// Entries and tuple buffers are prepared before taking the row latch.
SDMVCCEntry *alloc_entry(bool is_version, uint64_t sid) {
    if (!g_entry_cache.head) refill_entry_cache();
    SDMVCCEntry *e = g_entry_cache.head;
    g_entry_cache.head = e->retiredNext;
    --g_entry_cache.count;
    e->isVersion = is_version;
    e->sid = sid;
    e->prevAll = e->nextAll = NULL;
    e->prevWrite = e->nextWrite = NULL;
    e->ready = false;
    e->base_backed = false;
    e->data = NULL;
    e->data_len = 0;
    e->txn = NULL;
    e->myVersion = NULL;
    e->armed = false;
    e->ephemeral = false;
    e->waitHead = e->waitNext = e->retiredNext = nullptr;
    e->versionIndex.init(sid);
    e->gcIndex.init(0);
    return e;
}

void free_entry(SDMVCCEntry *e) {
    assert(e->data == NULL);
    e->retiredNext = g_entry_cache.head;
    g_entry_cache.head = e;
    if (++g_entry_cache.count < 256) return;
    SDMVCCEntry *tail = g_entry_cache.head;
    for (unsigned i = 1; i < 128; ++i) tail = tail->retiredNext;
    SDMVCCEntry *batch = g_entry_cache.head;
    g_entry_cache.head = tail->retiredNext;
    g_entry_cache.count -= 128;
    pthread_mutex_lock(&g_entry_pool.latch);
    tail->retiredNext = g_entry_pool.head;
    g_entry_pool.head = batch;
    pthread_mutex_unlock(&g_entry_pool.latch);
}

void free_version_data(SDMVCCEntry *e) {
    if (e->data) {
        g_version_bytes.fetch_sub(e->data_len, std::memory_order_relaxed);
        mem_allocator.free(e->data, e->data_len);
        e->data = NULL;
        e->data_len = 0;
    }
}
}

Row_sdmvcc::Row_sdmvcc() : _row(NULL), _version_cnt(0), _creation_sid(0),
    _deferred_threshold(UINT64_MAX), _retired(nullptr),
    _retired_buffer(nullptr), _retired_buffer_len(0) {
    pthread_mutex_init(&_latch, NULL);
    memset(&_sentinel, 0, sizeof(_sentinel));
    _deferred.row = this;
    _deferred.node.init(0);
}

void Row_sdmvcc::init(row_t *row) {
    _row = row;
    // The sentinel terminates both chains: isVersion, ready, SID = MAX. It is
    // never reclaimed and removes every empty-chain special case.
    _sentinel.isVersion = true;
    _sentinel.sid = UINT64_MAX;
    _sentinel.prevAll = _sentinel.nextAll = &_sentinel;
    _sentinel.prevWrite = _sentinel.nextWrite = &_sentinel;
    _sentinel.ready = true;
    _sentinel.base_backed = false;
    _sentinel.data = NULL;
    _sentinel.data_len = 0;
    _sentinel.txn = NULL;
    _sentinel.myVersion = NULL;
    _sentinel.armed = false;
    _sentinel.ephemeral = false;
    _version_cnt = 0;
    // Immutable base version 0 backs reads until the first update promotes a
    // committed version into the base row.
    SDMVCCEntry *v0 = alloc_entry(true, 0);
    v0->ready = true;
    v0->base_backed = true;
    v0->prevAll = v0->nextAll = &_sentinel;
    v0->prevWrite = v0->nextWrite = &_sentinel;
    _sentinel.prevAll = _sentinel.nextAll = v0;
    _sentinel.prevWrite = _sentinel.nextWrite = v0;
    _version_cnt = 1;
    _version_index.insert(&v0->versionIndex);
    g_tracked_rows.fetch_add(1, std::memory_order_relaxed);
    g_live_versions.fetch_add(1, std::memory_order_relaxed);
    update_peak(g_peak_version_chain, 1);
}

// Version lookup is indexed, with an O(1) common tail fast path.
SDMVCCEntry *Row_sdmvcc::find_version_locked(uint64_t sid) {
    SDMVCCEntry *tail = _sentinel.prevWrite;
    if (tail != &_sentinel && tail->sid == sid) return tail;
    auto n = _version_index.lower_bound(sid);
    if (n && n->key == sid) return from_version_index(n);
    // Keep the index as the fast path, but recover from an inconsistent or
    // incompletely updated index.  The write chain is ascending, so once an
    // entry with a smaller SID is seen, no equal SID can remain behind it.
    for (SDMVCCEntry *e = _sentinel.prevWrite;
         e != &_sentinel && e->sid >= sid; e = e->prevWrite) {
        if (e->sid == sid) return e;
    }
    return nullptr;
}

SDMVCCEntry *Row_sdmvcc::predecessor_locked(uint64_t snapshot) {
    SDMVCCEntry *tail = _sentinel.prevWrite;
    if (tail != &_sentinel && tail->sid < snapshot) return tail;
    auto n = _version_index.before(snapshot);
    return n ? from_version_index(n) : &_sentinel;
}

SDMVCCEntry *Row_sdmvcc::read_version_locked(TxnManager *txn, uint64_t snapshot) {
    SDMVCCEntry *intent = txn->sdmvcc_intent_node(_row);
    if (intent && intent->myVersion) return intent->myVersion;
    return predecessor_locked(snapshot);
}

// Insert an intent into the all-entry chain, sorted by SID, inside the gap
// (myVersion, myVersion->nextWrite). Registration order usually matches SID
// order, so the backward walk from the gap end is O(1); late remote
// reservations may force walking past a few younger intents.
void Row_sdmvcc::insert_intent_locked(SDMVCCEntry *intent,
                                      SDMVCCEntry *myVersion) {
    // Cache only at Arm, when admission has finalized the predecessor.
    assert(myVersion != &_sentinel && myVersion->sid < intent->sid);
    intent->myVersion = nullptr;
    cancel_gc_locked(myVersion);
    // Locate the insertion point forward from the version gap start.  This is
    // equivalent to the old backward walk when the chain is healthy, but it
    // never inserts before the base version when the predecessor is the
    // newest version (whose nextWrite is the sentinel).
    SDMVCCEntry *gap_end = myVersion->nextWrite;
    SDMVCCEntry *pos = myVersion;
    for (SDMVCCEntry *cur = myVersion->nextAll; cur != gap_end;
         cur = cur->nextAll) {
        if (cur->sid > intent->sid ||
            (cur->sid == intent->sid && cur->isVersion)) break;
        pos = cur;
    }
    intent->prevAll = pos;
    intent->nextAll = pos->nextAll;
    pos->nextAll->prevAll = intent;
    pos->nextAll = intent;
    g_active_read_intents.fetch_add(1, std::memory_order_relaxed);
}

// First locate the version gap, then search only its read entries.
// R(S) sorts before V(S). No reader retargeting during registration.
SDMVCCEntry *Row_sdmvcc::insert_version_locked(SDMVCCEntry *v) {
    // Never publish a second version with the same SID, even if the fast
    // index was temporarily inconsistent while another thread was working.
    SDMVCCEntry *existing = find_version_locked(v->sid);
    if (existing) return existing;

    SDMVCCEntry *wp = predecessor_locked(v->sid);
    SDMVCCEntry *w = wp == &_sentinel ? _sentinel.nextWrite : wp->nextWrite;
    cancel_gc_locked(wp);
    SDMVCCEntry *pos = wp;
    for (SDMVCCEntry *cur = wp->nextAll; cur != w; cur = cur->nextAll) {
        if (cur->sid > v->sid) break;
        pos = cur;
    }
    v->prevAll = pos; v->nextAll = pos->nextAll;
    pos->nextAll->prevAll = v; pos->nextAll = v;
    v->prevWrite = wp; v->nextWrite = w;
    wp->nextWrite = v; w->prevWrite = v;
    _version_index.insert(&v->versionIndex);
    ++_version_cnt;
    g_live_versions.fetch_add(1, std::memory_order_relaxed);
    g_versions_created.fetch_add(1, std::memory_order_relaxed);
    update_peak(g_peak_version_chain, _version_cnt);
    return v;
}

void Row_sdmvcc::validate_locked(const char *where) {
    auto dump = [&](const char *tag) {
        int n = 0;
        fprintf(stderr, "[DUMP %s] vcnt=%u all:", tag, _version_cnt);
        for (const SDMVCCEntry *e = _sentinel.nextAll;
             e != &_sentinel && n++ < 60; e = e->nextAll)
            fprintf(stderr, " %p:%lu%s%s", (void *)e, e->sid,
                    e->isVersion ? "V" : "i", e->ready ? "C" : "P");
        n = 0;
        fprintf(stderr, "\n[DUMP %s] wrt:", tag);
        for (const SDMVCCEntry *e = _sentinel.nextWrite;
             e != &_sentinel && n++ < 60; e = e->nextWrite)
            fprintf(stderr, " %p:%lu%s", (void *)e, e->sid,
                    e->ready ? "C" : "P");
        fprintf(stderr, "\n");
        fflush(stderr);
    };
    const SDMVCCEntry *pw = &_sentinel;
    uint64_t last = 0;
    bool first = true;
    bool last_version = false;
    for (const SDMVCCEntry *all = _sentinel.nextAll; all != &_sentinel;
         all = all->nextAll) {
        if (!first && (all->sid < last ||
            (all->sid == last && (last_version || !all->isVersion)))) {
            fprintf(stderr,
                    "[VALIDATE %s] all-chain order broken: %lu after %lu\n",
                    where, all->sid, last);
            dump(where);
            dump(where);
        abort();
        }
        if (all->isVersion) {
            if (pw->nextWrite != all) {
                fprintf(stderr,
                        "[VALIDATE %s] version %lu chain mismatch (write "
                        "pred %lu)\n",
                        where, all->sid, pw->nextWrite->sid);
                dump(where);
            dump(where);
        abort();
            }
            pw = all;
        }
        last = all->sid;
        last_version = all->isVersion;
        first = false;
    }
    if (pw != _sentinel.prevWrite) {
        fprintf(stderr, "[VALIDATE %s] write-chain tail mismatch\n", where);
        dump(where);
        abort();
    }
}

void Row_sdmvcc::unlink_all_locked(SDMVCCEntry *entry) {
    entry->prevAll->nextAll = entry->nextAll;
    entry->nextAll->prevAll = entry->prevAll;
    entry->prevAll = entry->nextAll = NULL;
}

// Scheduling path:
//   workload acquire_locks() -> row_t::get_lock() -> register_access().
// The transaction-level index deduplicates repeated accesses to this key;
// remaining_uses still records how many execution-time uses must be consumed.
RC Row_sdmvcc::register_access(access_t type, TxnManager *txn) {
    const uint64_t sid = txn->sdmvcc_snapshot();
    const int registration = txn->register_sdmvcc_access(_row, type);
    if (registration == 0) return RCOK;
    const bool blind = txn->is_sdmvcc_blind_write(_row, type);
    const bool guard = txn->uses_sdmvcc_long_read_guard();
    const bool unsafe = txn->uses_sdmvcc_unsafe_l1_no_intent();
    if (registration == 1 && unsafe)
        g_unsafe_intent_skips.fetch_add(1, std::memory_order_relaxed);
    if (type != WR && (guard || unsafe)) return RCOK;
    const bool eager = registration == 1 && !blind &&
        !SDMVCC_LAZY_READ_INTENT && !guard && !unsafe;
    SDMVCCEntry *intent = eager ? alloc_entry(false, sid) : nullptr;
    SDMVCCEntry *reserved = type == WR ? alloc_entry(true, sid) : nullptr;
    if (intent) intent->txn = txn;
    pthread_mutex_lock(&_latch);
    if (intent) {
        SDMVCCEntry *pred = predecessor_locked(sid);
        assert(pred != &_sentinel);
        insert_intent_locked(intent, pred);
        g_intents_registered.fetch_add(1, std::memory_order_relaxed);
        txn->sdmvcc_set_intent_node(_row, intent);
    } else if (registration == 1 && blind) {
        g_blind_writes_registered.fetch_add(1, std::memory_order_relaxed);
    } else if (registration == 1) {
        g_lazy_registration_skips.fetch_add(1, std::memory_order_relaxed);
    }
    if (reserved) {
        SDMVCCEntry *v = find_version_locked(sid);
        if (!v) {
            SDMVCCEntry *candidate = reserved;
            reserved = nullptr;
            v = insert_version_locked(candidate);
            if (v != candidate) free_entry(candidate);
        }
        txn->sdmvcc_set_version_node(_row, v);
    }
    update_deferred_locked();
    unlock_and_dispose();
    if (reserved) free_entry(reserved);
    return RCOK;
}

// Readiness test and waiter attachment remain atomic under the row latch.
// Notification still uses the original lr counter + lock_ready CAS protocol.
bool Row_sdmvcc::arm_read(TxnManager *txn, uint64_t snapshot) {
    SDMVCCEntry *node = txn->sdmvcc_intent_node(_row);
    SDMVCCEntry *spare = node ? nullptr : alloc_entry(false, snapshot);
    pthread_mutex_lock(&_latch);
    SDMVCCEntry *version = predecessor_locked(snapshot);
    if (node) node->myVersion = version;
    if (version == &_sentinel || version->ready) {
        unlock_and_dispose();
    if (spare) free_entry(spare);
        return true;
    }
    if (!node) {
        node = spare; spare = nullptr;
        node->txn = txn; node->ephemeral = true;
        insert_intent_locked(node, version);
        node->myVersion = version;
    }
    if (!node->armed) {
        node->armed = true;
        node->waitNext = version->waitHead;
        version->waitHead = node;
        txn->incr_lr();
        g_intent_waits.fetch_add(1, std::memory_order_relaxed);
    }
    update_deferred_locked();
    unlock_and_dispose();
    if (spare) free_entry(spare);
    return false;
}

// Spare storage is allocated before the caller acquires the row latch.
bool Row_sdmvcc::wait_for_predecessor_locked(TxnManager *txn,
                         SDMVCCEntry *version, SDMVCCEntry *&spare) {
    SDMVCCEntry *node = txn->sdmvcc_intent_node(_row);
    if (!node) {
        if (!SDMVCC_LAZY_READ_INTENT && !txn->uses_sdmvcc_long_read_guard())
            return false;
        bool new_intent = false;
        if (!txn->arm_sdmvcc_lazy_access(_row, new_intent)) return true;
        assert(spare);
        node = spare; spare = nullptr;
        node->txn = txn;
        node->ephemeral = !new_intent;
        insert_intent_locked(node, version);
        if (new_intent) {
            txn->sdmvcc_set_intent_node(_row, node);
            g_intents_registered.fetch_add(1, std::memory_order_relaxed);
        }
    }
    if (!node->armed) {
        ATOM_CAS(txn->lock_ready, true, false);
        node->myVersion = version;
        node->armed = true;
        node->waitNext = version->waitHead;
        version->waitHead = node;
        txn->incr_lr();
        g_intent_waits.fetch_add(1, std::memory_order_relaxed);
        if (SDMVCC_LAZY_READ_INTENT)
            g_lazy_waits.fetch_add(1, std::memory_order_relaxed);
    }
    update_deferred_locked();
    return true;
}



static SDMVCCEntry *newest_ready_version_locked(SDMVCCEntry &sentinel) {
    for (SDMVCCEntry *e = sentinel.prevWrite; e != &sentinel; e = e->prevWrite) {
        if (e->ready) return e;
    }
    return &sentinel;
}

RC Row_sdmvcc::read(TxnManager *txn, uint64_t snapshot, row_t *local_row) {
    SDMVCCEntry *spare =
        (SDMVCC_LAZY_READ_INTENT || txn->uses_sdmvcc_long_read_guard()) &&
        !txn->sdmvcc_intent_node(_row) ? alloc_entry(false, snapshot) : nullptr;
    pthread_mutex_lock(&_latch);
    const bool unsafe_no_intent = txn->uses_sdmvcc_unsafe_l1_no_intent();
    SDMVCCEntry *version = read_version_locked(txn, snapshot);
    if (_creation_sid && (snapshot <= _creation_sid ||
        (version != &_sentinel && version->sid < _creation_sid)))
        version = &_sentinel;
    bool fallback = false;
    if (unsafe_no_intent && (version == &_sentinel || !version->ready)) {
        // Intentionally incorrect upper-bound behavior: choose the newest
        // committed version instead of waiting for/preserving the snapshot
        // predecessor.  Selection and copying remain under _latch, so normal
        // GC cannot turn this experiment into a use-after-free.
        version = newest_ready_version_locked(_sentinel);
        fallback = true;
    }
    if (version == &_sentinel) {
        if (unsafe_no_intent) {
            memcpy(local_row->get_data(), _row->get_data(),
                   _row->get_tuple_size());
            g_unsafe_reads.fetch_add(1, std::memory_order_relaxed);
            g_unsafe_fallback_reads.fetch_add(1, std::memory_order_relaxed);
            unlock_and_dispose();
            if (spare) free_entry(spare);
            return RCOK;
        }
        unlock_and_dispose();
    if (spare) free_entry(spare);
        return Abort;
    }
    if (!version->ready) {
        wait_for_predecessor_locked(txn, version, spare);
        unlock_and_dispose();
    if (spare) free_entry(spare);
        return WAIT;
    }
    if (SDMVCC_LAZY_READ_INTENT || txn->uses_sdmvcc_long_read_guard())
        g_lazy_ready_reads.fetch_add(1, std::memory_order_relaxed);
    if (unsafe_no_intent) {
        g_unsafe_reads.fetch_add(1, std::memory_order_relaxed);
        if (fallback) g_unsafe_fallback_reads.fetch_add(1, std::memory_order_relaxed);
    }
    if (version->base_backed) {
        memcpy(local_row->get_data(), _row->get_data(),
               _row->get_tuple_size());
    } else {
        assert(version->data && version->data_len == _row->get_tuple_size());
        memcpy(local_row->get_data(), version->data, _row->get_tuple_size());
    }
    unlock_and_dispose();
    if (spare) free_entry(spare);
    return RCOK;
}

// TPC-C has rows whose concrete keys are discovered only during execution.
// SDPCC reads those rows directly at that point; use the newest completed
// SDMVCC version to preserve the same workload behavior without adding a
// second scheduling round.
RC Row_sdmvcc::read_latest(row_t *local_row) {
    pthread_mutex_lock(&_latch);
    SDMVCCEntry *version = newest_ready_version_locked(_sentinel);
    if (version != &_sentinel && version->sid < _creation_sid)
        version = &_sentinel;
    if (version == &_sentinel) {
        pthread_mutex_unlock(&_latch);
        return Abort;
    }
    if (version->base_backed) {
        memcpy(local_row->get_data(), _row->get_data(),
               _row->get_tuple_size());
    } else {
        assert(version->data && version->data_len == _row->get_tuple_size());
        memcpy(local_row->get_data(), version->data, _row->get_tuple_size());
    }
    pthread_mutex_unlock(&_latch);
    return RCOK;
}

RC Row_sdmvcc::read_value(TxnManager *txn, uint64_t snapshot, uint32_t column,
                          void *value, uint32_t size) {
    SDMVCCEntry *spare =
        (SDMVCC_LAZY_READ_INTENT || txn->uses_sdmvcc_long_read_guard()) &&
        !txn->sdmvcc_intent_node(_row) ? alloc_entry(false, snapshot) : nullptr;
    pthread_mutex_lock(&_latch);
    const bool unsafe_no_intent = txn->uses_sdmvcc_unsafe_l1_no_intent();
    SDMVCCEntry *version = read_version_locked(txn, snapshot);
    if (_creation_sid && (snapshot <= _creation_sid ||
        (version != &_sentinel && version->sid < _creation_sid)))
        version = &_sentinel;
    bool fallback = false;
    if (unsafe_no_intent && (version == &_sentinel || !version->ready)) {
        version = newest_ready_version_locked(_sentinel);
        fallback = true;
    }
    if (version == &_sentinel) {
        if (unsafe_no_intent) {
            assert(size <= _row->get_schema()->get_field_size(column));
            const uint32_t offset = _row->get_schema()->get_field_index(column);
            memcpy(value, _row->get_data() + offset, size);
            g_unsafe_reads.fetch_add(1, std::memory_order_relaxed);
            g_unsafe_fallback_reads.fetch_add(1, std::memory_order_relaxed);
            unlock_and_dispose();
            if (spare) free_entry(spare);
            return RCOK;
        }
        unlock_and_dispose();
    if (spare) free_entry(spare);
        return Abort;
    }
    if (!version->ready) {
        wait_for_predecessor_locked(txn, version, spare);
        unlock_and_dispose();
    if (spare) free_entry(spare);
        return WAIT;
    }
    if (SDMVCC_LAZY_READ_INTENT || txn->uses_sdmvcc_long_read_guard())
        g_lazy_ready_reads.fetch_add(1, std::memory_order_relaxed);
    if (unsafe_no_intent) {
        g_unsafe_reads.fetch_add(1, std::memory_order_relaxed);
        if (fallback) g_unsafe_fallback_reads.fetch_add(1, std::memory_order_relaxed);
    }
    assert(size <= _row->get_schema()->get_field_size(column));
    const uint32_t offset = _row->get_schema()->get_field_index(column);
    const char *data = version->base_backed
                           ? _row->get_data() + offset
                           : version->data + offset;
    memcpy(value, data, size);
    unlock_and_dispose();
    if (spare) free_entry(spare);
    return RCOK;
}

bool Row_sdmvcc::visible(uint64_t snapshot) {
    pthread_mutex_lock(&_latch);
    const bool result = snapshot > _creation_sid &&
        predecessor_locked(snapshot) != &_sentinel;
    pthread_mutex_unlock(&_latch);
    return result;
}

void Row_sdmvcc::set_creation_sid(uint64_t sid) {
    pthread_mutex_lock(&_latch);
    _creation_sid = sid;
    SDMVCCEntry *created = find_version_locked(sid);
    if (!created) {
        SDMVCCEntry *front = _sentinel.nextWrite;
        assert(front != &_sentinel && front->nextWrite == &_sentinel);
        assert(front->sid == 0 && front->ready && front->base_backed);
        // An active base reader would make renaming invalidate its binding.
        assert(front->nextAll == &_sentinel);
        _version_index.erase(&front->versionIndex);
        front->sid = sid; front->versionIndex.init(sid);
        _version_index.insert(&front->versionIndex);
    }
    // With a reserved version, leave V0 until local GC can reclaim it safely.
    // Visibility uses _creation_sid, so this does not expose the row to old SIDs.
    unlock_and_dispose();
}

void Row_sdmvcc::stage_write(uint64_t sid, row_t *local_row, SDMVCCEntry *handle) {
    const uint32_t size = _row->get_tuple_size();
    char *buffer = static_cast<char *>(mem_allocator.alloc(size));
    memcpy(buffer, local_row->get_data(), size);
    pthread_mutex_lock(&_latch);
    SDMVCCEntry *v = handle ? handle : find_version_locked(sid);
    assert(v && v->sid == sid && !v->ready);
    char *old = v->data;
    const uint32_t old_len = v->data_len;
    v->data = buffer; v->data_len = size;
    g_version_bytes.fetch_add(size, std::memory_order_relaxed);
    unlock_and_dispose();
    if (old) {
        g_version_bytes.fetch_sub(old_len, std::memory_order_relaxed);
        mem_allocator.free(old, old_len);
    }
}

// Notification path:
//   publish_write()/abort_write() -> notify_ready() -> txn_table.restart_txn().
// A transaction can wait on several keys, so only the final decr_lr() queues
// it for execution. lock_ready prevents duplicate queueing by concurrent
// publishers.
void Row_sdmvcc::notify_ready(TxnManager *txn, uint64_t thd_id) {
    g_intent_notifications.fetch_add(1, std::memory_order_relaxed);
    if (txn->decr_lr() == 0 && ATOM_CAS(txn->lock_ready, false, true)) {
        txn_table.restart_txn(thd_id, txn->get_txn_id(), txn->get_batch_id());
    }
}

// Each real waiter remains alive until its own lr notification. Take next
// and txn before notifying: the awakened transaction may immediately free it.
void Row_sdmvcc::dispatch_waiters(SDMVCCEntry *head, uint64_t thd_id) {
    while (head) {
        SDMVCCEntry *next = head->waitNext;
        TxnManager *txn = head->txn;
        const bool ephemeral = head->ephemeral;
        head->waitNext = nullptr;
        if (ephemeral) free_entry(head);
        notify_ready(txn, thd_id);
        head = next;
    }
}

void Row_sdmvcc::publish_write(uint64_t sid, uint64_t thd_id, bool early,
                               SDMVCCEntry *handle) {
    pthread_mutex_lock(&_latch);
    SDMVCCEntry *v = handle ? handle : find_version_locked(sid);
    assert(v && v->sid == sid && v->data && !v->ready);
    v->ready = true;
    SDMVCCEntry *wake = v->waitHead;
    v->waitHead = nullptr;
    for (SDMVCCEntry *e = wake; e; e = e->waitNext) {
        assert(e->armed && e->myVersion == v);
        e->armed = false;
        if (e->ephemeral) {
            unlink_all_locked(e);
            g_active_read_intents.fetch_sub(1, std::memory_order_relaxed);
        }
    }
    if (!early) {
        check_gc_locked(v->prevWrite, minSid);
        check_gc_locked(v, minSid);
    }
    update_deferred_locked();
    promote_locked();
    unlock_and_dispose();
    dispatch_waiters(wake, thd_id);
}

void Row_sdmvcc::abort_write(uint64_t sid, uint64_t thd_id, SDMVCCEntry *handle) {
    SDMVCCEntry *wake = nullptr;
    pthread_mutex_lock(&_latch);
    SDMVCCEntry *v = handle ? handle : find_version_locked(sid);
    if (v) {
        assert(v->sid == sid && !v->ready);
        SDMVCCEntry *pred = v->prevWrite;
        // Ordinary write reservations always have a predecessor (V0 included).
        assert(pred != &_sentinel);
        SDMVCCEntry *e = v->waitHead;
        v->waitHead = nullptr;
        while (e) {
            SDMVCCEntry *next = e->waitNext;
            e->myVersion = pred;
            if (pred->ready) {
                e->armed = false;
                e->waitNext = wake; wake = e;
                if (e->ephemeral) {
                    unlink_all_locked(e);
                    g_active_read_intents.fetch_sub(1, std::memory_order_relaxed);
                }
            } else {
                e->waitNext = pred->waitHead; pred->waitHead = e;
            }
            e = next;
        }
        cancel_gc_locked(v);
        _version_index.erase(&v->versionIndex);
        v->prevWrite->nextWrite = v->nextWrite;
        v->nextWrite->prevWrite = v->prevWrite;
        unlink_all_locked(v);
        retire_locked(v);
        --_version_cnt;
        g_live_versions.fetch_sub(1, std::memory_order_relaxed);
        check_gc_locked(pred, minSid);
    }
    update_deferred_locked();
    promote_locked();
    unlock_and_dispose();
    dispatch_waiters(wake, thd_id);
}

void Row_sdmvcc::retire_locked(SDMVCCEntry *entry) {
    entry->retiredNext = _retired;
    _retired = entry;
}

void Row_sdmvcc::unlock_and_dispose() {
    SDMVCCEntry *retired = _retired;
    char *buffer = _retired_buffer;
    const uint32_t size = _retired_buffer_len;
    _retired = nullptr;
    _retired_buffer = nullptr;
    _retired_buffer_len = 0;
    pthread_mutex_unlock(&_latch);
    while (retired) {
        SDMVCCEntry *next = retired->retiredNext;
        free_version_data(retired);
        free_entry(retired);
        retired = next;
    }
    if (buffer) {
        g_version_bytes.fetch_sub(size, std::memory_order_relaxed);
        mem_allocator.free(buffer, size);
    }
}

void Row_sdmvcc::promote_locked() {
    if (_version_cnt != 1) return;
    SDMVCCEntry *v = _sentinel.nextWrite;
    if (v->ready && !v->base_backed && v->data) {
        assert(!_retired_buffer);
        memcpy(_row->get_data(), v->data, _row->get_tuple_size());
        _retired_buffer = v->data; _retired_buffer_len = v->data_len;
        v->data = nullptr; v->data_len = 0; v->base_backed = true;
    }
}

void Row_sdmvcc::cancel_gc_locked(SDMVCCEntry *v) {
    if (v != &_sentinel) _gc_candidates.erase(&v->gcIndex);
}

// O(1) eligibility; only this pair is examined. Index removal/deferred
// insertion are O(log V), never a sweep through unrelated versions/readers.
void Row_sdmvcc::check_gc_locked(SDMVCCEntry *cur, uint64_t watermark) {
    // return;
    g_gc_calls.fetch_add(1, std::memory_order_relaxed);
#if !SDMVCC_INTENT_GC || SDMVCC_LAZY_READ_INTENT
    g_gc_disabled_calls.fetch_add(1, std::memory_order_relaxed);
    return;
#endif
    if (cur == &_sentinel) return;
    cancel_gc_locked(cur);
    SDMVCCEntry *next = cur->nextWrite;
    g_gc_local_checks.fetch_add(1, std::memory_order_relaxed);
    if (next == &_sentinel || !cur->ready || !next->ready ||
        cur->nextAll != next) return;
    watermark = std::min(watermark, oldest_pinned_snapshot());
    if (watermark <= next->sid || long_guard_covers(_row, cur->sid, next->sid)) {
        // Keep the established conservative watermark convention (W > next).
        cur->gcIndex.key = next->sid + 1;
        _gc_candidates.insert(&cur->gcIndex);
        return;
    }
    assert(!cur->waitHead);
    _version_index.erase(&cur->versionIndex);
    cur->prevWrite->nextWrite = next;
    next->prevWrite = cur->prevWrite;
    unlink_all_locked(cur);
    retire_locked(cur);
    --_version_cnt;
    g_live_versions.fetch_sub(1, std::memory_order_relaxed);
    g_versions_reclaimed.fetch_add(1, std::memory_order_relaxed);
}

// One intrusive, stable row handle per queue. No queued raw version pointers:
// a version can be removed safely while a polling scheduler is in flight.
void Row_sdmvcc::update_deferred_locked(bool force) {
#if SDMVCC_INTENT_GC && !SDMVCC_LAZY_READ_INTENT
    auto first = _gc_candidates.first();
    const uint64_t threshold = first ? first->key : UINT64_MAX;
    // Most row events have no deferred work; do not acquire a shared queue
    // latch on that path. The polling owner forces a refresh after popping.
    if (!force && threshold == _deferred_threshold) return;
    _deferred_threshold = threshold;
    DeferredShard &q = g_deferred[row_shard(this)];
    pthread_mutex_lock(&q.latch);
    if ((!first && _deferred.node.linked) ||
        (first && (!_deferred.node.linked || _deferred.node.key != first->key))) {
        q.rows.erase(&_deferred.node);
        if (first) {
            _deferred.node.key = first->key;
            q.rows.insert(&_deferred.node);
        }
        auto head = q.rows.first();
        q.first_threshold.store(head ? head->key : UINT64_MAX,
                                std::memory_order_release);
    }
    pthread_mutex_unlock(&q.latch);
#endif
}

void Row_sdmvcc::process_deferred_locked(uint64_t watermark, unsigned budget) {
    const uint64_t effective = std::min(watermark, oldest_pinned_snapshot());
    while (budget--) {
        auto node = _gc_candidates.first();
        if (!node || node->key > effective) break;
        SDMVCCEntry *v = from_gc_index(node);
        _gc_candidates.erase(node);
        g_gc_candidates_processed.fetch_add(1, std::memory_order_relaxed);
        check_gc_locked(v, watermark);
        // A live long guard can still protect an otherwise eligible pair.
        // Do not spin on it in this poll. A later guard release/poll retries.
        if (v->gcIndex.linked) break;
    }
}

void Row_sdmvcc::poll_gc(uint64_t watermark, uint64_t shard) {
#if SDMVCC_INTENT_GC && !SDMVCC_LAZY_READ_INTENT
    DeferredShard &q = g_deferred[shard % kGcShards];
    const uint64_t effective = std::min(watermark, oldest_pinned_snapshot());
    if (q.first_threshold.load(std::memory_order_acquire) > effective) return;
    pthread_mutex_lock(&q.latch);
    auto node = q.rows.first();
    Row_sdmvcc *row = nullptr;
    if (node && node->key <= effective) {
        auto item = reinterpret_cast<SDMVCCDeferredRow *>(
            reinterpret_cast<char *>(node) - offsetof(SDMVCCDeferredRow, node));
        row = item->row;
        q.rows.erase(node);
        auto head = q.rows.first();
        q.first_threshold.store(head ? head->key : UINT64_MAX,
                                std::memory_order_release);
    }
    pthread_mutex_unlock(&q.latch);
    // Never acquire a row latch while holding a queue latch.
    if (row) {
        pthread_mutex_lock(&row->_latch);
        row->process_deferred_locked(watermark, 16);
        row->update_deferred_locked(true);
        row->promote_locked();
        row->unlock_and_dispose();
    }
#endif
}

void Row_sdmvcc::release_intent(SDMVCCEntry *intent, uint64_t watermark) {
    if (!intent) return;
    pthread_mutex_lock(&_latch);
    assert(!intent->armed);
    SDMVCCEntry *pred = intent->myVersion ? intent->myVersion :
        predecessor_locked(intent->sid);
    unlink_all_locked(intent);
    const bool closed_gap = pred != &_sentinel && pred->nextAll == pred->nextWrite;
    g_active_read_intents.fetch_sub(1, std::memory_order_relaxed);
    g_intents_released.fetch_add(1, std::memory_order_relaxed);
    retire_locked(intent);
    if (closed_gap) check_gc_locked(pred, watermark);
    update_deferred_locked();
    promote_locked();
    unlock_and_dispose();
}

void Row_sdmvcc::long_read_guard_released(uint64_t watermark) {
    pthread_mutex_lock(&_latch);
    process_deferred_locked(watermark, 16);
    update_deferred_locked();
    promote_locked();
    unlock_and_dispose();
}

SDMVCCLongReadGuard *Row_sdmvcc::begin_long_read_guard(uint64_t snapshot) {
    assert(SDMVCC_LONG_READ_GUARD_BITS >= 64);
    assert(SDMVCC_LONG_READ_GUARD_HASHES > 0);
    SDMVCCLongReadGuard *guard = new SDMVCCLongReadGuard();
    guard->snapshot = snapshot;
    guard->building = true;
    guard->key_count = 0;
    guard->build_start_ns = get_sys_clock();
    guard->bloom.assign((SDMVCC_LONG_READ_GUARD_BITS + 63) / 64, 0);
    pthread_rwlock_wrlock(&g_long_guard_latch);
    g_long_guards.push_back(guard);
    pthread_rwlock_unlock(&g_long_guard_latch);
    const uint64_t active = g_long_guard_active.fetch_add(1) + 1;
    uint64_t peak = g_long_guard_peak_active.load();
    while (active > peak &&
           !g_long_guard_peak_active.compare_exchange_weak(peak, active)) {}
    g_long_guards_registered.fetch_add(1, std::memory_order_relaxed);
    return guard;
}

void Row_sdmvcc::add_long_read_guard_key(SDMVCCLongReadGuard *guard,
                                         row_t *row) {
    long_guard_add(guard, row);
}

void Row_sdmvcc::finalize_long_read_guard(SDMVCCLongReadGuard *guard) {
    assert(guard != NULL);
    pthread_rwlock_wrlock(&g_long_guard_latch);
    assert(guard->building);
    guard->building = false;
    pthread_rwlock_unlock(&g_long_guard_latch);
    g_long_guard_keys.fetch_add(guard->key_count, std::memory_order_relaxed);
    g_long_guard_build_ns.fetch_add(get_sys_clock() - guard->build_start_ns,
                                    std::memory_order_relaxed);
}

void Row_sdmvcc::remove_long_read_guard(SDMVCCLongReadGuard *guard) {
    if (guard == NULL) return;
    pthread_rwlock_wrlock(&g_long_guard_latch);
    auto it = std::find(g_long_guards.begin(), g_long_guards.end(), guard);
    assert(it != g_long_guards.end());
    g_long_guards.erase(it);
    pthread_rwlock_unlock(&g_long_guard_latch);
    g_long_guard_active.fetch_sub(1);
    g_long_guards_released.fetch_add(1, std::memory_order_relaxed);
    delete guard;
}

void Row_sdmvcc::pin_snapshot(uint64_t snapshot) {
    pthread_mutex_lock(&g_snapshot_pin_latch);
    g_snapshot_pins.insert(snapshot);
    g_oldest_pinned_snapshot.store(*g_snapshot_pins.begin(),
                                   std::memory_order_release);
    pthread_mutex_unlock(&g_snapshot_pin_latch);
}

void Row_sdmvcc::unpin_snapshot(uint64_t snapshot) {
    pthread_mutex_lock(&g_snapshot_pin_latch);
    auto it = g_snapshot_pins.find(snapshot);
    assert(it != g_snapshot_pins.end());
    g_snapshot_pins.erase(it);
    const uint64_t oldest = g_snapshot_pins.empty()
        ? UINT64_MAX : *g_snapshot_pins.begin();
    g_oldest_pinned_snapshot.store(oldest, std::memory_order_release);
    pthread_mutex_unlock(&g_snapshot_pin_latch);
}

uint64_t Row_sdmvcc::oldest_pinned_snapshot() {
    return g_oldest_pinned_snapshot.load(std::memory_order_acquire);
}

bool Row_sdmvcc::long_guard_covers(row_t *row, uint64_t current_sid,
                                   uint64_t next_sid) {
#if !SDMVCC_LONG_READ_GUARD
    // Keep the disabled side of the ablation identical to eager intent GC:
    // no process-wide registry probe or rwlock acquisition on the GC path.
    (void)row;
    (void)current_sid;
    (void)next_sid;
    return false;
#else
    // Almost every GC call runs while there is no L1 transaction.  The
    // scheduler installs and increments the guard before it can advance the
    // registration frontier, so an empty fast path is safe: the watermark
    // still protects the snapshot during guard construction.
    if (g_long_guard_active.load(std::memory_order_acquire) == 0) return false;
    bool covered = false;
    pthread_rwlock_rdlock(&g_long_guard_latch);
    for (SDMVCCLongReadGuard *guard : g_long_guards) {
        // Visibility is strict: predecessor(snapshot) is the greatest version
        // with sid < snapshot, hence current is selected for
        // current.sid < snapshot <= next.sid.
        if (guard->snapshot <= current_sid || guard->snapshot > next_sid)
            continue;
        g_long_guard_gc_probes.fetch_add(1, std::memory_order_relaxed);
        if (guard->building || long_guard_maybe_contains(guard, row)) {
            covered = true;
            g_long_guard_gc_protected.fetch_add(1, std::memory_order_relaxed);
            break;
        }
    }
    pthread_rwlock_unlock(&g_long_guard_latch);
    return covered;
#endif
}

void Row_sdmvcc::print_stats(FILE *outf) {
    update_peak(g_peak_active_read_intents, g_active_read_intents.load());
    fprintf(outf, ",sdmvcc_gc_local_checks=%lu,sdmvcc_gc_candidates_processed=%lu",
        g_gc_local_checks.load(), g_gc_candidates_processed.load());
    fprintf(outf,
            ",sdmvcc_intents_registered=%lu,sdmvcc_intents_released=%lu"
            ",sdmvcc_intent_waits=%lu,sdmvcc_notifications=%lu"
            ",sdmvcc_versions_created=%lu,sdmvcc_versions_reclaimed=%lu"
            ",sdmvcc_early_publish_enabled=%d"
            ",sdmvcc_early_versions_published=%lu"
            ",sdmvcc_version_bytes=%lu,sdmvcc_intent_gc=%d"
            ",sdmvcc_gc_calls=%lu,sdmvcc_gc_disabled_calls=%lu",
            g_intents_registered.load(), g_intents_released.load(),
            g_intent_waits.load(), g_intent_notifications.load(),
            g_versions_created.load(), g_versions_reclaimed.load(),
            SDMVCC_EARLY_VERSION_PUBLISH ? 1 : 0,
            SDMVCC_EARLY_VERSION_PUBLISH ? g_versions_created.load() : 0,
            g_version_bytes.load(), SDMVCC_INTENT_GC ? 1 : 0,
            g_gc_calls.load(), g_gc_disabled_calls.load());
    fprintf(outf,
            ",sdmvcc_lazy_read_intent=%d,sdmvcc_lazy_registration_skips=%lu"
            ",sdmvcc_lazy_ready_reads=%lu,sdmvcc_lazy_waits=%lu"
            ",sdmvcc_long_guard_enabled=%d,sdmvcc_long_guards_registered=%lu"
            ",sdmvcc_long_guards_released=%lu,sdmvcc_long_guard_keys=%lu"
            ",sdmvcc_long_guard_avg_build_ns=%f"
            ",sdmvcc_long_guard_gc_probes=%lu"
            ",sdmvcc_long_guard_gc_protected=%lu"
            ",sdmvcc_long_guard_peak_active=%lu"
            ",sdmvcc_long_guard_peak_metadata_bytes=%lu"
            ",sdmvcc_unsafe_l1_no_intent=%d"
            ",sdmvcc_unsafe_intent_skips=%lu"
            ",sdmvcc_unsafe_reads=%lu"
            ",sdmvcc_unsafe_fallback_reads=%lu"
            ",sdmvcc_blind_write_enabled=%d"
            ",sdmvcc_blind_writes_registered=%lu"
            ",sdmvcc_active_intents=%lu"
            ",sdmvcc_peak_active_intents=%lu"
            ",sdmvcc_live_versions=%lu"
            ",sdmvcc_tracked_rows=%lu"
            ",sdmvcc_avg_version_chain=%f"
            ",sdmvcc_peak_version_chain=%lu",
            SDMVCC_LAZY_READ_INTENT ? 1 : 0,
            g_lazy_registration_skips.load(), g_lazy_ready_reads.load(),
            g_lazy_waits.load(), SDMVCC_LONG_READ_GUARD ? 1 : 0,
            g_long_guards_registered.load(), g_long_guards_released.load(),
            g_long_guard_keys.load(),
            g_long_guards_registered.load() == 0 ? 0.0 :
                static_cast<double>(g_long_guard_build_ns.load()) /
                g_long_guards_registered.load(),
            g_long_guard_gc_probes.load(),
            g_long_guard_gc_protected.load(),
            g_long_guard_peak_active.load(),
            g_long_guard_peak_active.load() *
                ((SDMVCC_LONG_READ_GUARD_BITS + 7) / 8),
            SDMVCC_UNSAFE_L1_NO_INTENT ? 1 : 0,
            g_unsafe_intent_skips.load(), g_unsafe_reads.load(),
            g_unsafe_fallback_reads.load(), SDMVCC_BLIND_WRITE ? 1 : 0,
            g_blind_writes_registered.load(),
            g_active_read_intents.load(std::memory_order_relaxed),
            g_peak_active_read_intents.load(std::memory_order_relaxed),
            g_live_versions.load(std::memory_order_relaxed),
            g_tracked_rows.load(std::memory_order_relaxed),
            g_tracked_rows.load(std::memory_order_relaxed) ?
                static_cast<double>(g_live_versions.load(std::memory_order_relaxed)) /
                g_tracked_rows.load(std::memory_order_relaxed) : 0.0,
            g_peak_version_chain.load(std::memory_order_relaxed));
}

void Row_sdmvcc::print_timeseries(FILE *outf, uint64_t elapsed_ns) {
    update_peak(g_peak_active_read_intents, g_active_read_intents.load());
    const uint64_t tracked = g_tracked_rows.load(std::memory_order_relaxed);
    const uint64_t live = g_live_versions.load(std::memory_order_relaxed);
    fprintf(outf,
            "[timeseries] elapsed_ns=%lu"
            ",sdmvcc_active_intents=%lu"
            ",sdmvcc_peak_active_intents=%lu"
            ",sdmvcc_live_versions=%lu"
            ",sdmvcc_tracked_rows=%lu"
            ",sdmvcc_avg_version_chain=%f"
            ",sdmvcc_peak_version_chain=%lu\n",
            elapsed_ns,
            g_active_read_intents.load(std::memory_order_relaxed),
            g_peak_active_read_intents.load(std::memory_order_relaxed),
            live, tracked,
            tracked ? static_cast<double>(live) / tracked : 0.0,
            g_peak_version_chain.load(std::memory_order_relaxed));
    fflush(outf);
}

#endif
