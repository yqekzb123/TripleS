#include "row_sdmvcc.h"

#include "catalog.h"
#include "helper.h"
#include "row.h"
#include "txn.h"
#include "txn_table.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>

#if CC_ALG == SDMVCC

namespace {
std::atomic<uint64_t> g_intents_registered(0);
std::atomic<uint64_t> g_intents_released(0);
std::atomic<uint64_t> g_intent_waits(0);
std::atomic<uint64_t> g_intent_notifications(0);
std::atomic<uint64_t> g_versions_created(0);
std::atomic<uint64_t> g_versions_reclaimed(0);
std::atomic<uint64_t> g_version_bytes(0);
std::atomic<uint64_t> g_gc_calls(0);
std::atomic<uint64_t> g_gc_disabled_calls(0);
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
pthread_mutex_t g_snapshot_pin_latch = PTHREAD_MUTEX_INITIALIZER;
std::multiset<uint64_t> g_snapshot_pins;
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
    ++guard->key_count;
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
}

Row_sdmvcc::Row_sdmvcc() : _row(NULL), _initial_copied(false) {
    pthread_mutex_init(&_latch, NULL);
}

void Row_sdmvcc::init(row_t *row) {
    _row = row;
    _versions.emplace_back(0, true, true);
}

void Row_sdmvcc::ensure_initial_locked() {
    if (_initial_copied) return;
    // The base row is never updated by SDMVCC; it is the immutable version 0.
    // Avoid duplicating every database row merely because it was first read.
    _initial_copied = true;
}

std::list<Row_sdmvcc::Version>::iterator
Row_sdmvcc::find_version_locked(uint64_t sid) {
    for (auto it = _versions.begin(); it != _versions.end(); ++it) {
        if (it->sid == sid) return it;
        if (it->sid > sid) break;
    }
    return _versions.end();
}

std::list<Row_sdmvcc::Version>::iterator
Row_sdmvcc::predecessor_locked(uint64_t snapshot) {
    auto result = _versions.end();
    for (auto it = _versions.begin(); it != _versions.end(); ++it) {
        if (it->sid >= snapshot) break;
        result = it;
    }
    return result;
}

// Scheduling path:
//   workload acquire_locks() -> row_t::get_lock() -> register_access().
// The transaction-level index deduplicates repeated accesses to this key;
// remaining_uses still records how many execution-time uses must be consumed.
RC Row_sdmvcc::register_access(access_t type, TxnManager *txn) {
    const uint64_t sid = txn->sdmvcc_snapshot();
    const int registration = txn->register_sdmvcc_access(_row, type);
    if (registration == 0) return RCOK;
    const bool long_guard = txn->uses_sdmvcc_long_read_guard();
    // A guarded read needs neither the row latch nor a per-key intent during
    // registration.  Writes still reserve their private version below.
    if (type != WR && long_guard) return RCOK;
    pthread_mutex_lock(&_latch);
    ensure_initial_locked();
    if (registration == 1 && !SDMVCC_LAZY_READ_INTENT && !long_guard) {
        _read_intents[sid]++;
        g_intents_registered.fetch_add(1, std::memory_order_relaxed);
    } else if (registration == 1) {
        g_lazy_registration_skips.fetch_add(1, std::memory_order_relaxed);
    }

    if (type == WR) {
        auto existing = find_version_locked(sid);
        if (existing == _versions.end()) {
            auto pos = _versions.begin();
            while (pos != _versions.end() && pos->sid < sid) ++pos;
            _versions.emplace(pos, sid, false);
            g_versions_created.fetch_add(1, std::memory_order_relaxed);
        }
    }
    pthread_mutex_unlock(&_latch);
    return RCOK;
}

// Admission path:
//   SDPCCLockThread -> TxnManager::arm_sdmvcc_intents() -> arm_read().
// The intent already protects visibility. Arming adds notification state only
// when the predecessor selected by snapshot ordering has not been published.
// Returning false means notification is pending, not that the txn aborts.
bool Row_sdmvcc::arm_read(TxnManager *txn, uint64_t snapshot) {
    pthread_mutex_lock(&_latch);
    auto version = predecessor_locked(snapshot);
    if (version == _versions.end()) {
        pthread_mutex_unlock(&_latch);
        return true;
    }
    if (version->ready) {
        pthread_mutex_unlock(&_latch);
        return true;
    }
    version->waiters.push_back(txn);
    txn->incr_lr();
    g_intent_waits.fetch_add(1, std::memory_order_relaxed);
    pthread_mutex_unlock(&_latch);
    return false;
}

// Lazy mode installs both pieces of temporary state only after an execution
// miss: the snapshot intent protects the selected predecessor from GC, while
// Version::waiters provides completion notification. Holding _latch across
// the readiness test and waiter insertion prevents a lost wakeup.
bool Row_sdmvcc::wait_for_predecessor_locked(TxnManager *txn,
                                             uint64_t snapshot) {
    if (!SDMVCC_LAZY_READ_INTENT && !txn->uses_sdmvcc_long_read_guard())
        return false;
    bool new_intent = false;
    if (!txn->arm_sdmvcc_lazy_access(_row, new_intent)) return true;
    if (new_intent) {
        _read_intents[snapshot]++;
        g_intents_registered.fetch_add(1, std::memory_order_relaxed);
    }
    auto version = predecessor_locked(snapshot);
    assert(version != _versions.end() && !version->ready);
    ATOM_CAS(txn->lock_ready, true, false);
    txn->incr_lr();
    version->waiters.push_back(txn);
    g_intent_waits.fetch_add(1, std::memory_order_relaxed);
    g_lazy_waits.fetch_add(1, std::memory_order_relaxed);
    return true;
}

RC Row_sdmvcc::read(TxnManager *txn, uint64_t snapshot, row_t *local_row) {
    pthread_mutex_lock(&_latch);
    auto version = predecessor_locked(snapshot);
    if (version == _versions.end()) {
        pthread_mutex_unlock(&_latch);
        return Abort;
    }
    if (!version->ready) {
        wait_for_predecessor_locked(txn, snapshot);
        pthread_mutex_unlock(&_latch);
        return WAIT;
    }
    if (SDMVCC_LAZY_READ_INTENT || txn->uses_sdmvcc_long_read_guard())
        g_lazy_ready_reads.fetch_add(1, std::memory_order_relaxed);
    if (version->base_backed) {
        memcpy(local_row->get_data(), _row->get_data(), _row->get_tuple_size());
    } else {
        assert(version->data.size() == _row->get_tuple_size());
        memcpy(local_row->get_data(), version->data.data(), _row->get_tuple_size());
    }
    pthread_mutex_unlock(&_latch);
    return RCOK;
}

RC Row_sdmvcc::read_value(TxnManager *txn, uint64_t snapshot, uint32_t column, void *value,
                          uint32_t size) {
    pthread_mutex_lock(&_latch);
    auto version = predecessor_locked(snapshot);
    if (version == _versions.end()) {
        pthread_mutex_unlock(&_latch);
        return Abort;
    }
    if (!version->ready) {
        wait_for_predecessor_locked(txn, snapshot);
        pthread_mutex_unlock(&_latch);
        return WAIT;
    }
    if (SDMVCC_LAZY_READ_INTENT || txn->uses_sdmvcc_long_read_guard())
        g_lazy_ready_reads.fetch_add(1, std::memory_order_relaxed);
    assert(size <= _row->get_schema()->get_field_size(column));
    const uint32_t offset = _row->get_schema()->get_field_index(column);
    const char *data = version->base_backed ? _row->get_data()
                                            : version->data.data();
    assert(version->base_backed ||
           version->data.size() == _row->get_tuple_size());
    memcpy(value, data + offset, size);
    pthread_mutex_unlock(&_latch);
    return RCOK;
}

bool Row_sdmvcc::visible(uint64_t snapshot) {
    pthread_mutex_lock(&_latch);
    const bool result = predecessor_locked(snapshot) != _versions.end();
    pthread_mutex_unlock(&_latch);
    return result;
}

void Row_sdmvcc::set_creation_sid(uint64_t sid) {
    pthread_mutex_lock(&_latch);
    assert(_versions.size() == 1);
    Version &initial = _versions.front();
    assert(initial.sid == 0 && initial.ready && initial.base_backed);
    initial.sid = sid;
    pthread_mutex_unlock(&_latch);
}

void Row_sdmvcc::stage_write(uint64_t sid, row_t *local_row) {
    pthread_mutex_lock(&_latch);
    auto version = find_version_locked(sid);
    assert(version != _versions.end());
    assert(!version->ready);
    if (version->data.empty()) {
        version->data.resize(_row->get_tuple_size());
        g_version_bytes.fetch_add(_row->get_tuple_size(), std::memory_order_relaxed);
    }
    memcpy(version->data.data(), local_row->get_data(), _row->get_tuple_size());
    pthread_mutex_unlock(&_latch);
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

void Row_sdmvcc::publish_write(uint64_t sid, uint64_t thd_id) {
    std::vector<TxnManager *> wake;
    pthread_mutex_lock(&_latch);
    auto version = find_version_locked(sid);
    assert(version != _versions.end());
    assert(!version->data.empty());
    version->ready = true;
    wake.swap(version->waiters);
    gc_locked(minSid);
    pthread_mutex_unlock(&_latch);
    for (TxnManager *txn : wake) notify_ready(txn, thd_id);
}

void Row_sdmvcc::abort_write(uint64_t sid, uint64_t thd_id) {
    std::vector<TxnManager *> wake;
    pthread_mutex_lock(&_latch);
    auto version = find_version_locked(sid);
    if (version != _versions.end()) {
        std::vector<TxnManager *> affected;
        affected.swap(version->waiters);
        if (!version->data.empty()) {
            g_version_bytes.fetch_sub(version->data.size(), std::memory_order_relaxed);
        }
        _versions.erase(version);
        for (TxnManager *txn : affected) {
            auto predecessor = predecessor_locked(txn->sdmvcc_snapshot());
            if (predecessor->ready) wake.push_back(txn);
            else predecessor->waiters.push_back(txn);
        }
    }
    pthread_mutex_unlock(&_latch);
    for (TxnManager *txn : wake) notify_ready(txn, thd_id);
}

// Reclaim version current when all three conditions hold:
//   1. current and its successor next are committed (ready);
//   2. the effective watermark has passed next.sid, so no future transaction
//      admitted in deterministic order can select current;
//   3. no registered read-intent snapshot lies in [current.sid, next.sid),
//      which is exactly the snapshot interval that still selects current.
// A globally pinned scan snapshot lowers the effective watermark and is a
// conservative safety net for accesses that cannot be enumerated per key.
void Row_sdmvcc::gc_locked(uint64_t watermark) {
    g_gc_calls.fetch_add(1, std::memory_order_relaxed);
#if !SDMVCC_INTENT_GC || SDMVCC_LAZY_READ_INTENT
    // Watermark-only reclamation is not a safe ablation: an admitted long
    // reader may still hold an older snapshot.  Keep every version instead.
    g_gc_disabled_calls.fetch_add(1, std::memory_order_relaxed);
    return;
#endif
    const uint64_t pinned = oldest_pinned_snapshot();
    if (pinned < watermark) watermark = pinned;
    if (_versions.size() < 2) return;
    auto current = _versions.begin();
    while (current != _versions.end()) {
        auto next = std::next(current);
        if (next == _versions.end()) break; // Always retain the newest version.
        if (!current->ready || !next->ready || watermark <= next->sid) {
            current = next;
            continue;
        }
        auto intent = _read_intents.lower_bound(current->sid);
        const bool covered = intent != _read_intents.end() && intent->first < next->sid;
        if (covered || long_read_guard_covers(_row, current->sid, next->sid)) {
            current = next;
            continue;
        }
        g_version_bytes.fetch_sub(current->data.size(), std::memory_order_relaxed);
        g_versions_reclaimed.fetch_add(1, std::memory_order_relaxed);
        current = _versions.erase(current);
    }

    // Once no old snapshot needs the former base value, promote the sole
    // surviving committed version into the base row. This prevents one full
    // tuple copy from remaining forever for every key that was ever updated.
    if (_versions.size() == 1) {
        Version &latest = _versions.front();
        if (latest.ready && !latest.base_backed && !latest.data.empty()) {
            memcpy(_row->get_data(), latest.data.data(), _row->get_tuple_size());
            g_version_bytes.fetch_sub(latest.data.size(), std::memory_order_relaxed);
            std::vector<char>().swap(latest.data);
            latest.base_backed = true;
        }
    }
}

void Row_sdmvcc::pin_snapshot(uint64_t snapshot) {
    pthread_mutex_lock(&g_snapshot_pin_latch);
    g_snapshot_pins.insert(snapshot);
    pthread_mutex_unlock(&g_snapshot_pin_latch);
}

void Row_sdmvcc::unpin_snapshot(uint64_t snapshot) {
    pthread_mutex_lock(&g_snapshot_pin_latch);
    auto it = g_snapshot_pins.find(snapshot);
    assert(it != g_snapshot_pins.end());
    g_snapshot_pins.erase(it);
    pthread_mutex_unlock(&g_snapshot_pin_latch);
}

uint64_t Row_sdmvcc::oldest_pinned_snapshot() {
    pthread_mutex_lock(&g_snapshot_pin_latch);
    const uint64_t result = g_snapshot_pins.empty() ? UINT64_MAX : *g_snapshot_pins.begin();
    pthread_mutex_unlock(&g_snapshot_pin_latch);
    return result;
}

void Row_sdmvcc::release_intent(uint64_t snapshot, uint64_t watermark) {
    pthread_mutex_lock(&_latch);
    auto it = _read_intents.find(snapshot);
    assert(it != _read_intents.end() && it->second > 0);
    if (--it->second == 0) _read_intents.erase(it);
    g_intents_released.fetch_add(1, std::memory_order_relaxed);
    gc_locked(watermark);
    pthread_mutex_unlock(&_latch);
}

void Row_sdmvcc::long_read_guard_released(uint64_t watermark) {
    pthread_mutex_lock(&_latch);
    gc_locked(watermark);
    pthread_mutex_unlock(&_latch);
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
    g_long_guard_active.fetch_sub(1, std::memory_order_relaxed);
    g_long_guards_released.fetch_add(1, std::memory_order_relaxed);
    delete guard;
}

bool Row_sdmvcc::long_read_guard_covers(row_t *row, uint64_t current_sid,
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
    fprintf(outf,
            ",sdmvcc_intents_registered=%lu,sdmvcc_intents_released=%lu"
            ",sdmvcc_intent_waits=%lu,sdmvcc_notifications=%lu"
            ",sdmvcc_versions_created=%lu,sdmvcc_versions_reclaimed=%lu"
            ",sdmvcc_version_bytes=%lu,sdmvcc_intent_gc=%d"
            ",sdmvcc_gc_calls=%lu,sdmvcc_gc_disabled_calls=%lu",
            g_intents_registered.load(), g_intents_released.load(),
            g_intent_waits.load(), g_intent_notifications.load(),
            g_versions_created.load(), g_versions_reclaimed.load(),
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
            ",sdmvcc_long_guard_peak_metadata_bytes=%lu",
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
                ((SDMVCC_LONG_READ_GUARD_BITS + 7) / 8));
}

#endif
