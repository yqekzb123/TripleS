#include "row_sdmvcc.h"

#include "catalog.h"
#include "helper.h"
#include "row.h"
#include "txn.h"
#include "txn_table.h"

#include <algorithm>
#include <atomic>
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
pthread_mutex_t g_snapshot_pin_latch = PTHREAD_MUTEX_INITIALIZER;
std::multiset<uint64_t> g_snapshot_pins;
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

RC Row_sdmvcc::register_access(access_t type, TxnManager *txn) {
    const uint64_t sid = txn->sdmvcc_snapshot();
    const int registration = txn->register_sdmvcc_access(_row, type);
    if (registration == 0) return RCOK;
    pthread_mutex_lock(&_latch);
    ensure_initial_locked();
    if (registration == 1) {
        _read_intents[sid]++;
        g_intents_registered.fetch_add(1, std::memory_order_relaxed);
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

RC Row_sdmvcc::read(uint64_t snapshot, row_t *local_row) {
    pthread_mutex_lock(&_latch);
    auto version = predecessor_locked(snapshot);
    if (version == _versions.end()) {
        pthread_mutex_unlock(&_latch);
        return Abort;
    }
    assert(version->ready);
    if (version->base_backed) {
        memcpy(local_row->get_data(), _row->get_data(), _row->get_tuple_size());
    } else {
        assert(version->data.size() == _row->get_tuple_size());
        memcpy(local_row->get_data(), version->data.data(), _row->get_tuple_size());
    }
    pthread_mutex_unlock(&_latch);
    return RCOK;
}

RC Row_sdmvcc::read_value(uint64_t snapshot, uint32_t column, void *value,
                          uint32_t size) {
    pthread_mutex_lock(&_latch);
    auto version = predecessor_locked(snapshot);
    if (version == _versions.end()) {
        pthread_mutex_unlock(&_latch);
        return Abort;
    }
    assert(version->ready);
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

void Row_sdmvcc::gc_locked(uint64_t watermark) {
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
        if (covered) {
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

void Row_sdmvcc::print_stats(FILE *outf) {
    fprintf(outf,
            ",sdmvcc_intents_registered=%lu,sdmvcc_intents_released=%lu"
            ",sdmvcc_intent_waits=%lu,sdmvcc_notifications=%lu"
            ",sdmvcc_versions_created=%lu,sdmvcc_versions_reclaimed=%lu"
            ",sdmvcc_version_bytes=%lu",
            g_intents_registered.load(), g_intents_released.load(),
            g_intent_waits.load(), g_intent_notifications.load(),
            g_versions_created.load(), g_versions_reclaimed.load(),
            g_version_bytes.load());
}

#endif
