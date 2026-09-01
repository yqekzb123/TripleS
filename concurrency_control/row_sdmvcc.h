#ifndef _ROW_SDMVCC_H_
#define _ROW_SDMVCC_H_

#include "global.h"
#include <list>
#include <map>
#include <set>
#include <vector>

class row_t;
class TxnManager;

// Per-key deterministic MVCC state. Read intents are snapshot timestamps,
// not pointers to versions, so an intent remains valid while older write
// reservations are still arriving out of scheduler order.
//
// SDMVCC lifecycle / call stack:
//
// (1) Deterministic scheduling and metadata registration
//   SDPCCLockThread::run()
//     -> <workload>TxnManager::acquire_locks()
//     -> row_t::get_lock()
//     -> Row_sdmvcc::register_access()
//   register_access() records one per-key read intent and reserves an
//   uncommitted version for a local write. It does not block the scheduler.
//
// (2) Watermark admission and version readiness
//   SDPCCLockThread::{run(), handle_tmp_txn()}
//     -> TxnManager::arm_sdmvcc_intents()
//     -> Row_sdmvcc::arm_read()
//   A transaction whose visible predecessor is unfinished becomes a waiter
//   on that Version. Each unresolved key contributes one lock-ready count.
//
// (3) Execution and optional early intent release
//   TxnManager::get_row() -> row_t::get_row() -> Row_sdmvcc::read()
//   Workloads that know a key will not be read again may then call
//     TxnManager::consume_sdmvcc_access()
//     -> Row_sdmvcc::release_intent()
//
// (4) Commit/abort, notification, and GC
//   TxnManager::cleanup() -> TxnManager::finish_sdmvcc()
//     -> stage_write() for every local write
//     -> publish_write() (or abort_write())
//     -> notify_ready() for transactions waiting on the affected version
//   publish_write() and release_intent() both call gc_locked().
//
// Visibility rule: snapshot S reads the greatest committed version V with
// V.sid < S. _read_intents protects versions needed by active snapshots;
// Version::waiters is separate metadata used only for readiness notification.
class Row_sdmvcc {
public:
    Row_sdmvcc();
    void init(row_t *row);

    RC register_access(access_t type, TxnManager *txn);
    bool arm_read(TxnManager *txn, uint64_t snapshot);
    RC read(TxnManager *txn, uint64_t snapshot, row_t *local_row);
    RC read_value(TxnManager *txn, uint64_t snapshot, uint32_t column, void *value,
                  uint32_t size);
    bool visible(uint64_t snapshot);
    void set_creation_sid(uint64_t sid);
    void stage_write(uint64_t sid, row_t *local_row);
    void publish_write(uint64_t sid, uint64_t thd_id);
    void abort_write(uint64_t sid, uint64_t thd_id);
    void release_intent(uint64_t snapshot, uint64_t watermark);
    bool has_write_lock() const { return false; }

    static void print_stats(FILE *outf);
    static void pin_snapshot(uint64_t snapshot);
    static void unpin_snapshot(uint64_t snapshot);

private:
    struct Version {
        uint64_t sid;
        bool ready;
        bool base_backed;
        std::vector<char> data;
        std::vector<TxnManager *> waiters;
        Version(uint64_t version_sid, bool is_ready, bool uses_base = false)
            : sid(version_sid), ready(is_ready), base_backed(uses_base) {}
    };

    row_t *_row;
    pthread_mutex_t _latch;
    bool _initial_copied;
    std::list<Version> _versions;
    std::map<uint64_t, uint32_t> _read_intents;

    void ensure_initial_locked();
    std::list<Version>::iterator find_version_locked(uint64_t sid);
    std::list<Version>::iterator predecessor_locked(uint64_t snapshot);
    bool wait_for_predecessor_locked(TxnManager *txn, uint64_t snapshot);
    void gc_locked(uint64_t watermark);
    static void notify_ready(TxnManager *txn, uint64_t thd_id);
    static uint64_t oldest_pinned_snapshot();
};

#endif
