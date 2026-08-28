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
class Row_sdmvcc {
public:
    Row_sdmvcc();
    void init(row_t *row);

    RC register_access(access_t type, TxnManager *txn);
    bool arm_read(TxnManager *txn, uint64_t snapshot);
    RC read(uint64_t snapshot, row_t *local_row);
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
    void gc_locked(uint64_t watermark);
    static void notify_ready(TxnManager *txn, uint64_t thd_id);
    static uint64_t oldest_pinned_snapshot();
};

#endif
