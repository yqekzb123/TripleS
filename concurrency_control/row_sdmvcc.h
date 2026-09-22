#ifndef _ROW_SDMVCC_H_
#define _ROW_SDMVCC_H_

#include "global.h"
#include <vector>
#include "sdmvcc_index.h"

class row_t;
class TxnManager;

// Transaction-level protection for a large deterministic read set.  While a
// guard is BUILDING, its snapshot conservatively protects every row; after
// finalize_long_read_guard(), the Bloom filter limits protection to rows that
// the transaction registered locally.  False positives only retain an extra
// version and therefore cannot violate visibility.
struct SDMVCCLongReadGuard {
    uint64_t snapshot;
    bool building;
    uint64_t key_count;
    uint64_t build_start_ns;
    std::vector<uint64_t> bloom;
};

// Sorted all-entry chain, with R(S) preceding V(S) for read-modify-write.
// The version index locates a write gap; only that gap is searched for reads.
// myVersion is resolved at Arm (after admission), never eagerly retargeted
// during out-of-order registration. Waiters have a separate intrusive list.
class Row_sdmvcc;
struct SDMVCCDeferredRow {
    SDMVCCIndexNode node;
    Row_sdmvcc *row;
};

struct SDMVCCEntry {
    bool isVersion;
    uint64_t sid;
    // all-entry chain (versions + intents), ascending SID
    SDMVCCEntry *prevAll;
    SDMVCCEntry *nextAll;
    // version only: write-only chain, ascending SID
    SDMVCCEntry *prevWrite;
    SDMVCCEntry *nextWrite;
    // version only: committed (C) vs pending (P)
    bool ready;
    bool base_backed;
    char *data;        // private tuple copy; null until stage_write()
    uint32_t data_len;
    // intent only
    TxnManager *txn;   // null for versions
    SDMVCCEntry *myVersion; // version this intent currently selects
    bool armed;        // waiting for myVersion to publish
    bool ephemeral;    // waiter-only node; never owned by a transaction handle
    SDMVCCEntry *waitHead, *waitNext, *retiredNext;
    SDMVCCIndexNode versionIndex, gcIndex;
};

class Row_sdmvcc {
public:
    Row_sdmvcc();
    void init(row_t *row);

    RC register_access(access_t type, TxnManager *txn);
    bool arm_read(TxnManager *txn, uint64_t snapshot);
    RC read(TxnManager *txn, uint64_t snapshot, row_t *local_row);
    RC read_latest(row_t *local_row);
    RC read_value(TxnManager *txn, uint64_t snapshot, uint32_t column, void *value,
                  uint32_t size);
    bool visible(uint64_t snapshot);
    void set_creation_sid(uint64_t sid);
    void stage_write(uint64_t sid, row_t *local_row, SDMVCCEntry *handle = nullptr);
    void publish_write(uint64_t sid, uint64_t thd_id, bool early = false, SDMVCCEntry *handle = nullptr);
    void abort_write(uint64_t sid, uint64_t thd_id, SDMVCCEntry *handle = nullptr);
    void release_intent(SDMVCCEntry *intent, uint64_t watermark);
    void long_read_guard_released(uint64_t watermark);
    bool has_write_lock() const { return false; }

    static void poll_gc(uint64_t watermark, uint64_t shard);
    static void print_stats(FILE *outf);
    static void print_timeseries(FILE *outf, uint64_t elapsed_ns);
    static void pin_snapshot(uint64_t snapshot);
    static void unpin_snapshot(uint64_t snapshot);
    static SDMVCCLongReadGuard *begin_long_read_guard(uint64_t snapshot);
    static void add_long_read_guard_key(SDMVCCLongReadGuard *guard, row_t *row);
    static void finalize_long_read_guard(SDMVCCLongReadGuard *guard);
    static void remove_long_read_guard(SDMVCCLongReadGuard *guard);

private:
    row_t *_row;
    pthread_mutex_t _latch;
    // Fixed tail sentinel of both chains: isVersion, sid = UINT64_MAX,
    // ready. Circular linking keeps every insert/delete a pointer splice and
    // removes all empty-chain special cases.
    SDMVCCEntry _sentinel;
    uint32_t _version_cnt;
    uint64_t _creation_sid;
    SDMVCCIndex _version_index, _gc_candidates;
    SDMVCCDeferredRow _deferred;
    uint64_t _deferred_threshold;
    SDMVCCEntry *_retired;
    char *_retired_buffer;
    uint32_t _retired_buffer_len;


    void validate_locked(const char *where);
    SDMVCCEntry *find_version_locked(uint64_t sid);
    SDMVCCEntry *predecessor_locked(uint64_t snapshot);
    void insert_intent_locked(SDMVCCEntry *intent, SDMVCCEntry *myVersion);
    SDMVCCEntry *insert_version_locked(SDMVCCEntry *version);
    void unlink_all_locked(SDMVCCEntry *entry);
    bool wait_for_predecessor_locked(TxnManager *txn, SDMVCCEntry *version,
                                     SDMVCCEntry *&spare);
    SDMVCCEntry *read_version_locked(TxnManager *txn, uint64_t snapshot);
    void check_gc_locked(SDMVCCEntry *version, uint64_t watermark);
    void cancel_gc_locked(SDMVCCEntry *version);
    void update_deferred_locked(bool force = false);
    void process_deferred_locked(uint64_t watermark, unsigned budget);
    void unlock_and_dispose();
    void retire_locked(SDMVCCEntry *entry);
    void promote_locked();
    static void dispatch_waiters(SDMVCCEntry *head, uint64_t thd_id);
    static void notify_ready(TxnManager *txn, uint64_t thd_id);
    static uint64_t oldest_pinned_snapshot();
    static bool long_guard_covers(row_t *row, uint64_t current_sid,
                                  uint64_t next_sid);
};

#endif
