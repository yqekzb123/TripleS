#ifndef _SDPCC_LONG_HOLE_H_
#define _SDPCC_LONG_HOLE_H_

#include <atomic>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <unordered_set>
#include <vector>

class TxnManager;

class SDPCCLongHoleManager {
public:
    SDPCCLongHoleManager();
    ~SDPCCLongHoleManager();

    void init(uint64_t scheduler_cnt);
    bool enabled() const;
    bool is_long_txn(TxnManager *txn) const;
    bool should_publish(uint64_t scheduler_id, TxnManager *txn);
    void publish(uint64_t scheduler_id, uint64_t key, TxnManager *txn);
    void clear(uint64_t scheduler_id, uint64_t key);
    void advance_normal(uint64_t scheduler_id, uint64_t key);
    bool should_check(uint64_t scheduler_id, uint64_t key);
    bool can_bypass(uint64_t scheduler_id, TxnManager *txn, uint64_t key);
    void record_watermark_wait(uint64_t scheduler_id, uint64_t wait_ns);
    void print(FILE *outf) const;

private:
    struct BloomSet {
        std::vector<uint64_t> words;
        void init();
        void add(uint64_t key);
        bool may_contain(uint64_t key) const;
    };

    struct Hole {
        Hole() : active(false), key(0), local_key_count(0) {}
        bool active;
        uint64_t key;
        uint64_t local_key_count;
        std::unordered_set<uint64_t> reads;
        std::unordered_set<uint64_t> writes;
        BloomSet read_bloom;
        BloomSet write_bloom;
    };

    void build_hole(Hole &hole, TxnManager *txn) const;
    bool conflicts(const Hole &hole, TxnManager *txn, uint64_t &probes) const;
    uint64_t metadata_bytes(const Hole &hole) const;
    static uint64_t mix64(uint64_t value);
    static void update_peak(std::atomic<uint64_t> &peak, uint64_t value);

    uint64_t scheduler_cnt_;
    std::atomic<uint64_t> *frontiers_;
    mutable pthread_rwlock_t latch_;
    std::vector<Hole> holes_;
    uint64_t active_holes_;
    std::atomic<uint64_t> adaptive_enabled_schedulers_;
    uint64_t metadata_bytes_;

    // Each scheduler owns a 128-byte counter slot. The large stride keeps hot
    // counters written by different schedulers off the same cache line.
    static const uint64_t ADAPTIVE_COUNTER_STRIDE = 32;
    enum AdaptiveCounterOffset {
        ADAPTIVE_SKIPPED_NO_HOLE = 0,
        ADAPTIVE_SKIPPED_INFEASIBLE = 1,
        ADAPTIVE_ELIGIBLE = 2,
        COUNTER_PUBLISHED = 3,
        COUNTER_BYPASS_CHECKS = 4,
        COUNTER_BYPASSED = 5,
        COUNTER_REJECTED_NON_HOLE = 6,
        COUNTER_REJECTED_CONFLICT = 7,
        COUNTER_MATCH_PROBES = 8,
        COUNTER_BUILD_TIME_NS = 9,
        COUNTER_MATCH_TIME_NS = 10,
        COUNTER_WATERMARK_WAIT_COUNT = 11,
        COUNTER_WATERMARK_WAIT_NS = 12,
        ADAPTIVE_WINDOW_TXNS = 13,
        ADAPTIVE_WINDOW_LONG_TXNS = 14,
        ADAPTIVE_ENABLED = 15,
        ADAPTIVE_ENABLE_TRANSITIONS = 16,
        ADAPTIVE_DISABLE_TRANSITIONS = 17
    };
    std::vector<uint64_t> adaptive_counters_;
    std::atomic<uint64_t> peak_active_holes_;
    std::atomic<uint64_t> peak_metadata_bytes_;
};

#endif
