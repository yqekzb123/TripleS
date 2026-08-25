#include "sdpcc_long_hole.h"

#include <assert.h>
#include <utility>

#include "global.h"
#include "helper.h"
#include "txn.h"
#include "ycsb.h"
#include "ycsb_query.h"

#if CC_ALG == SDPCC

SDPCCLongHoleManager::SDPCCLongHoleManager()
    : scheduler_cnt_(0), frontiers_(NULL), active_holes_(0), adaptive_enabled_schedulers_(0),
      metadata_bytes_(0), peak_active_holes_(0), peak_metadata_bytes_(0) {
    pthread_rwlock_init(&latch_, NULL);
}

SDPCCLongHoleManager::~SDPCCLongHoleManager() {
    delete[] frontiers_;
    pthread_rwlock_destroy(&latch_);
}

void SDPCCLongHoleManager::init(uint64_t scheduler_cnt) {
    scheduler_cnt_ = scheduler_cnt;
    holes_.resize(scheduler_cnt_);
    adaptive_counters_.assign(scheduler_cnt_ * ADAPTIVE_COUNTER_STRIDE, 0);
    frontiers_ = new std::atomic<uint64_t>[scheduler_cnt_];
    for (uint64_t i = 0; i < scheduler_cnt_; ++i) {
        frontiers_[i].store(0, std::memory_order_relaxed);
    }
}

bool SDPCCLongHoleManager::enabled() const {
    return SDPCC_LONG_HOLE_MODE != SDPCC_LONG_HOLE_DISABLED;
}

bool SDPCCLongHoleManager::is_long_ycsb(TxnManager *txn) const {
#if WORKLOAD == YCSB && LONG_TXN_WORKLOAD
    YCSBQuery *query = static_cast<YCSBQuery *>(txn->get_query());
    return enabled() && query->requests.size() > g_req_per_short_query;
#else
    (void)txn;
    return false;
#endif
}

bool SDPCCLongHoleManager::should_publish(uint64_t scheduler_id, TxnManager *txn) {
    const bool is_long = is_long_ycsb(txn);
    if (!SDPCC_LONG_HOLE_ADAPTIVE) return is_long;
    assert(scheduler_id < scheduler_cnt_);
    uint64_t *counters = &adaptive_counters_[scheduler_id * ADAPTIVE_COUNTER_STRIDE];
    ++counters[ADAPTIVE_WINDOW_TXNS];
    if (is_long) ++counters[ADAPTIVE_WINDOW_LONG_TXNS];

    if (counters[ADAPTIVE_WINDOW_TXNS] >= SDPCC_LONG_HOLE_ADAPTIVE_WINDOW) {
        const uint64_t total = counters[ADAPTIVE_WINDOW_TXNS];
        const uint64_t long_txns = counters[ADAPTIVE_WINDOW_LONG_TXNS];
        if (!counters[ADAPTIVE_ENABLED] &&
                long_txns * 100 >= total * SDPCC_LONG_HOLE_ENABLE_PCT) {
            counters[ADAPTIVE_ENABLED] = 1;
            ++counters[ADAPTIVE_ENABLE_TRANSITIONS];
            const uint64_t previously_enabled =
                    adaptive_enabled_schedulers_.fetch_add(1, std::memory_order_acq_rel);
            if (previously_enabled == 0) {
                // Older snapshots can only make the final locked validation
                // reject conservatively. Subsequent normal advances refresh
                // each scheduler's own frontier.
                for (uint64_t i = 0; i < scheduler_cnt_; ++i) {
                    frontiers_[i].store(sids[i], std::memory_order_relaxed);
                }
            }
        } else if (counters[ADAPTIVE_ENABLED] &&
                long_txns * 100 <= total * SDPCC_LONG_HOLE_DISABLE_PCT) {
            counters[ADAPTIVE_ENABLED] = 0;
            ++counters[ADAPTIVE_DISABLE_TRANSITIONS];
            const uint64_t previously_enabled =
                    adaptive_enabled_schedulers_.fetch_sub(1, std::memory_order_acq_rel);
            assert(previously_enabled > 0);
        }
        counters[ADAPTIVE_WINDOW_TXNS] = 0;
        counters[ADAPTIVE_WINDOW_LONG_TXNS] = 0;
    }
    return is_long && counters[ADAPTIVE_ENABLED];
}

uint64_t SDPCCLongHoleManager::mix64(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

void SDPCCLongHoleManager::BloomSet::init() {
    words.assign((SDPCC_LONG_BLOOM_BITS + 63) / 64, 0);
}

void SDPCCLongHoleManager::BloomSet::add(uint64_t key) {
    const uint64_t h1 = SDPCCLongHoleManager::mix64(key);
    const uint64_t h2 = SDPCCLongHoleManager::mix64(key ^ 0xd6e8feb86659fd93ULL) | 1ULL;
    for (uint64_t i = 0; i < SDPCC_LONG_BLOOM_HASHES; ++i) {
        const uint64_t bit = (h1 + i * h2) % SDPCC_LONG_BLOOM_BITS;
        words[bit >> 6] |= 1ULL << (bit & 63);
    }
}

bool SDPCCLongHoleManager::BloomSet::may_contain(uint64_t key) const {
    const uint64_t h1 = SDPCCLongHoleManager::mix64(key);
    const uint64_t h2 = SDPCCLongHoleManager::mix64(key ^ 0xd6e8feb86659fd93ULL) | 1ULL;
    for (uint64_t i = 0; i < SDPCC_LONG_BLOOM_HASHES; ++i) {
        const uint64_t bit = (h1 + i * h2) % SDPCC_LONG_BLOOM_BITS;
        if ((words[bit >> 6] & (1ULL << (bit & 63))) == 0) return false;
    }
    return true;
}

void SDPCCLongHoleManager::build_hole(Hole &hole, TxnManager *txn) const {
#if WORKLOAD == YCSB
    YCSBQuery *query = static_cast<YCSBQuery *>(txn->get_query());
    YCSBWorkload *wl = static_cast<YCSBWorkload *>(txn->get_wl());
    if (SDPCC_LONG_HOLE_MODE == SDPCC_LONG_HOLE_BLOOM) {
        hole.read_bloom.init();
        hole.write_bloom.init();
    }
    for (uint64_t i = 0; i < query->requests.size(); ++i) {
        ycsb_request *req = query->requests[i];
        if (GET_NODE_ID(wl->key_to_part(req->key)) != g_node_id) continue;
        ++hole.local_key_count;
        if (SDPCC_LONG_HOLE_MODE == SDPCC_LONG_HOLE_EXACT) {
            if (req->acctype == WR) hole.writes.insert(req->key);
            else hole.reads.insert(req->key);
        } else if (req->acctype == WR) {
            hole.write_bloom.add(req->key);
        } else {
            hole.read_bloom.add(req->key);
        }
    }
#else
    (void)hole;
    (void)txn;
#endif
}

bool SDPCCLongHoleManager::conflicts(const Hole &hole, TxnManager *txn,
                                     uint64_t &probes) const {
#if WORKLOAD == YCSB
    YCSBQuery *query = static_cast<YCSBQuery *>(txn->get_query());
    YCSBWorkload *wl = static_cast<YCSBWorkload *>(txn->get_wl());
    for (uint64_t i = 0; i < query->requests.size(); ++i) {
        ycsb_request *req = query->requests[i];
        if (GET_NODE_ID(wl->key_to_part(req->key)) != g_node_id) continue;
        ++probes;
        if (SDPCC_LONG_HOLE_MODE == SDPCC_LONG_HOLE_EXACT) {
            if (hole.writes.count(req->key) != 0) return true;
            if (req->acctype == WR && hole.reads.count(req->key) != 0) return true;
        } else {
            if (hole.write_bloom.may_contain(req->key)) return true;
            if (req->acctype == WR && hole.read_bloom.may_contain(req->key)) return true;
        }
    }
#else
    (void)hole;
    (void)txn;
    (void)probes;
#endif
    return false;
}

uint64_t SDPCCLongHoleManager::metadata_bytes(const Hole &hole) const {
    if (SDPCC_LONG_HOLE_MODE == SDPCC_LONG_HOLE_EXACT) {
        // libstdc++ node layout is implementation-specific; include the key,
        // link/hash overhead, and bucket arrays as a stable approximation.
        const uint64_t nodes = hole.reads.size() + hole.writes.size();
        const uint64_t buckets = hole.reads.bucket_count() + hole.writes.bucket_count();
        return nodes * (sizeof(uint64_t) + 2 * sizeof(void *)) + buckets * sizeof(void *);
    }
    return (hole.read_bloom.words.size() + hole.write_bloom.words.size()) * sizeof(uint64_t);
}

void SDPCCLongHoleManager::update_peak(std::atomic<uint64_t> &peak, uint64_t value) {
    uint64_t old = peak.load(std::memory_order_relaxed);
    while (old < value && !peak.compare_exchange_weak(old, value, std::memory_order_relaxed)) {}
}

void SDPCCLongHoleManager::publish(uint64_t scheduler_id, uint64_t key, TxnManager *txn) {
    const uint64_t start = get_sys_clock();
    Hole built;
    built.active = true;
    built.key = key;
    build_hole(built, txn);
    const uint64_t bytes = metadata_bytes(built);

    pthread_rwlock_wrlock(&latch_);
    assert(scheduler_id < scheduler_cnt_);
    assert(!holes_[scheduler_id].active);
    assert(key > frontiers_[scheduler_id].load(std::memory_order_relaxed));
    holes_[scheduler_id] = std::move(built);
    ++active_holes_;
    metadata_bytes_ += bytes;
    // The hole must be visible before the scheduler's sid advances.
    frontiers_[scheduler_id].store(key, std::memory_order_release);
    sids[scheduler_id] = key;
    update_peak(peak_active_holes_, active_holes_);
    update_peak(peak_metadata_bytes_, metadata_bytes_);
    pthread_rwlock_unlock(&latch_);

    uint64_t *counters = &adaptive_counters_[scheduler_id * ADAPTIVE_COUNTER_STRIDE];
    ++counters[COUNTER_PUBLISHED];
    counters[COUNTER_BUILD_TIME_NS] += get_sys_clock() - start;
}

void SDPCCLongHoleManager::clear(uint64_t scheduler_id, uint64_t key) {
    pthread_rwlock_wrlock(&latch_);
    assert(scheduler_id < scheduler_cnt_);
    Hole &hole = holes_[scheduler_id];
    assert(hole.active && hole.key == key &&
           frontiers_[scheduler_id].load(std::memory_order_relaxed) == key);
    const uint64_t bytes = metadata_bytes(hole);
    assert(active_holes_ > 0 && metadata_bytes_ >= bytes);
    --active_holes_;
    metadata_bytes_ -= bytes;
    hole = Hole();
    pthread_rwlock_unlock(&latch_);
}

void SDPCCLongHoleManager::advance_normal(uint64_t scheduler_id, uint64_t key) {
    assert(scheduler_id < scheduler_cnt_);
    if (!enabled() || (SDPCC_LONG_HOLE_ADAPTIVE &&
            adaptive_enabled_schedulers_.load(std::memory_order_acquire) == 0)) {
        assert(key > sids[scheduler_id]);
        sids[scheduler_id] = key;
        return;
    }
    const uint64_t old = frontiers_[scheduler_id].load(std::memory_order_relaxed);
    assert(key > old);
    // Lock registration happens-before publishing this frontier.
    frontiers_[scheduler_id].store(key, std::memory_order_release);
    sids[scheduler_id] = key;
}

bool SDPCCLongHoleManager::should_check(uint64_t scheduler_id, uint64_t key) {
    if (!SDPCC_LONG_HOLE_ADAPTIVE) return true;
    assert(scheduler_id < scheduler_cnt_);
    uint64_t *counters = &adaptive_counters_[scheduler_id * ADAPTIVE_COUNTER_STRIDE];

    if (adaptive_enabled_schedulers_.load(std::memory_order_acquire) == 0) {
        return false;
    }

    // Do not predict feasibility from several independently changing
    // frontiers here. That snapshot is safe but can be unnecessarily stale and
    // miss a short bypass window. The locked can_bypass() validation below is
    // the sole correctness boundary.
    (void)key;
    ++counters[ADAPTIVE_ELIGIBLE];
    return true;
}

bool SDPCCLongHoleManager::can_bypass(uint64_t scheduler_id, TxnManager *txn, uint64_t key) {
    assert(scheduler_id < scheduler_cnt_);
    uint64_t *counters = &adaptive_counters_[scheduler_id * ADAPTIVE_COUNTER_STRIDE];
    const uint64_t start = get_sys_clock();
    uint64_t probes = 0;
    bool safe = true;
    bool non_hole = false;
    bool conflict = false;
    pthread_rwlock_rdlock(&latch_);
    for (uint64_t i = 0; i < scheduler_cnt_; ++i) {
        const uint64_t frontier = frontiers_[i].load(std::memory_order_acquire);
        if (frontier >= key) continue;
        const Hole &hole = holes_[i];
        if (!hole.active || hole.key != frontier) {
            safe = false;
            non_hole = true;
            break;
        }
        if (conflicts(hole, txn, probes)) {
            safe = false;
            conflict = true;
            break;
        }
    }
    pthread_rwlock_unlock(&latch_);
    ++counters[COUNTER_BYPASS_CHECKS];
    counters[COUNTER_MATCH_PROBES] += probes;
    counters[COUNTER_MATCH_TIME_NS] += get_sys_clock() - start;
    if (safe) ++counters[COUNTER_BYPASSED];
    if (non_hole) ++counters[COUNTER_REJECTED_NON_HOLE];
    if (conflict) ++counters[COUNTER_REJECTED_CONFLICT];
    return safe;
}

void SDPCCLongHoleManager::record_watermark_wait(uint64_t scheduler_id, uint64_t wait_ns) {
    assert(scheduler_id < scheduler_cnt_);
    uint64_t *counters = &adaptive_counters_[scheduler_id * ADAPTIVE_COUNTER_STRIDE];
    ++counters[COUNTER_WATERMARK_WAIT_COUNT];
    counters[COUNTER_WATERMARK_WAIT_NS] += wait_ns;
}

void SDPCCLongHoleManager::print(FILE *outf) const {
    uint64_t skipped_no_hole = 0;
    uint64_t skipped_infeasible = 0;
    uint64_t eligible = 0;
    uint64_t published = 0;
    uint64_t checks = 0;
    uint64_t bypassed = 0;
    uint64_t rejected_non_hole = 0;
    uint64_t rejected_conflict = 0;
    uint64_t match_probes = 0;
    uint64_t build_time_ns = 0;
    uint64_t match_time_ns = 0;
    uint64_t waits = 0;
    uint64_t watermark_wait_ns = 0;
    uint64_t enable_transitions = 0;
    uint64_t disable_transitions = 0;
    uint64_t enabled_schedulers = 0;
    for (uint64_t i = 0; i < scheduler_cnt_; ++i) {
        const uint64_t *counters = &adaptive_counters_[i * ADAPTIVE_COUNTER_STRIDE];
        skipped_no_hole += counters[ADAPTIVE_SKIPPED_NO_HOLE];
        skipped_infeasible += counters[ADAPTIVE_SKIPPED_INFEASIBLE];
        eligible += counters[ADAPTIVE_ELIGIBLE];
        published += counters[COUNTER_PUBLISHED];
        checks += counters[COUNTER_BYPASS_CHECKS];
        bypassed += counters[COUNTER_BYPASSED];
        rejected_non_hole += counters[COUNTER_REJECTED_NON_HOLE];
        rejected_conflict += counters[COUNTER_REJECTED_CONFLICT];
        match_probes += counters[COUNTER_MATCH_PROBES];
        build_time_ns += counters[COUNTER_BUILD_TIME_NS];
        match_time_ns += counters[COUNTER_MATCH_TIME_NS];
        waits += counters[COUNTER_WATERMARK_WAIT_COUNT];
        watermark_wait_ns += counters[COUNTER_WATERMARK_WAIT_NS];
        enable_transitions += counters[ADAPTIVE_ENABLE_TRANSITIONS];
        disable_transitions += counters[ADAPTIVE_DISABLE_TRANSITIONS];
        enabled_schedulers += counters[ADAPTIVE_ENABLED] ? 1 : 0;
    }
    fprintf(outf,
            ",sdpcc_long_hole_mode=%d,sdpcc_long_hole_published=%lu"
            ",sdpcc_long_hole_bypass_checks=%lu,sdpcc_long_hole_bypassed=%lu"
            ",sdpcc_long_hole_reject_non_hole=%lu,sdpcc_long_hole_reject_conflict=%lu"
            ",sdpcc_long_hole_adaptive_skipped_no_hole=%lu"
            ",sdpcc_long_hole_adaptive_skipped_infeasible=%lu"
            ",sdpcc_long_hole_adaptive_eligible=%lu"
            ",sdpcc_long_hole_adaptive_enable_transitions=%lu"
            ",sdpcc_long_hole_adaptive_disable_transitions=%lu"
            ",sdpcc_long_hole_adaptive_enabled_schedulers=%lu"
            ",sdpcc_long_hole_match_probes=%lu,sdpcc_long_hole_avg_build_ns=%f"
            ",sdpcc_long_hole_avg_match_ns=%f,sdpcc_watermark_wait_count=%lu"
            ",sdpcc_watermark_avg_wait_ns=%f,sdpcc_long_hole_peak_active=%lu"
            ",sdpcc_long_hole_peak_metadata_est_bytes=%lu",
            SDPCC_LONG_HOLE_MODE, published, checks,
            bypassed, rejected_non_hole, rejected_conflict,
            skipped_no_hole, skipped_infeasible, eligible,
            enable_transitions, disable_transitions, enabled_schedulers,
            match_probes,
            published ? static_cast<double>(build_time_ns) / published : 0.0,
            checks ? static_cast<double>(match_time_ns) / checks : 0.0,
            waits, waits ? static_cast<double>(watermark_wait_ns) / waits : 0.0,
            peak_active_holes_.load(std::memory_order_relaxed),
            peak_metadata_bytes_.load(std::memory_order_relaxed));
}

#endif
