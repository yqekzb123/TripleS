#ifndef _BOMB_H_
#define _BOMB_H_

#include "bomb_query.h"
#include "txn.h"
#include "wl.h"

#include <atomic>
#include <cstdio>
#include <mutex>
#include <vector>

class BombClientQueryMessage;
class BombWorkload;

class BombStats {
public:
  static void record_submit(BombClientQueryMessage *msg, BombWorkload *wl,
                            uint64_t nodes);
  static void record_complete(BombClientQueryMessage *msg, uint64_t latency,
                              bool aborted);
  static void record_abort_attempt(BombClientQueryMessage *msg);
  static void print(FILE *out);

private:
  static std::atomic<uint64_t> submitted[BOMB_TXN_TYPE_COUNT];
  static std::atomic<uint64_t> committed[BOMB_TXN_TYPE_COUNT];
  static std::atomic<uint64_t> aborted[BOMB_TXN_TYPE_COUNT];
  static std::atomic<uint64_t> read_sum[BOMB_TXN_TYPE_COUNT];
  static std::atomic<uint64_t> write_sum[BOMB_TXN_TYPE_COUNT];
  static std::atomic<uint64_t> node_sum[BOMB_TXN_TYPE_COUNT];
  static std::atomic<uint64_t> remote_read_sum[BOMB_TXN_TYPE_COUNT];
  static std::atomic<uint64_t> remote_write_sum[BOMB_TXN_TYPE_COUNT];
  static std::atomic<uint64_t> active_l1;
  static std::atomic<uint64_t> peak_l1;
  static std::atomic<uint64_t> first_submit_time;
  static std::atomic<uint64_t> source_submitted[64];
  static std::atomic<uint64_t> source_committed[64];
  static std::mutex latency_mutex;
  static std::vector<uint64_t> latencies[BOMB_TXN_TYPE_COUNT];
};

class BombWorkload : public Workload {
public:
  RC init();
  RC init_schema(const char *schema_file);
  RC init_table();
  RC get_txn_man(TxnManager *&txn_manager);

  uint64_t request_to_part(const BombRequest &request) const;
  INDEX *request_index(const BombRequest &request) const;
  table_t *request_table(const BombRequest &request) const;

  table_t *bomb_tables[BOMB_TABLE_COUNT];
  INDEX *bomb_indexes[BOMB_TABLE_COUNT];

private:
  RC insert_row(BombTable table, uint64_t key,
                const std::vector<uint64_t> &u64_values,
                const std::vector<double> &double_values);
};

class BombTxnManager : public TxnManager {
public:
  void init(uint64_t thd_id, Workload *wl);
  void reset();
  RC acquire_locks();
  RC run_txn();
  RC run_txn_post_wait();
  RC run_calvin_txn();
#if CC_ALG == ARIA
  RC run_aria_txn();
  RC process_aria_remote(ARIA_PHASE phase);
  void merge_version_hints(Array<uint64_t> &hints);
#endif
#if CC_ALG == CARACAL
  RC run_caracal_txn();
  RC run_sub_caracal_txn();
#endif
  RC send_remote_request() { return RCOK; }

private:
  RC execute_phase(bool writes);
  RC validate_plan();
  RC access_request(BombRequest *request, bool write_phase);
#if CC_ALG == ARIA
  RC send_aria_remote(ARIA_PHASE phase, bool reads_only);
  bool node_has_requests(uint64_t node, bool reads_only) const;
#endif
  BombWorkload *_bomb_wl;
  uint64_t checksum;
  uint64_t next_record_id;
  RC do_insert() { return RCOK; }
};

#endif
