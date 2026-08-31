#ifndef _BOMB_QUERY_H_
#define _BOMB_QUERY_H_

#include "array.h"
#include "global.h"
#include "query.h"

#include <atomic>
#include <set>
#include <map>
#include <vector>

class Message;
class Workload;
class BombClientQueryMessage;

enum BombTxnType : uint32_t {
  BOMB_L1 = 0,
  BOMB_S1,
  BOMB_S2,
  BOMB_S3,
  BOMB_S4,
  BOMB_S5,
  BOMB_TXN_TYPE_COUNT
};

enum BombTable : uint32_t {
  BOMB_TABLE_BOM = 0,
  BOMB_TABLE_PRODUCT,
  BOMB_TABLE_MATERIAL_COST,
  BOMB_TABLE_RESULT_COST,
  BOMB_TABLE_JOURNAL_VOUCHER,
  BOMB_TABLE_COUNT
};

enum BombRequestRole : uint32_t {
  BOMB_ROLE_READ = 0,
  BOMB_ROLE_MATERIAL_RMW,
  BOMB_ROLE_RESULT_WRITE,
  BOMB_ROLE_VOUCHER_WRITE,
  BOMB_ROLE_PRODUCT_REPLACE,
  BOMB_ROLE_BOM_REPLACE,
  BOMB_ROLE_QUANTITY_WRITE,
  BOMB_ROLE_GUARD_VALIDATE
};

struct BombRequest {
  uint32_t table;
  access_t acctype;
  uint32_t role;
  uint64_t key;
  uint64_t expected_version;
  uint64_t arg0;
  uint64_t arg1;
  double value;
};

class BombQuery : public BaseQuery {
public:
  BombQuery();
  void init();
  void init(uint64_t thd_id, Workload *wl);
  void reset();
  void release();
  void print();
  bool readonly();
  uint64_t get_participants(Workload *wl);
  static std::set<uint64_t> participants(Message *msg, Workload *wl);

  BombTxnType txn_type;
  uint64_t factory_id;
  uint64_t ordinal;
  uint64_t source_id;
  uint64_t plan_epoch;
  bool planned;
  Array<BombRequest *> requests;
  // Planner-only index. It is rebuilt when a plan is materialized and is not
  // part of the wire format.
  std::map<std::pair<uint32_t, uint64_t>, BombRequest *> request_index;
};

class BombQueryGenerator : public QueryGenerator {
public:
  BombQueryGenerator();
  BaseQuery *create_query(Workload *wl, uint64_t home_partition_id);
  BombQuery *create_for_client(Workload *wl, uint64_t home_node,
                               uint64_t client_thread);
  static void prepare_next(BombQuery *query, uint64_t home_node,
                           uint64_t client_thread);
  static void init_source_state(uint64_t thread_count);
  static bool is_long_source(uint64_t client_thread);
  static bool is_enabled_client(uint64_t client_thread);
  static bool try_begin_long(uint64_t client_thread);
  static void complete_long(uint64_t client_thread);
  static void materialize(BombQuery *query);
  static void release_plan(BombQuery *query);

  static uint64_t composite_key(uint64_t first, uint64_t second);
  static uint64_t item_material_start();
  static uint64_t item_raw_start();
  static uint64_t tree_count();
  static void product_roots(uint64_t product_id,
                            std::vector<uint64_t> &roots);
  static void tree_edges(uint64_t root_index,
                         std::vector<std::pair<uint64_t,uint64_t> > &edges,
                         std::vector<uint64_t> &raw_leaves);

private:
  static std::atomic<uint64_t> next_ordinal;
  static std::atomic<bool> *source_busy;
  static uint64_t source_count;
  static uint64_t mix_hash(uint64_t value);
  static BombTxnType choose_short(uint64_t ordinal);
  static void add_request(BombQuery *query, BombTable table, access_t access,
                          BombRequestRole role, uint64_t key,
                          uint64_t arg0 = 0, uint64_t arg1 = 0,
                          double value = 0.0,
                          uint64_t expected_version = 0);
  static void plan_l1(BombQuery *query);
  static void plan_s1(BombQuery *query);
  static void plan_s2(BombQuery *query);
  static void plan_s3(BombQuery *query);
  static void plan_s4(BombQuery *query);
  static void plan_s5(BombQuery *query);
};

#endif
