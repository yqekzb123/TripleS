#ifndef _CHBENCHMARK_H_
#define _CHBENCHMARK_H_

#include "chbenchmark_query.h"
#include "tpcc.h"
#include <unordered_set>
#include <vector>

enum CHRegionCols { R_REGIONKEY, R_NAME, R_COMMENT };
enum CHNationCols { N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT };
enum CHSupplierCols {
  SU_SUPPKEY,
  SU_NAME,
  SU_ADDRESS,
  SU_NATIONKEY,
  SU_PHONE,
  SU_ACCTBAL,
  SU_COMMENT
};

enum CHTableSlot {
  CH_WAREHOUSE_SLOT,
  CH_DISTRICT_SLOT,
  CH_CUSTOMER_SLOT,
  CH_HISTORY_SLOT,
  CH_NEWORDER_SLOT,
  CH_ORDER_SLOT,
  CH_ORDERLINE_SLOT,
  CH_ITEM_SLOT,
  CH_STOCK_SLOT,
  CH_REGION_SLOT,
  CH_NATION_SLOT,
  CH_SUPPLIER_SLOT,
  CH_TABLE_SLOT_COUNT
};

class CHBenchmarkWorkload : public TPCCWorkload {
public:
  RC init();
  RC init_schema(const char *schema_file);
  RC init_table();
  RC get_txn_man(TxnManager *&txn_manager);

  table_t *t_region;
  table_t *t_nation;
  table_t *t_supplier;
  INDEX *i_region;
  INDEX *i_nation;
  INDEX *i_supplier;

private:
  void init_dimensions();
  void normalize_tpcc_analytical_columns();
};

class CHBenchmarkTxnManager : public TPCCTxnManager {
public:
  void init(uint64_t thd_id, Workload *wl);
  RC acquire_locks();
  RC run_txn();
  RC run_txn_post_wait();
  RC run_calvin_txn();

private:
  CHBenchmarkWorkload *_ch_wl;
  std::vector<row_t *> ch_rows[CH_TABLE_SLOT_COUNT];
  std::vector<std::vector<char> > ch_data[CH_TABLE_SLOT_COUNT];
  std::unordered_set<row_t *> ch_registered;
  uint64_t ch_result_checksum;
  uint64_t ch_result_rows;
  RC acquire_olap_intents();
  RC run_olap_query();
  void collect_initial_rows(uint64_t table_mask);
  void refresh_dynamic_rows(uint64_t table_mask);
  void register_row(CHTableSlot slot, row_t *row, bool arm_now);
  void materialize_rows();
  uint64_t execute_olap_operators(uint64_t query_number);
};

#endif
