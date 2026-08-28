#include "chbenchmark.h"

#include "catalog.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "row.h"
#include "table.h"
#include "tpcc_const.h"
#include "tpcc_helper.h"

#include <cstdio>
#include <cstring>
#include <vector>

#if WORKLOAD == CHBENCHMARK

namespace {
const uint64_t kNationKeys[62] = {
    '0','1','2','3','4','5','6','7','8','9',
    'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
    'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};

const char *kRegionNames[5] = {"Africa", "America", "Asia", "Australia", "Europe"};

const char *nation_name(uint64_t key) {
  if (key == '6') return "Germany";
  if (key == 'N') return "Cambodia";
  if (key == 'F') return "France";
  if (key == 'U') return "United Kingdom";
  static thread_local char name[25];
  snprintf(name, sizeof(name), "Nation-%c", static_cast<char>(key));
  return name;
}

void set_text(row_t *row, int column, const char *value) {
  const uint64_t size = row->get_schema()->get_field_size(column);
  std::vector<char> bytes(size, 0);
  strncpy(bytes.data(), value, size);
  row->set_value(column, bytes.data());
}
}

RC CHBenchmarkWorkload::init() {
  static_assert(CC_ALG == SDMVCC,
                "The initial standalone CH-benCHmark implementation supports SDMVCC only");
  Workload::init();
  const char *schema_root = getenv("SCHEMA_PATH");
  string path = schema_root == NULL ? "./benchmarks/" : string(schema_root);
  path += "CHBENCHMARK_schema.txt";

  delivering = new bool *[g_num_wh + 1];
  for (uint64_t wid = 1; wid <= g_num_wh; ++wid)
    delivering[wid] = static_cast<bool *>(mem_allocator.alloc(CL_SIZE));

  printf("Initializing standalone CH-benCHmark schema from %s\n", path.c_str());
  init_schema(path.c_str());
  init_table();
  return RCOK;
}

RC CHBenchmarkWorkload::init_schema(const char *schema_file) {
  TPCCWorkload::init_schema(schema_file);
  t_region = tables["REGION"];
  t_nation = tables["NATION"];
  t_supplier = tables["SUPPLIER"];
  i_region = static_cast<INDEX *>(indexes["REGION_IDX"]);
  i_nation = static_cast<INDEX *>(indexes["NATION_IDX"]);
  i_supplier = static_cast<INDEX *>(indexes["SUPPLIER_IDX"]);
  return RCOK;
}

RC CHBenchmarkWorkload::init_table() {
  TPCCWorkload::init_table();
  normalize_tpcc_analytical_columns();
  init_dimensions();
  return RCOK;
}

void CHBenchmarkWorkload::normalize_tpcc_analytical_columns() {
  itemid_t *item = NULL;
  for (uint64_t iid = 1; iid <= g_max_items; ++iid) {
    i_item->index_read(iid, item, 0, 0, NULL);
    row_t *row = static_cast<row_t *>(item->location);
    char name[32]; snprintf(name, sizeof(name), "Item-%06lu", iid);
    set_text(row, I_NAME, name);
    char data[64];
    switch (iid % 10) {
      case 0: snprintf(data, sizeof(data), "PRco-item-%lubb", iid); break;
      case 1: snprintf(data, sizeof(data), "co-item-%lubb", iid); break;
      case 2: snprintf(data, sizeof(data), "item-%lua", iid); break;
      case 3: snprintf(data, sizeof(data), "item-%lub", iid); break;
      case 4: snprintf(data, sizeof(data), "item-%luc", iid); break;
      case 5: snprintf(data, sizeof(data), "zz-item-%lu", iid); break;
      default: snprintf(data, sizeof(data), "item-%lu", iid); break;
    }
    set_text(row, I_DATA, data);
  }

  const char nation_chars[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  for (uint64_t wid = 1; wid <= g_num_wh; ++wid) {
    if (GET_NODE_ID(wh_to_part(wid)) != g_node_id) continue;
    for (uint64_t did = 1; did <= g_dist_per_wh; ++did) {
      for (uint64_t cid = 1; cid <= g_cust_per_dist; ++cid) {
        i_customer_id->index_read(custKey(cid, did, wid), item,
                                  wh_to_part(wid), 0, NULL);
        row_t *row = static_cast<row_t *>(item->location);
        char state[3] = {nation_chars[(cid + did + wid) % 62], 'A', 0};
        char phone[17]; snprintf(phone, sizeof(phone), "%lu%015lu",
                                 1 + (cid % 7), cid);
        char city[24]; snprintf(city, sizeof(city), "City-%02lu", did);
        set_text(row, C_STATE, state);
        set_text(row, C_PHONE, phone);
        set_text(row, C_CITY, city);
        if (cid % 10 == 0) row->set_value(C_BALANCE, 100.0 + cid);
      }

      std::vector<row_t *> rows;
      i_order->snapshot_rows(wd_to_part(wid, did), rows);
      for (row_t *row : rows) row->set_value(O_ENTRY_D, 20130101ul);
      rows.clear();
      i_orderline->snapshot_rows(wd_to_part(wid, did), rows);
      for (row_t *row : rows) {
        uint64_t oid; row->get_value(OL_O_ID, oid);
        row->set_value(OL_DELIVERY_D, oid < 2101 ? 20130102ul : 0ul);
      }
    }
    for (uint64_t iid = 1; iid <= g_max_items; ++iid) {
      i_stock->index_read(stockKey(iid, wid), item, wh_to_part(wid), 0, NULL);
      row_t *row = static_cast<row_t *>(item->location);
      char data[64]; snprintf(data, sizeof(data), "stock-%lu-%lu", wid, iid);
      set_text(row, S_DATA, data);
    }
  }
}

void CHBenchmarkWorkload::init_dimensions() {
  for (uint64_t rid = 1; rid <= 5; ++rid) {
    row_t *row; uint64_t row_id;
    t_region->get_new_row(row, 0, row_id);
    row->set_primary_key(rid);
    row->set_value(R_REGIONKEY, rid);
    set_text(row, R_NAME, kRegionNames[rid - 1]);
    set_text(row, R_COMMENT, "CH region");
    index_insert(i_region, rid, row, 0);
  }

  for (uint64_t i = 0; i < 62; ++i) {
    const uint64_t key = kNationKeys[i];
    row_t *row; uint64_t row_id;
    t_nation->get_new_row(row, 0, row_id);
    row->set_primary_key(key);
    row->set_value(N_NATIONKEY, key);
    set_text(row, N_NAME, nation_name(key));
    uint64_t region = key == '6' ? 5 : (key == 'N' ? 3 : 1 + (i % 5));
    row->set_value(N_REGIONKEY, region);
    set_text(row, N_COMMENT, "CH nation");
    index_insert(i_nation, key, row, 0);
  }

  for (uint64_t sid = 0; sid < CH_SUPPLIER_COUNT; ++sid) {
    row_t *row; uint64_t row_id;
    t_supplier->get_new_row(row, 0, row_id);
    row->set_primary_key(sid);
    row->set_value(SU_SUPPKEY, sid);
    char value[128];
    snprintf(value, sizeof(value), "Supplier-%05lu", sid);
    set_text(row, SU_NAME, value);
    snprintf(value, sizeof(value), "Address-%05lu", sid);
    set_text(row, SU_ADDRESS, value);
    row->set_value(SU_NATIONKEY, kNationKeys[sid % 62]);
    snprintf(value, sizeof(value), "%015lu", sid);
    set_text(row, SU_PHONE, value);
    const double balance = 10000.0 + static_cast<double>(sid * 97);
    row->set_value(SU_ACCTBAL, balance);
    set_text(row, SU_COMMENT, sid % 100 == 0 ? "bad supplier" : "CH supplier");
    index_insert(i_supplier, sid, row, 0);
  }
  printf("CH dimensions initialized: 5 regions, 62 nations, %d suppliers\n",
         CH_SUPPLIER_COUNT);
}

RC CHBenchmarkWorkload::get_txn_man(TxnManager *&txn_manager) {
  txn_manager = static_cast<CHBenchmarkTxnManager *>(
      mem_allocator.align_alloc(sizeof(CHBenchmarkTxnManager)));
  new (txn_manager) CHBenchmarkTxnManager();
  return RCOK;
}
#endif
