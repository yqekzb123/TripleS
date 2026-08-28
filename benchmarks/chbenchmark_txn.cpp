#include "chbenchmark.h"

#include "index_btree.h"
#include "index_hash.h"
#include "row.h"
#include "row_sdmvcc.h"
#include "tpcc_helper.h"

#include <cstring>

#if WORKLOAD == CHBENCHMARK

namespace {
uint64_t query_table_mask(uint64_t q) {
  const uint64_t C = 1ULL << CH_CUSTOMER_SLOT;
  const uint64_t NO = 1ULL << CH_NEWORDER_SLOT;
  const uint64_t O = 1ULL << CH_ORDER_SLOT;
  const uint64_t OL = 1ULL << CH_ORDERLINE_SLOT;
  const uint64_t I = 1ULL << CH_ITEM_SLOT;
  const uint64_t S = 1ULL << CH_STOCK_SLOT;
  const uint64_t R = 1ULL << CH_REGION_SLOT;
  const uint64_t N = 1ULL << CH_NATION_SLOT;
  const uint64_t SU = 1ULL << CH_SUPPLIER_SLOT;
  switch (q) {
    case 1: return OL;
    case 2: return I|SU|S|N|R;
    case 3: return C|NO|O|OL;
    case 4: return O|OL;
    case 5: return C|O|OL|S|SU|N|R;
    case 6: return OL;
    case 7: return SU|S|OL|O|C|N;
    case 8: return I|SU|S|OL|O|C|N|R;
    case 9: return I|S|SU|OL|O|N;
    case 10: return C|O|OL|N;
    case 11: return S|SU|N;
    case 12: return O|OL;
    case 13: return C|O;
    case 14: return OL|I;
    case 15: return OL|S|SU;
    case 16: return S|I|SU;
    case 17: return OL|I;
    case 18: return C|O|OL;
    case 19: return OL|I;
    case 20: return SU|N|S|I|OL;
    case 21: return SU|OL|O|S|N;
    case 22: return C|O;
    default: assert(false); return 0;
  }
}
}

void CHBenchmarkTxnManager::init(uint64_t thd_id, Workload *wl) {
  TPCCTxnManager::init(thd_id, wl);
  _ch_wl = static_cast<CHBenchmarkWorkload *>(wl);
}

RC CHBenchmarkTxnManager::acquire_locks() {
  CHBenchmarkQuery *ch_query = static_cast<CHBenchmarkQuery *>(query);
  if (!ch_query->is_olap()) return TPCCTxnManager::acquire_locks();
  return acquire_olap_intents();
}

RC CHBenchmarkTxnManager::run_txn() {
  CHBenchmarkQuery *ch_query = static_cast<CHBenchmarkQuery *>(query);
  if (!ch_query->is_olap()) return TPCCTxnManager::run_txn();
  return run_olap_query();
}

RC CHBenchmarkTxnManager::run_txn_post_wait() {
  CHBenchmarkQuery *ch_query = static_cast<CHBenchmarkQuery *>(query);
  if (!ch_query->is_olap()) return TPCCTxnManager::run_txn_post_wait();
  return run_olap_query();
}

RC CHBenchmarkTxnManager::run_calvin_txn() {
  CHBenchmarkQuery *ch_query = static_cast<CHBenchmarkQuery *>(query);
  if (!ch_query->is_olap()) return TPCCTxnManager::run_calvin_txn();
  if (phase == CALVIN_DONE) return RCOK;
  RC rc = run_olap_query();
  if (rc == RCOK) phase = CALVIN_DONE;
  return rc;
}

RC CHBenchmarkTxnManager::acquire_olap_intents() {
  CHBenchmarkQuery *ch_query = static_cast<CHBenchmarkQuery *>(query);
  const uint64_t mask = query_table_mask(ch_query->query_number());
  pin_sdmvcc_snapshot();
  ch_registered.clear();
  for (uint64_t i = 0; i < CH_TABLE_SLOT_COUNT; ++i) {
    ch_rows[i].clear();
    ch_data[i].clear();
  }
  collect_initial_rows(mask);
  txn_stats.wait_starttime = get_sys_clock();
  locking_done = true;
  return RCOK;
}

RC CHBenchmarkTxnManager::run_olap_query() {
  CHBenchmarkQuery *ch_query = static_cast<CHBenchmarkQuery *>(query);
  const uint64_t mask = query_table_mask(ch_query->query_number());
  refresh_dynamic_rows(mask);
  materialize_rows();
  ch_result_checksum = execute_olap_operators(ch_query->query_number());
  return RCOK;
}

void CHBenchmarkTxnManager::register_row(CHTableSlot slot, row_t *target,
                                         bool arm_now) {
  if (!target->manager->visible(sdmvcc_snapshot())) return;
  if (!ch_registered.insert(target).second) return;
  RC rc = get_lock(target, RD);
  assert(rc == RCOK);
  if (arm_now) {
    const bool ready = target->manager->arm_read(this, sdmvcc_snapshot());
    assert(ready);
  }
  ch_rows[slot].push_back(target);
}

void CHBenchmarkTxnManager::collect_initial_rows(uint64_t mask) {
  CHBenchmarkQuery *ch_query = static_cast<CHBenchmarkQuery *>(query);
  auto hash_row = [&](CHTableSlot slot, INDEX *index, uint64_t key, int part) {
    itemid_t *item = NULL;
    RC rc = index->index_read(key, item, part, get_thd_id(), this);
    assert(rc == RCOK && item != NULL);
    register_row(slot, static_cast<row_t *>(item->location), false);
  };

  if (mask & (1ULL << CH_ITEM_SLOT)) {
    for (uint64_t iid = 1; iid <= g_max_items; ++iid)
      hash_row(CH_ITEM_SLOT, _ch_wl->i_item, iid, 0);
  }
  if (mask & (1ULL << CH_REGION_SLOT)) {
    for (uint64_t rid = 1; rid <= 5; ++rid)
      hash_row(CH_REGION_SLOT, _ch_wl->i_region, rid, 0);
  }
  if (mask & (1ULL << CH_NATION_SLOT)) {
    const char chars[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (uint64_t i = 0; i < 62; ++i)
      hash_row(CH_NATION_SLOT, _ch_wl->i_nation, chars[i], 0);
  }
  if (mask & (1ULL << CH_SUPPLIER_SLOT)) {
    for (uint64_t sid = 0; sid < CH_SUPPLIER_COUNT; ++sid)
      hash_row(CH_SUPPLIER_SLOT, _ch_wl->i_supplier, sid, 0);
  }

  for (uint64_t off = 0; off < ch_query->ch_wh_count; ++off) {
    const uint64_t wid = ch_query->ch_wh_start + off;
    const uint64_t wh_part = wh_to_part(wid);
    if (GET_NODE_ID(wh_part) != g_node_id) continue;
    if (mask & (1ULL << CH_STOCK_SLOT)) {
      for (uint64_t iid = 1; iid <= g_max_items; ++iid)
        hash_row(CH_STOCK_SLOT, _ch_wl->i_stock, stockKey(iid, wid), wh_part);
    }
    if (mask & (1ULL << CH_CUSTOMER_SLOT)) {
      for (uint64_t did = 1; did <= g_dist_per_wh; ++did)
        for (uint64_t cid = 1; cid <= g_cust_per_dist; ++cid)
          hash_row(CH_CUSTOMER_SLOT, _ch_wl->i_customer_id,
                   custKey(cid, did, wid), wh_part);
    }
    for (uint64_t did = 1; did <= g_dist_per_wh; ++did) {
      const int part = wd_to_part(wid, did);
      std::vector<row_t *> rows;
      std::vector<row_t *> guards;
      if (mask & (1ULL << CH_ORDER_SLOT)) {
        _ch_wl->i_order->snapshot_rows(part, rows, &guards);
        for (row_t *guard : guards) register_row(CH_ORDER_SLOT, guard, false);
        for (row_t *r : rows) register_row(CH_ORDER_SLOT, r, false);
      }
      rows.clear(); guards.clear();
      if (mask & (1ULL << CH_NEWORDER_SLOT)) {
        _ch_wl->i_neworder->snapshot_rows(part, rows, &guards);
        for (row_t *guard : guards) register_row(CH_NEWORDER_SLOT, guard, false);
        for (row_t *r : rows) register_row(CH_NEWORDER_SLOT, r, false);
      }
      rows.clear(); guards.clear();
      if (mask & (1ULL << CH_ORDERLINE_SLOT)) {
        _ch_wl->i_orderline->snapshot_rows(part, rows, &guards);
        for (row_t *guard : guards) register_row(CH_ORDERLINE_SLOT, guard, false);
        for (row_t *r : rows) register_row(CH_ORDERLINE_SLOT, r, false);
      }
    }
  }
}

void CHBenchmarkTxnManager::refresh_dynamic_rows(uint64_t mask) {
  CHBenchmarkQuery *ch_query = static_cast<CHBenchmarkQuery *>(query);
  for (uint64_t off = 0; off < ch_query->ch_wh_count; ++off) {
    const uint64_t wid = ch_query->ch_wh_start + off;
    if (GET_NODE_ID(wh_to_part(wid)) != g_node_id) continue;
    for (uint64_t did = 1; did <= g_dist_per_wh; ++did) {
      const int part = wd_to_part(wid, did);
      std::vector<row_t *> rows;
      if (mask & (1ULL << CH_ORDER_SLOT)) {
        _ch_wl->i_order->snapshot_rows(part, rows);
        for (row_t *r : rows) register_row(CH_ORDER_SLOT, r, true);
      }
      rows.clear();
      if (mask & (1ULL << CH_NEWORDER_SLOT)) {
        _ch_wl->i_neworder->snapshot_rows(part, rows);
        for (row_t *r : rows) register_row(CH_NEWORDER_SLOT, r, true);
      }
      rows.clear();
      if (mask & (1ULL << CH_ORDERLINE_SLOT)) {
        _ch_wl->i_orderline->snapshot_rows(part, rows);
        for (row_t *r : rows) register_row(CH_ORDERLINE_SLOT, r, true);
      }
    }
  }
}

void CHBenchmarkTxnManager::materialize_rows() {
  for (uint64_t slot = 0; slot < CH_TABLE_SLOT_COUNT; ++slot) {
    ch_data[slot].reserve(ch_rows[slot].size());
    for (row_t *source : ch_rows[slot]) {
      // Leaf guards participate in phantom ordering but are not query tuples.
      if (source->get_primary_key() == 0 &&
          (slot == CH_ORDER_SLOT || slot == CH_NEWORDER_SLOT ||
           slot == CH_ORDERLINE_SLOT)) continue;
      row_t local;
      local.init(source->get_table(), source->get_part_id());
      RC rc = source->manager->read(sdmvcc_snapshot(), &local);
      assert(rc == RCOK);
      ch_data[slot].push_back(std::vector<char>(
          local.get_data(), local.get_data() + local.get_tuple_size()));
      consume_sdmvcc_access(source);
      local.free_row();
    }
  }
}
#endif
