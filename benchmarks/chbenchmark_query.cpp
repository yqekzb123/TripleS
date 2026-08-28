#include "chbenchmark_query.h"

#include "mem_alloc.h"
#include "message.h"
#include "tpcc_helper.h"

#include <cstring>

namespace {
static_assert(CH_QUERY_MIN >= 1 && CH_QUERY_MAX <= 22,
              "CH query range must stay within Q1..Q22");
static_assert(CH_QUERY_MIN <= CH_QUERY_MAX,
              "CH query range must not be empty");
static_assert(CH_QUERY_WAREHOUSE_PCT >= 1 && CH_QUERY_WAREHOUSE_PCT <= 100,
              "CH warehouse coverage must be in [1, 100]");

CHBenchmarkTxnType from_tpcc(TPCCTxnType type) {
  switch (type) {
    case TPCC_PAYMENT: return CH_PAYMENT;
    case TPCC_NEW_ORDER: return CH_NEW_ORDER;
    case TPCC_ORDER_STATUS: return CH_ORDER_STATUS;
    case TPCC_DELIVERY: return CH_DELIVERY;
    case TPCC_STOCK_LEVEL: return CH_STOCK_LEVEL;
    default: assert(false); return CH_PAYMENT;
  }
}
}

void CHBenchmarkQuery::init(uint64_t, Workload *) { init(); }

void CHBenchmarkQuery::init() {
  items.init(g_max_items_per_txn);
  BaseQuery::init();
  ch_txn_type = CH_PAYMENT;
  ch_wh_start = 1;
  ch_wh_count = 1;
}

void CHBenchmarkQuery::reset() {
  BaseQuery::clear();
#if !CALVIN_FAMILY
  release_items();
#endif
  items.clear();
}

void CHBenchmarkQuery::release() {
  BaseQuery::release();
#if !CALVIN_FAMILY
  release_items();
#endif
  items.release();
}

bool CHBenchmarkQuery::is_olap() const {
  return ch_txn_type >= CH_Q1 && ch_txn_type <= CH_Q22;
}

uint64_t CHBenchmarkQuery::query_number() const {
  return is_olap() ? static_cast<uint64_t>(ch_txn_type) - 100 : 0;
}

bool CHBenchmarkQuery::readonly() { return is_olap(); }

void CHBenchmarkQuery::print() {
  if (is_olap()) {
    printf("CH-benCHmark Q%lu warehouses=[%lu,%lu)\n", query_number(),
           ch_wh_start, ch_wh_start + ch_wh_count);
  } else {
    TPCCQuery::print();
  }
}

std::set<uint64_t> CHBenchmarkQuery::participants(Message *message, Workload *) {
  CHBenchmarkClientQueryMessage *msg =
      static_cast<CHBenchmarkClientQueryMessage *>(message);
  std::set<uint64_t> result;
  const CHBenchmarkTxnType type = static_cast<CHBenchmarkTxnType>(msg->ch_txn_type);
  if (type >= CH_Q1 && type <= CH_Q22) {
    for (uint64_t off = 0; off < msg->ch_wh_count; ++off)
      result.insert(GET_NODE_ID(wh_to_part(msg->ch_wh_start + off)));
    return result;
  }
  result.insert(GET_NODE_ID(wh_to_part(msg->w_id)));
  if (type == CH_PAYMENT)
    result.insert(GET_NODE_ID(wh_to_part(msg->c_w_id)));
  if (type == CH_NEW_ORDER) {
    for (uint64_t i = 0; i < msg->items.size(); ++i)
      result.insert(GET_NODE_ID(wh_to_part(msg->items[i]->ol_supply_w_id)));
  }
  return result;
}

uint64_t CHBenchmarkQuery::participants(bool *&pps, Workload *) {
  for (uint64_t i = 0; i < g_node_cnt; ++i) pps[i] = false;
  uint64_t count = 0;
  auto add_wh = [&](uint64_t wid) {
    const uint64_t node = GET_NODE_ID(wh_to_part(wid));
    if (!pps[node]) { pps[node] = true; ++count; }
  };
  if (is_olap()) {
    for (uint64_t off = 0; off < ch_wh_count; ++off) add_wh(ch_wh_start + off);
  } else {
    add_wh(w_id);
    if (ch_txn_type == CH_PAYMENT) add_wh(c_w_id);
    if (ch_txn_type == CH_NEW_ORDER) {
      for (uint64_t i = 0; i < items.size(); ++i) add_wh(items[i]->ol_supply_w_id);
    }
  }
  return count;
}

uint64_t CHBenchmarkQuery::get_participants(Workload *wl) {
  bool *pps = new bool[g_node_cnt];
  const uint64_t count = participants(pps, wl);
  participant_nodes.clear();
  active_nodes.clear();
  for (uint64_t i = 0; i < g_node_cnt; ++i) {
    participant_nodes.add(pps[i] ? 1 : 0);
    active_nodes.add(pps[i] ? 1 : 0);
  }
  delete[] pps;
  return count;
}

void CHBenchmarkQueryGenerator::init() {
  tpcc_generator.init();
  mrand = static_cast<myrand *>(mem_allocator.alloc(sizeof(myrand)));
  mrand->init(get_sys_clock());
  next_olap_query = 0;
}

CHBenchmarkQuery *CHBenchmarkQueryGenerator::make_olap(uint64_t) {
  CHBenchmarkQuery *query = new CHBenchmarkQuery();
  query->init();
  const uint64_t qcount = CH_QUERY_MAX - CH_QUERY_MIN + 1;
  query->ch_txn_type = static_cast<CHBenchmarkTxnType>(
      100 + CH_QUERY_MIN + (next_olap_query++ % qcount));
  // The CH message adds the real logical type. Keeping a valid inherited
  // TPCC discriminator lets the shared fixed-field codec remain reusable.
  query->txn_type = TPCC_STOCK_LEVEL;
  query->rwset_known = true;
  query->rwset_variable = false;
  query->isDeterministicAbort = false;

  query->ch_wh_count = (g_num_wh * CH_QUERY_WAREHOUSE_PCT + 99) / 100;
  if (query->ch_wh_count == 0) query->ch_wh_count = 1;
  if (query->ch_wh_count > g_num_wh) query->ch_wh_count = g_num_wh;
  query->ch_wh_start = 1 + (mrand->next() % (g_num_wh - query->ch_wh_count + 1));

  for (uint64_t off = 0; off < query->ch_wh_count; ++off)
    query->partitions.add_unique(wh_to_part(query->ch_wh_start + off));
  return query;
}

CHBenchmarkQuery *CHBenchmarkQueryGenerator::wrap_tpcc(TPCCQuery *source) {
  CHBenchmarkQuery *query = new CHBenchmarkQuery();
  query->init();
  query->ch_txn_type = from_tpcc(source->txn_type);
  query->txn_type = source->txn_type;
  query->w_id = source->w_id; query->d_id = source->d_id; query->c_id = source->c_id;
  query->d_w_id = source->d_w_id; query->c_w_id = source->c_w_id;
  query->c_d_id = source->c_d_id; query->h_amount = source->h_amount;
  query->by_last_name = source->by_last_name;
  memcpy(query->c_last, source->c_last, LASTNAME_LEN);
  query->rbk = source->rbk; query->remote = source->remote;
  query->ol_cnt = source->ol_cnt; query->o_entry_d = source->o_entry_d;
  if (query->txn_type == TPCC_NEW_ORDER) query->o_entry_d = 20130101;
  query->o_carrier_id = source->o_carrier_id;
  query->ol_delivery_d = source->ol_delivery_d; query->o_id = source->o_id;
  query->threshold = source->threshold;
  query->rwset_known = source->rwset_known;
  query->rwset_variable = source->rwset_variable;
  query->isDeterministicAbort = source->isDeterministicAbort;
  for (uint64_t i = 0; i < source->partitions.size(); ++i)
    query->partitions.add_unique(source->partitions[i]);
  if (source->txn_type == TPCC_NEW_ORDER) {
    for (uint64_t i = 0; i < source->items.size(); ++i) {
      Item_no *item = static_cast<Item_no *>(mem_allocator.alloc(sizeof(Item_no)));
      item->copy(source->items[i]);
      query->items.add(item);
    }
    source->release_items();
    source->items.clear();
  }
  source->release();
  delete source;
  return query;
}

BaseQuery *CHBenchmarkQueryGenerator::create_query(Workload *wl,
                                                    uint64_t home_partition_id) {
  const double choose = static_cast<double>(mrand->next() % 1000000) / 1000000.0;
  if (choose < CH_OLAP_PERC) return make_olap(home_partition_id);
  return wrap_tpcc(static_cast<TPCCQuery *>(
      tpcc_generator.create_query(wl, home_partition_id)));
}
