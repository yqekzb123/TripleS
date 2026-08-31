#include "bomb.h"

#include "catalog.h"
#include "index_base.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "row.h"
#include "table.h"
#include "message.h"

#include <algorithm>

namespace {
uint64_t hash64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}
}

std::atomic<uint64_t> BombStats::submitted[BOMB_TXN_TYPE_COUNT];
std::atomic<uint64_t> BombStats::committed[BOMB_TXN_TYPE_COUNT];
std::atomic<uint64_t> BombStats::aborted[BOMB_TXN_TYPE_COUNT];
std::atomic<uint64_t> BombStats::read_sum[BOMB_TXN_TYPE_COUNT];
std::atomic<uint64_t> BombStats::write_sum[BOMB_TXN_TYPE_COUNT];
std::atomic<uint64_t> BombStats::node_sum[BOMB_TXN_TYPE_COUNT];
std::atomic<uint64_t> BombStats::remote_read_sum[BOMB_TXN_TYPE_COUNT];
std::atomic<uint64_t> BombStats::remote_write_sum[BOMB_TXN_TYPE_COUNT];
std::atomic<uint64_t> BombStats::active_l1(0);
std::atomic<uint64_t> BombStats::peak_l1(0);
std::atomic<uint64_t> BombStats::first_submit_time(0);
std::atomic<uint64_t> BombStats::source_submitted[64];
std::atomic<uint64_t> BombStats::source_committed[64];
std::mutex BombStats::latency_mutex;
std::vector<uint64_t> BombStats::latencies[BOMB_TXN_TYPE_COUNT];

void BombStats::record_submit(BombClientQueryMessage *msg, BombWorkload *wl,
                              uint64_t nodes) {
  const uint64_t type = msg->txn_type;
  assert(type < BOMB_TXN_TYPE_COUNT);
  uint64_t zero = 0;
  first_submit_time.compare_exchange_strong(zero, get_sys_clock());
  submitted[type].fetch_add(1);
  node_sum[type].fetch_add(nodes);
  uint64_t reads = 0, writes = 0, remote_reads = 0, remote_writes = 0;
  for (uint64_t i = 0; i < msg->requests.size(); ++i) {
    BombRequest *request = msg->requests[i];
    const bool remote = GET_NODE_ID(wl->request_to_part(*request)) != g_node_id;
    if (request->acctype == WR) {
      ++writes;
      if (remote) ++remote_writes;
    } else {
      ++reads;
      if (remote) ++remote_reads;
    }
  }
  read_sum[type].fetch_add(reads);
  write_sum[type].fetch_add(writes);
  remote_read_sum[type].fetch_add(remote_reads);
  remote_write_sum[type].fetch_add(remote_writes);
  if (type == BOMB_L1) {
    if (msg->source_id < 64) source_submitted[msg->source_id].fetch_add(1);
    uint64_t now = active_l1.fetch_add(1) + 1;
    uint64_t peak = peak_l1.load();
    while (now > peak && !peak_l1.compare_exchange_weak(peak, now)) {}
  }
}

void BombStats::record_complete(BombClientQueryMessage *msg, uint64_t latency,
                                bool was_aborted) {
  const uint64_t type = msg->txn_type;
  assert(type < BOMB_TXN_TYPE_COUNT);
  if (was_aborted) aborted[type].fetch_add(1);
  else committed[type].fetch_add(1);
  {
    std::lock_guard<std::mutex> guard(latency_mutex);
    latencies[type].push_back(latency);
  }
  if (type == BOMB_L1) {
    if (!was_aborted && msg->source_id < 64)
      source_committed[msg->source_id].fetch_add(1);
    active_l1.fetch_sub(1);
  }
}

void BombStats::record_abort_attempt(BombClientQueryMessage *msg) {
  const uint64_t type = msg->txn_type;
  assert(type < BOMB_TXN_TYPE_COUNT);
  aborted[type].fetch_add(1);
}

void BombStats::print(FILE *out) {
  static const char *names[BOMB_TXN_TYPE_COUNT] = {"l1","s1","s2","s3","s4","s5"};
  const uint64_t start = first_submit_time.load();
  const double seconds = start == 0 ? 0.0 :
      static_cast<double>(get_sys_clock() - start) / BILLION;
  uint64_t short_commits = 0;
  for (uint64_t t = 1; t < BOMB_TXN_TYPE_COUNT; ++t)
    short_commits += committed[t].load();
  fprintf(out, ",bomb_long_mode=%d,bomb_long_sources=%d,bomb_active_l1=%lu,bomb_peak_l1=%lu",
          BOMB_LONG_TX_MODE, BOMB_LONG_TX_SOURCES, active_l1.load(), peak_l1.load());
  fprintf(out, ",bomb_short_tput=%f", seconds > 0 ? short_commits / seconds : 0.0);
  std::lock_guard<std::mutex> guard(latency_mutex);
  for (uint64_t t = 0; t < BOMB_TXN_TYPE_COUNT; ++t) {
    const uint64_t sub = submitted[t].load();
    const uint64_t com = committed[t].load();
    std::vector<uint64_t> values = latencies[t];
    std::sort(values.begin(), values.end());
    auto pct = [&values](double p) -> uint64_t {
      if (values.empty()) return 0;
      return values[static_cast<uint64_t>((values.size() - 1) * p)];
    };
    uint64_t total = 0;
    for (uint64_t value : values) total += value;
    fprintf(out,
      ",bomb_%s_submitted=%lu,bomb_%s_committed=%lu,bomb_%s_aborted=%lu"
      ",bomb_%s_avg_latency_ns=%lu,bomb_%s_p50_ns=%lu,bomb_%s_p95_ns=%lu,bomb_%s_p99_ns=%lu"
      ",bomb_%s_avg_reads=%f,bomb_%s_avg_writes=%f,bomb_%s_avg_nodes=%f"
      ",bomb_%s_avg_remote_reads=%f,bomb_%s_avg_remote_writes=%f",
      names[t], sub, names[t], com, names[t], aborted[t].load(),
      names[t], values.empty() ? 0 : total / values.size(),
      names[t], pct(.50), names[t], pct(.95), names[t], pct(.99),
      names[t], sub ? static_cast<double>(read_sum[t].load()) / sub : 0.0,
      names[t], sub ? static_cast<double>(write_sum[t].load()) / sub : 0.0,
      names[t], sub ? static_cast<double>(node_sum[t].load()) / sub : 0.0,
      names[t], sub ? static_cast<double>(remote_read_sum[t].load()) / sub : 0.0,
      names[t], sub ? static_cast<double>(remote_write_sum[t].load()) / sub : 0.0);
  }
  for (uint64_t s = 0; s < BOMB_LONG_TX_SOURCES && s < 64; ++s)
    fprintf(out, ",bomb_l1_source%lu_submitted=%lu,bomb_l1_source%lu_committed=%lu",
            s, source_submitted[s].load(), s, source_committed[s].load());
}
RC BombWorkload::init() {
  Workload::init();
  const char *base = getenv("SCHEMA_PATH");
  std::string path = base ? std::string(base) + "BOMB_schema.txt"
                          : "./benchmarks/BOMB_schema.txt";
  printf("Initializing BoMB schema... "); fflush(stdout);
  init_schema(path.c_str());
  printf("Done\nInitializing BoMB data... "); fflush(stdout);
  RC rc = init_table();
  printf("Done\n"); fflush(stdout);
  return rc;
}

RC BombWorkload::init_schema(const char *schema_file) {
  Workload::init_schema(schema_file);
  const char *table_names[BOMB_TABLE_COUNT] = {
    "BOMB_BOM", "BOMB_PRODUCT", "BOMB_MATERIAL_COST",
    "BOMB_RESULT_COST", "BOMB_JOURNAL_VOUCHER"
  };
  const char *index_names[BOMB_TABLE_COUNT] = {
    "BOMB_BOM_IDX", "BOMB_PRODUCT_IDX", "BOMB_MATERIAL_COST_IDX",
    "BOMB_RESULT_COST_IDX", "BOMB_JOURNAL_VOUCHER_IDX"
  };
  for (uint32_t i = 0; i < BOMB_TABLE_COUNT; ++i) {
    bomb_tables[i] = tables[table_names[i]];
    bomb_indexes[i] = (INDEX *)indexes[index_names[i]];
    assert(bomb_tables[i] && bomb_indexes[i]);
  }
  return RCOK;
}

uint64_t BombWorkload::request_to_part(const BombRequest &request) const {
  // Product rows and their product->root BoM edges share a partition. This
  // preserves the natural ownership relation and makes S3's replacement
  // atomic on one Calvin participant. Material-tree edges remain distributed
  // by their material parent.
  if (request.table == BOMB_TABLE_PRODUCT)
    return hash64(request.key & 0xffffffffULL) % g_part_cnt;
  if (request.table == BOMB_TABLE_BOM)
    return hash64(request.key >> 32) % g_part_cnt;
  return hash64(request.key ^ (0x9e3779b97f4a7c15ULL * (request.table + 1)))
      % g_part_cnt;
}

INDEX *BombWorkload::request_index(const BombRequest &request) const {
  assert(request.table < BOMB_TABLE_COUNT);
  return bomb_indexes[request.table];
}

table_t *BombWorkload::request_table(const BombRequest &request) const {
  assert(request.table < BOMB_TABLE_COUNT);
  return bomb_tables[request.table];
}

RC BombWorkload::insert_row(BombTable table, uint64_t key,
    const std::vector<uint64_t> &u64_values,
    const std::vector<double> &double_values) {
  BombRequest request = {};
  request.table = table; request.key = key;
  const uint64_t part = request_to_part(request);
  if (GET_NODE_ID(part) != g_node_id) return RCOK;
  row_t *row = NULL; uint64_t row_id = 0;
  RC rc = bomb_tables[table]->get_new_row(row, part, row_id);
  assert(rc == RCOK && row != NULL);
  row->set_primary_key(key);
  uint32_t ui = 0, di = 0;
  Catalog *schema = bomb_tables[table]->get_schema();
  for (uint32_t col = 0; col < schema->get_field_cnt(); ++col) {
    if (schema->get_field_size(col) != 8) assert(false);
    // Quantity/cost columns are the only doubles in these compact schemas.
    const bool is_double =
      (table == BOMB_TABLE_BOM && col == 2) ||
      (table == BOMB_TABLE_PRODUCT && col == 2) ||
      (table == BOMB_TABLE_MATERIAL_COST && (col == 2 || col == 3)) ||
      (table == BOMB_TABLE_RESULT_COST && col == 2) ||
      (table == BOMB_TABLE_JOURNAL_VOUCHER && col == 4);
    if (is_double) {
      double value = double_values.at(di++);
      row->set_value(col, &value, sizeof(value));
    } else {
      uint64_t value = u64_values.at(ui++);
      row->set_value(col, &value, sizeof(value));
    }
  }
  itemid_t *item = static_cast<itemid_t *>(mem_allocator.alloc(sizeof(itemid_t)));
  item->type = DT_row; item->location = row; item->valid = true;
  rc = bomb_indexes[table]->index_insert(key, item, part);
  assert(rc == RCOK);
  return rc;
}

RC BombWorkload::init_table() {
  std::vector<uint64_t> roots, raw;
  std::vector<std::pair<uint64_t,uint64_t> > edges;
  // Shared BoM topology: product -> root material and material -> child.
  for (uint64_t p = 1; p <= BOMB_PRODUCT_TYPES; ++p) {
    BombQueryGenerator::product_roots(p, roots);
    for (uint64_t root : roots) {
      uint64_t child = BombQueryGenerator::item_material_start() + root * BOMB_TREE_SIZE;
      insert_row(BOMB_TABLE_BOM, BombQueryGenerator::composite_key(p, child),
                 {p, child, 1, 0}, {1.0});
    }
  }
  for (uint64_t tree = 0; tree < BombQueryGenerator::tree_count(); ++tree) {
    BombQueryGenerator::tree_edges(tree, edges, raw);
    for (const auto &edge : edges)
      insert_row(BOMB_TABLE_BOM,
                 BombQueryGenerator::composite_key(edge.first, edge.second),
                 {edge.first, edge.second, 1, 0}, {1.0});
  }
  for (uint64_t f = 1; f <= BOMB_FACTORY_COUNT; ++f) {
    for (uint64_t p = 1; p <= BOMB_TARGET_PRODUCTS; ++p) {
      uint64_t key = BombQueryGenerator::composite_key(f, p);
      insert_row(BOMB_TABLE_PRODUCT, key, {f, p, 1, 0}, {1.0});
      insert_row(BOMB_TABLE_RESULT_COST, key, {f, p}, {0.0});
    }
    for (uint64_t r = 0; r < BOMB_RAW_MATERIAL_TYPES; ++r) {
      uint64_t item = BombQueryGenerator::item_raw_start() + r;
      insert_row(BOMB_TABLE_MATERIAL_COST,
                 BombQueryGenerator::composite_key(f, item),
                 {f, item}, {1.0, 1.0});
    }
    for (uint64_t slot = 1; slot <= 1024ULL * BOMB_TARGET_PRODUCTS; ++slot) {
      insert_row(BOMB_TABLE_JOURNAL_VOUCHER,
                 BombQueryGenerator::composite_key(f, slot),
                 {slot, 20220101235959ULL, 0, 1, 0}, {0.0});
    }
  }
  return RCOK;
}

RC BombWorkload::get_txn_man(TxnManager *&txn_manager) {
  txn_manager = static_cast<BombTxnManager *>(
      mem_allocator.align_alloc(sizeof(BombTxnManager)));
  new(txn_manager) BombTxnManager();
  return RCOK;
}
