#include "bomb_query.h"
#include "helper.h"

#include "bomb.h"
#include "mem_alloc.h"
#include "message.h"

#include <algorithm>
#include <map>

std::atomic<uint64_t> BombQueryGenerator::next_ordinal(0);
std::atomic<bool> *BombQueryGenerator::source_busy = NULL;
uint64_t BombQueryGenerator::source_count = 0;

BombQuery::BombQuery()
    : txn_type(BOMB_S1), factory_id(1), ordinal(0), source_id(UINT64_MAX),
      plan_epoch(0), planned(false) {}

void BombQuery::init() {
  BaseQuery::init();
}

void BombQuery::init(uint64_t, Workload *) { init(); }

void BombQuery::reset() {
  BOMB_TRACE("BQ-RESET ord=%lu src=%lu reqs_old=%lu parts_old=%lu planned=%d\n",
          ordinal, source_id, requests.size(), partitions_touched.size(), (int)planned);
  BombQueryGenerator::release_plan(this);
  BaseQuery::clear();
  planned = false;
}

void BombQuery::release() {
  BombQueryGenerator::release_plan(this);
  requests.release();
  BaseQuery::release();
}

void BombQuery::print() {
  printf("BoMB txn=%u factory=%lu ordinal=%lu requests=%lu epoch=%lu\n",
         static_cast<uint32_t>(txn_type), factory_id, ordinal,
         requests.size(), plan_epoch);
}

bool BombQuery::readonly() { return false; }

uint64_t BombQuery::get_participants(Workload *wl) {
  assert(participant_nodes.size() == 0 && active_nodes.size() == 0);
  for (uint64_t i = 0; i < g_node_cnt; ++i) {
    participant_nodes.add(0);
    active_nodes.add(0);
  }
  bool *seen = new bool[g_node_cnt]();
  uint64_t count = 0;
  BombWorkload *bomb = static_cast<BombWorkload *>(wl);
  for (uint64_t i = 0; i < requests.size(); ++i) {
    uint64_t node = GET_NODE_ID(bomb->request_to_part(*requests[i]));
    participant_nodes.set(node, 1);
    if (requests[i]->acctype == WR) active_nodes.set(node, 1);
    if (!seen[node]) { seen[node] = true; ++count; }
  }
  delete [] seen;
  return count;
}

std::set<uint64_t> BombQuery::participants(Message *msg, Workload *wl) {
  std::set<uint64_t> result;
  BombClientQueryMessage *bomb_msg = static_cast<BombClientQueryMessage *>(msg);
  BombWorkload *bomb = static_cast<BombWorkload *>(wl);
  for (uint64_t i = 0; i < bomb_msg->requests.size(); ++i)
    result.insert(GET_NODE_ID(bomb->request_to_part(*bomb_msg->requests[i])));
  return result;
}

BombQueryGenerator::BombQueryGenerator() {}

uint64_t BombQueryGenerator::mix_hash(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

uint64_t BombQueryGenerator::composite_key(uint64_t first, uint64_t second) {
  return (first << 32) | (second & 0xffffffffULL);
}

uint64_t BombQueryGenerator::item_material_start() {
  return BOMB_PRODUCT_TYPES + 1;
}

uint64_t BombQueryGenerator::item_raw_start() {
  return item_material_start() + BOMB_MATERIAL_TYPES;
}

uint64_t BombQueryGenerator::tree_count() {
  return std::max<uint64_t>(1, BOMB_MATERIAL_TYPES / BOMB_TREE_SIZE);
}

void BombQueryGenerator::product_roots(uint64_t product_id,
                                       std::vector<uint64_t> &roots) {
  roots.clear();
  std::set<uint64_t> unique;
  uint64_t salt = 0;
  while (unique.size() < BOMB_TREES_PER_PRODUCT) {
    unique.insert(mix_hash(product_id * 1315423911ULL + salt++) % tree_count());
  }
  roots.assign(unique.begin(), unique.end());
}

void BombQueryGenerator::tree_edges(
    uint64_t root_index, std::vector<std::pair<uint64_t,uint64_t> > &edges,
    std::vector<uint64_t> &raw_leaves) {
  edges.clear(); raw_leaves.clear();
  const uint64_t base = item_material_start() + root_index * BOMB_TREE_SIZE;
  std::vector<uint64_t> child_count(BOMB_TREE_SIZE, 0);
  for (uint64_t i = 1; i < BOMB_TREE_SIZE; ++i) {
    uint64_t parent = mix_hash(root_index * 65537ULL + i) % i;
    edges.push_back(std::make_pair(base + parent, base + i));
    child_count[parent]++;
  }
  for (uint64_t i = 0; i < BOMB_TREE_SIZE; ++i) {
    if (child_count[i] != 0) continue;
    for (uint64_t r = 0; r < BOMB_RAW_MATERIALS_PER_LEAF; ++r) {
      uint64_t raw = item_raw_start() +
          (mix_hash(root_index * 104729ULL + i * 17 + r) % BOMB_RAW_MATERIAL_TYPES);
      edges.push_back(std::make_pair(base + i, raw));
      raw_leaves.push_back(raw);
    }
  }
}

BombTxnType BombQueryGenerator::choose_short(uint64_t ordinal) {
  uint64_t pct = mix_hash(ordinal) % 100;
  if (pct < BOMB_S1_PCT) return BOMB_S1;
  pct -= BOMB_S1_PCT;
  if (pct < BOMB_S2_PCT) return BOMB_S2;
  pct -= BOMB_S2_PCT;
  if (pct < BOMB_S3_PCT) return BOMB_S3;
  pct -= BOMB_S3_PCT;
  if (pct < BOMB_S4_PCT) return BOMB_S4;
  return BOMB_S5;
}

BaseQuery *BombQueryGenerator::create_query(Workload *wl, uint64_t home) {
  return create_for_client(wl, home, UINT64_MAX);
}

BombQuery *BombQueryGenerator::create_for_client(Workload *, uint64_t home,
                                                  uint64_t client_thread) {
  BombQuery *query = new BombQuery();
  query->init();
  prepare_next(query, home, client_thread);
  return query;
}

void BombQueryGenerator::prepare_next(BombQuery *query, uint64_t home,
                                      uint64_t client_thread) {
  release_plan(query);
  query->ordinal = next_ordinal.fetch_add(1);
  query->factory_id = 1 + (mix_hash(query->ordinal ^ home) % BOMB_FACTORY_COUNT);
  query->source_id = client_thread == UINT64_MAX ? UINT64_MAX :
      (g_node_id - g_node_cnt) * g_client_thread_cnt + client_thread;
  query->txn_type = is_long_source(client_thread) ? BOMB_L1 :
      (BOMB_FORCE_SHORT_TYPE >= static_cast<int>(BOMB_S1)
       ? static_cast<BombTxnType>(BOMB_FORCE_SHORT_TYPE)
       : choose_short(query->ordinal));
  query->plan_epoch = 0;
  query->rwset_known = true;
  query->rwset_variable = false;
}

void BombQueryGenerator::init_source_state(uint64_t thread_count) {
  if (source_busy != NULL) return;
  source_count = thread_count;
  source_busy = new std::atomic<bool>[thread_count];
  for (uint64_t i = 0; i < thread_count; ++i) source_busy[i].store(false);
}

bool BombQueryGenerator::is_long_source(uint64_t client_thread) {
  if (client_thread == UINT64_MAX) return false;
  const uint64_t source_id =
      (g_node_id - g_node_cnt) * g_client_thread_cnt + client_thread;
  if (BOMB_LONG_TX_SOURCES == 0) return false;
  if (BOMB_LONG_TX_MODE == BOMB_LONG_TX_GLOBAL) return source_id == 0;
  return source_id < BOMB_LONG_TX_SOURCES;
}

bool BombQueryGenerator::is_enabled_client(uint64_t client_thread) {
  if (client_thread == UINT64_MAX) return true;
  const uint64_t client_id =
      (g_node_id - g_node_cnt) * g_client_thread_cnt + client_thread;
  const uint64_t long_count = BOMB_LONG_TX_MODE == BOMB_LONG_TX_GLOBAL
      ? (BOMB_LONG_TX_SOURCES == 0 ? 0 : 1) : BOMB_LONG_TX_SOURCES;
#if CC_ALG == ARIA || CC_ALG == SDMVCC
  // Every Aria/SDMVCC server owns a sequencer that must fill a same-sized batch.
  // Keep long sources globally unique, but provision BOMB_SHORT_WORKERS local
  // short sources for every paired client/server so no sequencer starves.
  if (client_id < long_count) return true;
  const uint64_t client_node = g_node_id - g_node_cnt;
  const uint64_t node_begin = client_node * g_client_thread_cnt;
  const uint64_t node_end = node_begin + g_client_thread_cnt;
  const uint64_t local_long_count = long_count <= node_begin ? 0 :
      (long_count >= node_end ? g_client_thread_cnt : long_count - node_begin);
  return client_thread >= local_long_count &&
      client_thread < local_long_count + BOMB_SHORT_WORKERS;
#else
  return client_id < long_count + BOMB_SHORT_WORKERS;
#endif
}

bool BombQueryGenerator::try_begin_long(uint64_t client_thread) {
  if (!is_long_source(client_thread)) return true;
  assert(source_busy != NULL && client_thread < source_count);
  bool expected = false;
  return source_busy[client_thread].compare_exchange_strong(expected, true);
}

void BombQueryGenerator::complete_long(uint64_t client_thread) {
  if (client_thread == UINT64_MAX) return;
  const uint64_t local_thread = client_thread % g_client_thread_cnt;
  if (!is_long_source(local_thread)) return;
  assert(source_busy != NULL && local_thread < source_count);
  source_busy[local_thread].store(false);
}

void BombQueryGenerator::add_request(BombQuery *query, BombTable table,
    access_t access, BombRequestRole role, uint64_t key, uint64_t arg0,
    uint64_t arg1, double value, uint64_t expected_version) {
  const std::pair<uint32_t, uint64_t> index_key(table, key);
  auto found = query->request_index.find(index_key);
  if (found != query->request_index.end()) {
    BombRequest *old = found->second;
    if (access == WR) { old->acctype = WR; old->role = role; }
    return;
  }
  BombRequest *request = new BombRequest();
  request->table = table; request->acctype = access; request->role = role;
  request->key = key; request->expected_version = expected_version;
  request->arg0 = arg0; request->arg1 = arg1; request->value = value;
  query->requests.add(request);
  query->request_index[index_key] = request;
}

void BombQueryGenerator::plan_l1(BombQuery *query) {
  std::vector<uint64_t> roots, raw;
  std::vector<std::pair<uint64_t,uint64_t> > edges;
  for (uint64_t p = 1; p <= BOMB_TARGET_PRODUCTS; ++p) {
    add_request(query, BOMB_TABLE_PRODUCT, RD, BOMB_ROLE_READ,
                composite_key(query->factory_id, p));
    product_roots(p, roots);
    for (uint64_t root : roots) {
      const uint64_t root_item = item_material_start() + root * BOMB_TREE_SIZE;
      add_request(query, BOMB_TABLE_BOM, RD, BOMB_ROLE_READ,
                  composite_key(p, root_item));
      tree_edges(root, edges, raw);
      for (const auto &edge : edges)
        add_request(query, BOMB_TABLE_BOM, RD, BOMB_ROLE_READ,
                    composite_key(edge.first, edge.second));
      for (uint64_t raw_id : raw)
        add_request(query, BOMB_TABLE_MATERIAL_COST, RD, BOMB_ROLE_READ,
                    composite_key(query->factory_id, raw_id));
    }
    add_request(query, BOMB_TABLE_RESULT_COST, WR, BOMB_ROLE_RESULT_WRITE,
                composite_key(query->factory_id, p), p);
  }
}

void BombQueryGenerator::plan_s1(BombQuery *query) {
  std::set<uint64_t> materials;
  uint64_t salt = 0;
  while (materials.size() < BOMB_TARGET_MATERIALS)
    materials.insert(item_raw_start() +
      mix_hash(query->ordinal + salt++) % BOMB_RAW_MATERIAL_TYPES);
  for (uint64_t raw : materials)
    add_request(query, BOMB_TABLE_MATERIAL_COST, WR, BOMB_ROLE_MATERIAL_RMW,
                composite_key(query->factory_id, raw));
}

void BombQueryGenerator::plan_s2(BombQuery *query) {
  const uint64_t batch = query->ordinal % 1024;
  for (uint64_t p = 1; p <= BOMB_TARGET_PRODUCTS; ++p) {
    add_request(query, BOMB_TABLE_RESULT_COST, RD, BOMB_ROLE_READ,
                composite_key(query->factory_id, p));
    add_request(query, BOMB_TABLE_JOURNAL_VOUCHER, WR,
                BOMB_ROLE_VOUCHER_WRITE,
                composite_key(query->factory_id,
                              batch * BOMB_TARGET_PRODUCTS + p), p);
  }
}

void BombQueryGenerator::plan_s3(BombQuery *query) {
  const uint64_t product = 1 + mix_hash(query->ordinal) % BOMB_TARGET_PRODUCTS;
  const uint64_t replacement = BOMB_PRODUCT_TYPES + 1 +
      (mix_hash(query->ordinal ^ 0x5333ULL) % BOMB_TARGET_PRODUCTS);
  const uint64_t expected = BOMB_INJECT_STALE_PRESET
      ? UINT64_MAX : query->plan_epoch;
  add_request(query, BOMB_TABLE_PRODUCT, WR, BOMB_ROLE_PRODUCT_REPLACE,
              composite_key(query->factory_id, product), replacement,
              product, 1.0, expected);
  std::vector<uint64_t> roots;
  product_roots(product, roots);
  for (uint64_t root : roots) {
    const uint64_t root_item = item_material_start() + root * BOMB_TREE_SIZE;
    add_request(query, BOMB_TABLE_BOM, WR, BOMB_ROLE_PRODUCT_REPLACE,
                composite_key(product, root_item), replacement, product,
                1.0, expected);
  }
}

void BombQueryGenerator::plan_s4(BombQuery *query) {
  const uint64_t tree = mix_hash(query->ordinal) % tree_count();
  std::vector<std::pair<uint64_t,uint64_t> > edges;
  std::vector<uint64_t> raw;
  tree_edges(tree, edges, raw);
  assert(!edges.empty());
  const auto &edge = edges[mix_hash(query->ordinal ^ 0x5334ULL) % edges.size()];
  const uint64_t replacement = item_raw_start() +
      mix_hash(query->ordinal ^ 0x424f4dULL) % BOMB_RAW_MATERIAL_TYPES;
  const uint64_t expected = BOMB_INJECT_STALE_PRESET
      ? UINT64_MAX : query->plan_epoch;
  add_request(query, BOMB_TABLE_BOM, WR, BOMB_ROLE_BOM_REPLACE,
              composite_key(edge.first, edge.second), replacement,
              edge.second, 1.0, expected);
}

void BombQueryGenerator::plan_s5(BombQuery *query) {
  const uint64_t product = 1 + mix_hash(query->ordinal) % BOMB_TARGET_PRODUCTS;
  add_request(query, BOMB_TABLE_PRODUCT, WR, BOMB_ROLE_QUANTITY_WRITE,
              composite_key(query->factory_id, product), product, 0,
              10.0, query->plan_epoch);
}

void BombQueryGenerator::materialize(BombQuery *query) {
  if (query->planned) return;
  if (!query->requests.initialized()) query->requests.init(8);
  query->request_index.clear();
  switch (query->txn_type) {
    case BOMB_L1: plan_l1(query); break;
    case BOMB_S1: plan_s1(query); break;
    case BOMB_S2: plan_s2(query); break;
    case BOMB_S3: plan_s3(query); break;
    case BOMB_S4: plan_s4(query); break;
    case BOMB_S5: plan_s5(query); break;
    default: assert(false); break;
  }
  query->planned = true;
}

void BombQueryGenerator::release_plan(BombQuery *query) {
  for (uint64_t i = 0; i < query->requests.size(); ++i)
    delete query->requests[i];
  query->requests.clear();
  query->request_index.clear();
  query->planned = false;
}
