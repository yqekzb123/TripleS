#include "message.h"

#include "bomb.h"
#include "mem_alloc.h"

namespace {
uint64_t request_wire_size() {
  return sizeof(uint32_t) * 2 + sizeof(access_t) + sizeof(uint64_t) * 4 +
         sizeof(double);
}

void release_requests(Array<BombRequest *> &requests) {
  for (uint64_t i = 0; i < requests.size(); ++i)
    mem_allocator.free(requests[i], sizeof(BombRequest));
  requests.release();
}

void copy_request_to_buf(char *buf, uint64_t &ptr, const BombRequest &request) {
  COPY_BUF(buf, request.table, ptr);
  COPY_BUF(buf, request.acctype, ptr);
  COPY_BUF(buf, request.role, ptr);
  COPY_BUF(buf, request.key, ptr);
  COPY_BUF(buf, request.expected_version, ptr);
  COPY_BUF(buf, request.arg0, ptr);
  COPY_BUF(buf, request.arg1, ptr);
  COPY_BUF(buf, request.value, ptr);
}

BombRequest *copy_request_from_buf(char *buf, uint64_t &ptr) {
  BombRequest *request = static_cast<BombRequest *>(
      mem_allocator.alloc(sizeof(BombRequest)));
  COPY_VAL(request->table, buf, ptr);
  COPY_VAL(request->acctype, buf, ptr);
  COPY_VAL(request->role, buf, ptr);
  COPY_VAL(request->key, buf, ptr);
  COPY_VAL(request->expected_version, buf, ptr);
  COPY_VAL(request->arg0, buf, ptr);
  COPY_VAL(request->arg1, buf, ptr);
  COPY_VAL(request->value, buf, ptr);
  return request;
}

void clone_requests(Array<BombRequest *> &dst,
                    Array<BombRequest *> &src) {
  dst.init(src.size() == 0 ? 1 : src.size());
  for (uint64_t i = 0; i < src.size(); ++i) {
    BombRequest *copy = static_cast<BombRequest *>(
        mem_allocator.alloc(sizeof(BombRequest)));
    *copy = *src[i];
    dst.add(copy);
  }
}
}

void BombClientQueryMessage::init() { ClientQueryMessage::init(); }

void BombClientQueryMessage::release() {
  release_requests(requests);
  ClientQueryMessage::release();
}

uint64_t BombClientQueryMessage::get_size() {
  return ClientQueryMessage::get_size() + sizeof(uint64_t) * 6 +
         requests.size() * request_wire_size();
}

void BombClientQueryMessage::copy_from_query(BaseQuery *base) {
  ClientQueryMessage::copy_from_query(base);
  BombQuery *query = static_cast<BombQuery *>(base);
  txn_type = query->txn_type;
  factory_id = query->factory_id;
  ordinal = query->ordinal;
  source_id = query->source_id;
  plan_epoch = query->plan_epoch;
  // The client sends only this deterministic logical descriptor. In
  // particular, L1 does not put its thousands of expanded keys on the
  // client-to-sequencer link.
  requests.init(1);
}

void BombClientQueryMessage::copy_from_txn(TxnManager *txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
  BombQuery *query = static_cast<BombQuery *>(txn->query);
  txn_type = query->txn_type;
  factory_id = query->factory_id;
  ordinal = query->ordinal;
  source_id = query->source_id;
  plan_epoch = query->plan_epoch;
  clone_requests(requests, query->requests);
}

void BombClientQueryMessage::materialize_requests() {
  if (!requests.is_empty()) return;
  BombQuery query;
  query.init();
  query.txn_type = static_cast<BombTxnType>(txn_type);
  query.factory_id = factory_id;
  query.ordinal = ordinal;
  query.source_id = source_id;
  query.plan_epoch = plan_epoch;
  BombQueryGenerator::materialize(&query);
  if (ordinal == 2065)
    fprintf(stderr, "MAT-REQS ord=%lu src=%lu type=%d factory=%lu count=%lu\n",
            ordinal, source_id, (int)txn_type, factory_id, query.requests.size());
  requests.release();
  clone_requests(requests, query.requests);
  query.release();
}

void BombClientQueryMessage::copy_to_txn(TxnManager *txn) {
  if (requests.is_empty()) {
    // Defensive re-materialization: a late duplicate ACK at the sequencer may
    // release() this message's requests after it was already re-enqueued for
    // a retry batch. The six logical descriptor fields survive release(), so
    // the deterministic plan can be rebuilt instead of running an empty plan.
    fprintf(stderr, "CPY2TXN-REMAT txn=%ld ord=%lu src=%lu\n",
            txn->get_txn_id(), ordinal, source_id);
    materialize_requests();
  }
  BombQuery *q_before = static_cast<BombQuery *>(txn->query);
  fprintf(stderr, "CPY2TXN txn=%ld batch=%ld ord=%lu src=%lu stats_abort=%lu man_abort=%lu msg_reqs=%lu qry_reqs_before=%lu qry_parts_before=%lu epoch_msg=%lu\n",
          txn->get_txn_id(), txn->get_batch_id(), ordinal, source_id,
          txn->txn_stats.abort_cnt, txn->abort_cnt, requests.size(),
          q_before->requests.size(), q_before->partitions_touched.size(), plan_epoch);
  ClientQueryMessage::copy_to_txn(txn);
  BombQuery *query = static_cast<BombQuery *>(txn->query);
  query->txn_type = static_cast<BombTxnType>(txn_type);
  query->factory_id = factory_id;
  query->ordinal = ordinal;
  query->source_id = source_id;
  query->plan_epoch = plan_epoch;
  query->planned = true;
  clone_requests(query->requests, requests);
}

void BombClientQueryMessage::copy_from_buf(char *buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  COPY_VAL(txn_type, buf, ptr);
  COPY_VAL(factory_id, buf, ptr);
  COPY_VAL(ordinal, buf, ptr);
  COPY_VAL(source_id, buf, ptr);
  COPY_VAL(plan_epoch, buf, ptr);
  uint64_t count = 0;
  COPY_VAL(count, buf, ptr);
  requests.init(count == 0 ? 1 : count);
  for (uint64_t i = 0; i < count; ++i)
    requests.add(copy_request_from_buf(buf, ptr));
  assert(ptr == get_size());
}

void BombClientQueryMessage::copy_to_buf(char *buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  COPY_BUF(buf, txn_type, ptr);
  COPY_BUF(buf, factory_id, ptr);
  COPY_BUF(buf, ordinal, ptr);
  COPY_BUF(buf, source_id, ptr);
  COPY_BUF(buf, plan_epoch, ptr);
  uint64_t count = requests.size();
  COPY_BUF(buf, count, ptr);
  for (uint64_t i = 0; i < count; ++i)
    copy_request_to_buf(buf, ptr, *requests[i]);
  assert(ptr == get_size());
}

void BombQueryMessage::init() {}

void BombQueryMessage::release() { release_requests(requests); }

uint64_t BombQueryMessage::get_size() {
  return QueryMessage::get_size() + sizeof(uint64_t) * 6 +
         requests.size() * request_wire_size();
}

void BombQueryMessage::copy_from_txn(TxnManager *txn) {
  QueryMessage::copy_from_txn(txn);
  BombQuery *query = static_cast<BombQuery *>(txn->query);
  txn_type = query->txn_type;
  factory_id = query->factory_id;
  ordinal = query->ordinal;
  source_id = query->source_id;
  plan_epoch = query->plan_epoch;
  clone_requests(requests, query->requests);
}

void BombQueryMessage::copy_to_txn(TxnManager *txn) {
  QueryMessage::copy_to_txn(txn);
  BombQuery *query = static_cast<BombQuery *>(txn->query);
  query->txn_type = static_cast<BombTxnType>(txn_type);
  query->factory_id = factory_id;
  query->ordinal = ordinal;
  query->source_id = source_id;
  query->plan_epoch = plan_epoch;
  query->planned = true;
  clone_requests(query->requests, requests);
}

void BombQueryMessage::copy_from_buf(char *buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  COPY_VAL(txn_type, buf, ptr);
  COPY_VAL(factory_id, buf, ptr);
  COPY_VAL(ordinal, buf, ptr);
  COPY_VAL(source_id, buf, ptr);
  COPY_VAL(plan_epoch, buf, ptr);
  uint64_t count = 0;
  COPY_VAL(count, buf, ptr);
  requests.init(count == 0 ? 1 : count);
  for (uint64_t i = 0; i < count; ++i)
    requests.add(copy_request_from_buf(buf, ptr));
  assert(ptr == get_size());
}

void BombQueryMessage::copy_to_buf(char *buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  COPY_BUF(buf, txn_type, ptr);
  COPY_BUF(buf, factory_id, ptr);
  COPY_BUF(buf, ordinal, ptr);
  COPY_BUF(buf, source_id, ptr);
  COPY_BUF(buf, plan_epoch, ptr);
  uint64_t count = requests.size();
  COPY_BUF(buf, count, ptr);
  for (uint64_t i = 0; i < count; ++i)
    copy_request_to_buf(buf, ptr, *requests[i]);
  assert(ptr == get_size());
}
