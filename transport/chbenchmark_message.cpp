#include "message.h"

#include "chbenchmark.h"

void CHBenchmarkClientQueryMessage::init() { TPCCClientQueryMessage::init(); }
void CHBenchmarkClientQueryMessage::release() { TPCCClientQueryMessage::release(); }

uint64_t CHBenchmarkClientQueryMessage::get_size() {
  return TPCCClientQueryMessage::get_size() + sizeof(uint64_t) * 3;
}

void CHBenchmarkClientQueryMessage::copy_from_query(BaseQuery *base_query) {
  TPCCClientQueryMessage::copy_from_query(base_query);
  CHBenchmarkQuery *query = static_cast<CHBenchmarkQuery *>(base_query);
  ch_txn_type = query->ch_txn_type;
  ch_wh_start = query->ch_wh_start;
  ch_wh_count = query->ch_wh_count;
}

void CHBenchmarkClientQueryMessage::copy_from_txn(TxnManager *txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
  copy_from_query(txn->query);
}

void CHBenchmarkClientQueryMessage::copy_to_txn(TxnManager *txn) {
  CHBenchmarkQuery *query = static_cast<CHBenchmarkQuery *>(txn->query);
  query->ch_txn_type = static_cast<CHBenchmarkTxnType>(ch_txn_type);
  query->ch_wh_start = ch_wh_start;
  query->ch_wh_count = ch_wh_count;
  TPCCClientQueryMessage::copy_to_txn(txn);
}

void CHBenchmarkClientQueryMessage::copy_from_buf(char *buf) {
  TPCCClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = TPCCClientQueryMessage::get_size();
  COPY_VAL(ch_txn_type, buf, ptr);
  COPY_VAL(ch_wh_start, buf, ptr);
  COPY_VAL(ch_wh_count, buf, ptr);
  assert(ptr == get_size());
}

void CHBenchmarkClientQueryMessage::copy_to_buf(char *buf) {
  TPCCClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = TPCCClientQueryMessage::get_size();
  COPY_BUF(buf, ch_txn_type, ptr);
  COPY_BUF(buf, ch_wh_start, ptr);
  COPY_BUF(buf, ch_wh_count, ptr);
  assert(ptr == get_size());
}

void CHBenchmarkQueryMessage::init() { TPCCQueryMessage::init(); }
void CHBenchmarkQueryMessage::release() { TPCCQueryMessage::release(); }

uint64_t CHBenchmarkQueryMessage::get_size() {
  return TPCCQueryMessage::get_size() + sizeof(uint64_t) * 3;
}

void CHBenchmarkQueryMessage::copy_from_txn(TxnManager *txn) {
  TPCCQueryMessage::copy_from_txn(txn);
  CHBenchmarkQuery *query = static_cast<CHBenchmarkQuery *>(txn->query);
  ch_txn_type = query->ch_txn_type;
  ch_wh_start = query->ch_wh_start;
  ch_wh_count = query->ch_wh_count;
}

void CHBenchmarkQueryMessage::copy_to_txn(TxnManager *txn) {
  CHBenchmarkQuery *query = static_cast<CHBenchmarkQuery *>(txn->query);
  query->ch_txn_type = static_cast<CHBenchmarkTxnType>(ch_txn_type);
  query->ch_wh_start = ch_wh_start;
  query->ch_wh_count = ch_wh_count;
  TPCCQueryMessage::copy_to_txn(txn);
}

void CHBenchmarkQueryMessage::copy_from_buf(char *buf) {
  TPCCQueryMessage::copy_from_buf(buf);
  uint64_t ptr = TPCCQueryMessage::get_size();
  COPY_VAL(ch_txn_type, buf, ptr);
  COPY_VAL(ch_wh_start, buf, ptr);
  COPY_VAL(ch_wh_count, buf, ptr);
  assert(ptr == get_size());
}

void CHBenchmarkQueryMessage::copy_to_buf(char *buf) {
  TPCCQueryMessage::copy_to_buf(buf);
  uint64_t ptr = TPCCQueryMessage::get_size();
  COPY_BUF(buf, ch_txn_type, ptr);
  COPY_BUF(buf, ch_wh_start, ptr);
  COPY_BUF(buf, ch_wh_count, ptr);
  assert(ptr == get_size());
}
