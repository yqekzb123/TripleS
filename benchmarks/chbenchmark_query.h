#ifndef _CHBENCHMARK_QUERY_H_
#define _CHBENCHMARK_QUERY_H_

#include "tpcc_query.h"

class CHBenchmarkQueryMessage;
class CHBenchmarkClientQueryMessage;

enum CHBenchmarkTxnType {
  CH_PAYMENT = 1,
  CH_NEW_ORDER,
  CH_ORDER_STATUS,
  CH_DELIVERY,
  CH_STOCK_LEVEL,
  CH_Q1 = 101,
  CH_Q2,
  CH_Q3,
  CH_Q4,
  CH_Q5,
  CH_Q6,
  CH_Q7,
  CH_Q8,
  CH_Q9,
  CH_Q10,
  CH_Q11,
  CH_Q12,
  CH_Q13,
  CH_Q14,
  CH_Q15,
  CH_Q16,
  CH_Q17,
  CH_Q18,
  CH_Q19,
  CH_Q20,
  CH_Q21,
  CH_Q22
};

class CHBenchmarkQuery : public TPCCQuery {
public:
  void init(uint64_t thd_id, Workload *wl);
  void init();
  void reset();
  void release();
  void print();
  bool readonly();
  bool is_olap() const;
  uint64_t query_number() const;
  uint64_t participants(bool *&pps, Workload *wl);
  uint64_t get_participants(Workload *wl);
  static std::set<uint64_t> participants(Message *msg, Workload *wl);

  CHBenchmarkTxnType ch_txn_type;
  uint64_t ch_wh_start;
  uint64_t ch_wh_count;
};

class CHBenchmarkQueryGenerator : public QueryGenerator {
public:
  void init();
  BaseQuery *create_query(Workload *wl, uint64_t home_partition_id);

private:
  TPCCQueryGenerator tpcc_generator;
  myrand *mrand;
  uint64_t next_olap_query;
  CHBenchmarkQuery *make_olap(uint64_t home_partition_id);
  CHBenchmarkQuery *wrap_tpcc(TPCCQuery *source);
};

#endif
