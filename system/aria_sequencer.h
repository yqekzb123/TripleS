#ifndef _ARIASEQUENCER_H_
#define _ARIASEQUENCER_H_

#include "global.h"
#include "query.h"
#include <vector>
#include <boost/lockfree/queue.hpp>

class Workload;
class BaseQuery;
class Message;

#if CC_ALG == ARIA

typedef struct aria_txn_entry {
    BaseQuery * qry;
    uint32_t client_id;
    uint64_t client_startts;
    uint64_t seq_startts;
    uint64_t seq_first_startts;
    // uint64_t skew_startts;
    uint64_t total_batch_time;
    // uint32_t server_ack_cnt;
    uint32_t abort_cnt;
    Message * msg;
} aria_txn;

#if LONG_TXN_SCHEDULE
struct PBatch {
    uint64_t id;
    std::vector<aria_txn*> txns;
    uint64_t txns_left;
    bool sent;
    PBatch(uint64_t _id) : id(_id), txns_left(0), sent(false) {}
};
#endif

class AriaSequencer {
public:
    void init(Workload * wl);
    void process_ack(Message * msg, uint64_t thd_id);
    void process_txn(Message* msg, uint64_t thd_id);
    void send_next_batch(uint64_t thd_id);
    void fill_batch(uint64_t _thd_id);
    #if LONG_TXN_SCHEDULE
    void put_one_txn_to_batch(uint64_t _thd_id);
    bool is_batch_full() { return aria_batch.size() >= g_aria_batch_size; }
    #endif
private:
    volatile uint64_t next_txn_id;
    volatile uint64_t batch_id;
    uint64_t last_batch_time;
    uint64_t txns_left;
    Workload * _wl;
    vector<aria_txn *> aria_batch;
    #if LONG_TXN_SCHEDULE
    std::vector<PBatch*> pipeline_batches;
    PBatch * pipeline_current_batch = nullptr;
    uint64_t pipeline_next_batch_id = 0;
    uint64_t pipeline_txns_left = 0;
    #endif
};



#endif

#endif
