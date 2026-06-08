#ifndef _CARACALEQUENCER_H_
#define _CARACALEQUENCER_H_

#include "global.h"
#include "query.h"
#include <boost/lockfree/queue.hpp>

class Workload;
class BaseQuery;
class Message;

#if CC_ALG == CARACAL

typedef struct caracal_txn_entry {
    BaseQuery * qry;
    uint32_t client_id;
    uint64_t client_startts;
    uint64_t seq_startts;
    uint64_t seq_first_startts;
    // uint64_t skew_startts;
    uint64_t total_batch_time;
    uint32_t server_ack_cnt;
    uint32_t abort_cnt;
    Message * msg;
} caracal_txn;

class CaracalSequencer {
public:
    void init(Workload * wl);
    void process_ack(Message * msg, uint64_t thd_id);
    void process_txn(Message* msg, uint64_t thd_id);
    void send_next_batch(uint64_t thd_id);
    void fill_batch(uint64_t _thd_id);
    uint64_t get_batch_id() { return batch_id; }

    uint64_t get_total_ack_count() { return total_ack_count; }

private:
    volatile uint64_t next_txn_id;
    volatile uint64_t batch_id;
    uint64_t last_batch_time;
    uint64_t txns_left;
    Workload * _wl;
    vector<caracal_txn *> caracal_batch;

    // 每个阶段的总预期ACK数量
    uint64_t total_ack_count;

    boost::lockfree::queue<Message*, boost::lockfree::capacity<65526> > * fill_queue;
};
#endif
#endif
