#ifndef _ARIASEQUENCER_H_
#define _ARIASEQUENCER_H_

#include "global.h"
#include "query.h"
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
    // Whether an ACK of the current batch has already been consumed for this
    // txn. A distributed txn can emit a second ACK within the same batch
    // (abort + in-batch retry, or the RACK_FIN completion path); counting
    // those twice would drain txns_left early and advance the phase while
    // other txns' ACKs are still queued, so duplicate ACKs are ignored.
    bool acked;
    Message * msg;
} aria_txn;

class AriaSequencer {
public:
    void init(Workload * wl);
    void process_ack(Message * msg, uint64_t thd_id);
    void process_txn(Message* msg, uint64_t thd_id);
    void send_next_batch(uint64_t thd_id);
    void fill_batch(uint64_t _thd_id);
    // Deferred-release bookkeeping: messages of completed txns must not be
    // released while a re-sent copy may still be in flight at the workers.
    void retire_stale_messages(uint64_t current_batch);
    uint64_t get_batch_id() { return batch_id; }

private:
    volatile uint64_t next_txn_id;
    volatile uint64_t batch_id;
    uint64_t last_batch_time;
    uint64_t txns_left;
    Workload * _wl;
    vector<aria_txn *> aria_batch;
    // Messages whose txn completed (RCOK ACK processed). Kept alive until
    // two batches past retirement so that any in-flight re-sent copy can
    // still clone a fully valid plan.
    vector<Message *> retired_msgs;
    vector<uint64_t> retired_batches;
};
#endif
#endif
