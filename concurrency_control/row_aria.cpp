#include "row_aria.h"
#include "txn.h"

void Row_aria::init(row_t* row) {
    _row = row;
    read_reseration = UINT64_MAX;
    write_reseration = UINT64_MAX;
    latch = new pthread_mutex_t();
    pthread_mutex_init(latch, NULL);
    read_reserations.clear();
    write_reserations.clear();
}

RC Row_aria::access(TxnManager * txn, access_t type, row_t * local_row){
    if (type == RD || type == SCAN) {
        // 读操作
        add_reseration(read_reserations, txn->get_batch_id(), txn->return_id, txn->get_txn_id());
    } else if (type == WR) {
        // 写操作
        add_reseration(write_reserations, txn->get_batch_id(), txn->return_id, txn->get_txn_id());
    } else {
        // 其他操作
        assert(false);
    }
    return RCOK;
}

RC Row_aria::check(TxnManager * txn, access_t type, row_t * local_row) {
    // 暂时先不检查，看看情况；
    // return RCOK;

    uint64_t key = get_calvin_key(txn->get_batch_id(), txn->return_id, txn->get_txn_id());
    
    for (size_t i = 0; i < write_reserations.size(); i++) {
        if (write_reserations[i] < key) {
            return WAIT;
        } else if (write_reserations[i] >= key) {
            break;
        }
    }
    return RCOK;
}

RC Row_aria::clean(TxnManager * txn, access_t type) {
    // 清理掉对应的reservation
    uint64_t batch_id = txn->get_batch_id();
    uint64_t return_id = txn->return_id;
    uint64_t txn_id = txn->get_txn_id();
    if (type == RD || type == SCAN) {
        clean_reseration(read_reserations, batch_id, return_id, txn_id);
    } else if (type == WR) {
        clean_reseration(write_reserations, batch_id, return_id, txn_id);
    } else {
        assert(false);
    }
    return RCOK;
}