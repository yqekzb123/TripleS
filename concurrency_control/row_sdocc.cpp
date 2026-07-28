#include "row_sdocc.h"
#include "txn.h"
#include "water_mark.h"
#if CC_ALG == SDOCC
void Row_sdocc::init(row_t* row) {
    _row = row;
    // read_reservation = UINT64_MAX;
    // write_reservation = UINT64_MAX;
    // read_latch = new pthread_mutex_t();
    write_latch = new pthread_mutex_t();
    // pthread_mutex_init(read_latch, NULL);
    pthread_mutex_init(write_latch, NULL);
    // read_reservations.clear();
    write_reservations.clear();
    write_reservations.insert(write_reservations.begin(), new sdocc_version(0, true, false, 0));
}

bool Row_sdocc::add_reservation(std::vector<sdocc_version*>& reservations, pthread_mutex_t * latch, TxnManager* txn,bool is_blind) {
    // 先加锁
    bool insert = false;
    uint64_t key = get_batch_key(txn->get_batch_id(),txn->return_id,txn->get_txn_id());
    pthread_mutex_lock(latch);
    // 然后遍历reservations，找到合适的位置插入；
    // 如果key是最大的，就插在最后面；如果key在中间，就插在中间；如果key已经存在，就不插入了，直接返回。
    for (size_t i = reservations.size(); i > 0; i--) {
        if (key > reservations[i-1]->id) {
            reservations.insert(reservations.begin() + i, new sdocc_version(key, false, is_blind, 0, txn));
            insert = true;
            break;
        } else if (key == reservations[i-1]->id) {
            // 已经存在，直接返回
            insert = true;
            break;
        } 
    }
    if (!insert) {
        reservations.push_back(new sdocc_version(key, false, is_blind, 0, txn));
        insert = true;
    }
    pthread_mutex_unlock(latch);
    return insert;
}

sdocc_version* Row_sdocc::get_reservations(std::vector<sdocc_version*>& reservations, pthread_mutex_t * latch, uint64_t batch_id,uint64_t return_id,uint64_t txn_id) {
    uint64_t key = get_batch_key(batch_id,return_id,txn_id);
    sdocc_version* result = nullptr;
    pthread_mutex_lock(latch);
    // 倒序查询
    // 快一点查询到位置，拿到
    // auto it = std::lower_bound(reservations.begin(), reservations.end(), key, [](const sdocc_version& a, uint64_t b) {
    //     return a.id < b;
    // });
    // if (it != reservations.end() && it->id == key) {
    //     result = *it;
    // } else {
    //     // 如果没有找到，就找小于key的最大值
    //     if (it != reservations.begin()) {
    //         --it;
    //         result = *it;
    //     }
    // }
    for (size_t i = reservations.size(); i > 0; i--) {
        if (reservations[i-1]->id <= key) {
            // assert(result.id <= reservations[i-1].id);
            result = reservations[i-1];
            break;
        }
    }
    pthread_mutex_unlock(latch);
    return result;
}

bool Row_sdocc::clean_reservation(std::vector<sdocc_version*>& reservations, pthread_mutex_t * latch, uint64_t batch_id,uint64_t return_id,uint64_t txn_id) {
    bool removed = false;
    uint64_t key = get_batch_key(batch_id,return_id,txn_id);
    pthread_mutex_lock(latch);
    // 倒序查询，找到key并删除
    for (size_t i = reservations.size(); i > 0; i--) {
        if (reservations[i-1]->id == key) {
            // reservations.erase(reservations.begin() + i - 1);
            reservations[i-1]->available = true;
            removed = true;
            break;
        }
    }

    pthread_mutex_unlock(latch);
    DEBUG_WRK("[SDOCC] txn %ld,%ld clean key %ld, reservations size %ld\n", batch_id, txn_id, key, reservations.size());
    return removed;
}

RC Row_sdocc::access(TxnManager * txn, access_t type, row_t * local_row){
    // return RCOK;
    // 和师兄讨论，SDOCC，access里只检查是否满足条件，真正的reservation放到check里
    if (type == RD || type == SCAN) {
        // 读操作，检查写操作，即处理读写冲突
        sdocc_version* reservation = get_reservations(write_reservations, write_latch, txn->get_batch_id(), txn->return_id, txn->get_txn_id());
        txn->last_sdocc_write_reservation = reservation;
    } else if (type == WR) {
        // 写操作，检查读操作和写操作，即处理写写冲突和写读冲突
        sdocc_version* reservation = get_reservations(write_reservations, write_latch, txn->get_batch_id(), txn->return_id, txn->get_txn_id() - 1);
        txn->last_sdocc_write_reservation = reservation;
        // reservation = get_reservations(read_reservations, read_latch, txn->get_batch_id(), txn->return_id, txn->get_txn_id());
        // txn->last_sdocc_read_reservation = reservation;
    } else {
        // 其他操作
        assert(false);
    }
    return RCOK;
}

RC Row_sdocc::wait_commit_dependency(TxnManager * txn) {
    // 只要进来，就是一定是RETRY状态；
    RC rc = RETRY;

    // 只有过了水印，才能等待提交水印
    uint64_t key = get_batch_key(txn->get_batch_id(), txn->return_id, txn->get_txn_id());
    bool watermark_passed = key <= (check_water_mark->get_global_watermark() + 1);
    
    // 先测试本地进行等待，远程的不管
    bool is_local = IS_LOCAL(txn->get_txn_id());

    // 考虑两个事务 Ti < Tj，当前事务是Tj，前驱是Ti
    if (watermark_passed) {
    // if (watermark_passed && is_local) {
        TxnManager * predecessor = txn->last_sdocc_write_reservation->txn;
        // 先给自己加Ti
        pthread_mutex_lock(&txn->predecessor_lock);
        txn->predecessor_transaction.insert(predecessor);
        pthread_mutex_unlock(&txn->predecessor_lock);

        // 然后给Ti加自己
        pthread_mutex_lock(&predecessor->successor_lock);
        predecessor->successor_transaction.insert(txn);
        pthread_mutex_unlock(&predecessor->successor_lock);
        // txn->local_wait_commit_cnt.fetch_add(1);
        // txn->wait_commit_cnt.fetch_add(1);

        // 再次验证，当前操作是不是已经提交了，如果已经提交，这一步操作按照RCOK来
        if (txn->last_sdocc_write_reservation->available) {
            pthread_mutex_lock(&txn->predecessor_lock);
            auto iter = txn->predecessor_transaction.find(predecessor);
            if (iter ==  txn->predecessor_transaction.end()) {
                // 已经被Ti处理，所以等待重试即可
            } else {
                txn->predecessor_transaction.erase(predecessor);
                rc = RCOK;
            }
            pthread_mutex_unlock(&txn->predecessor_lock);
        } else {
            DEBUG_WRK("[SDOCC] txn %ld,%ld read key %ld, but write reservation %ld,%d has not commit, waiting transaction %ld,%ld, now wait_commit_cnt %ld\n", txn->get_batch_id(), txn->get_txn_id(), key, txn->last_sdocc_write_reservation->id, txn->last_sdocc_write_reservation->available, predecessor->get_batch_id(),predecessor->get_txn_id(),txn->predecessor_transaction.size());
        }
    }
    return rc;
}

RC Row_sdocc::check(TxnManager * txn, access_t type, row_t * local_row, Access *a) {
    // return RCOK;
    uint64_t key = get_batch_key(txn->get_batch_id(), txn->return_id, txn->get_txn_id());
    RC rc = RCOK;
    // 这里要干的事情是，先把自己的key作为reservation放到reservations里;
    if (type == RD || type == SCAN) {
        // 读操作，检查写操作，即处理读写冲突
        // add_reservation(read_reservations, read_latch, txn->get_batch_id(), txn->return_id, txn->get_txn_id());
    } else if (type == WR) {
        // 写操作，检查读操作和写操作，即处理写写冲突和写读冲突
        add_reservation(write_reservations, write_latch, txn, txn->is_blind);
    } else {
        // 其他操作
        assert(false);
    }
    // 然后检查上次读取到的reservation和上次写入到reservation里reservation的值，是否与上次相同？如果是就检验成功，如果不是就说明在这期间有其他事务提交了，检验失败，返回WAIT。

    // 重新检查
    access(txn, type, local_row);
    
    if (type == RD || type == SCAN) {
        // 读操作，检查写操作，即处理读写冲突
        // #if WORKLOAD == YCSB
        // if (txn->last_sdocc_write_reservation.id > a->sdocc_write_reservation.id) {
        // #else
        if (txn->last_sdocc_write_reservation->id > a->sdocc_write_reservation->id || 
            (txn->last_sdocc_write_reservation->available != true && 
             txn->last_sdocc_write_reservation->is_blind != true)) {
        // #endif
            DEBUG_WRK("[SDOCC] txn %ld,%ld read key %ld, but write reservation changed from %ld,%d to %ld,%d, return RETRY\n", txn->get_batch_id(), txn->get_txn_id(), key, a->sdocc_write_reservation->id, a->sdocc_write_reservation->available, txn->last_sdocc_write_reservation->id, txn->last_sdocc_write_reservation->available);
            rc = RETRY;
            a->sdocc_write_reservation = txn->last_sdocc_write_reservation;
            if (txn->last_sdocc_write_reservation->available != true && 
            txn->last_sdocc_write_reservation->is_blind != true) {
                // 如果是在等待未提交事务
                // rc = wait_commit_dependency(txn);
            }
        } else {
            DEBUG_WRK("[SDOCC] txn %ld,%ld read key %ld, write reservation from %ld,%d to %ld,%d, return RCOK\n", txn->get_batch_id(), txn->get_txn_id(), key, a->sdocc_write_reservation->id, a->sdocc_write_reservation->available, txn->last_sdocc_write_reservation->id, txn->last_sdocc_write_reservation->available);
        }
    } else if (type == WR) {
        // 写操作，检查读操作和写操作，即处理写写冲突和写读冲突
        if ((txn->last_sdocc_write_reservation->id > a->sdocc_write_reservation->id && 
                txn->last_sdocc_write_reservation->id != key)) {
            DEBUG_WRK("[SDOCC] txn %ld,%ld write key %ld, but reservation changed, write reservation from %ld,%d to %ld,%d, return RETRY\n", txn->get_batch_id(), txn->get_txn_id(), key, a->sdocc_write_reservation->id, a->sdocc_write_reservation->available, txn->last_sdocc_write_reservation->id, txn->last_sdocc_write_reservation->available);
            rc = RETRY;
            a->sdocc_write_reservation = txn->last_sdocc_write_reservation;
            // a->sdocc_read_reservation = txn->last_sdocc_read_reservation;
        } else {
            DEBUG_WRK("[SDOCC] txn %ld,%ld write key %ld, write reservation from %ld,%d to %ld,%d, return RCOK\n", txn->get_batch_id(), txn->get_txn_id(), key, a->sdocc_write_reservation->id, a->sdocc_write_reservation->available, txn->last_sdocc_write_reservation->id, txn->last_sdocc_write_reservation->available);
        }
    } else {
        // 其他操作
        assert(false);
    }
    return rc;
}

RC Row_sdocc::clean(TxnManager * txn, access_t type) {
    // return RCOK;
    // 清理掉对应的reservation
    uint64_t batch_id = txn->get_batch_id();
    uint64_t return_id = txn->return_id;
    uint64_t txn_id = txn->get_txn_id();
    if (type == RD || type == SCAN) {
        // clean_reservation(read_reservations, read_latch, batch_id, return_id, txn_id);
    } else if (type == WR) {
        clean_reservation(write_reservations, write_latch, batch_id, return_id, txn_id);
    } else {
        assert(false);
    }
    return RCOK;
}

#endif 