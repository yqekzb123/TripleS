#include "row_caracal.h"
#include "caracal.h"
#include "txn.h"
#include "global.h"
#if CC_ALG == CARACAL
void Row_caracal::init(row_t* row) {
    _row = row;
    latch = new pthread_mutex_t();
    pthread_mutex_init(latch, NULL);
    reservations.reserve(10); // 预先分配一些空间，避免频繁扩容
    reservations.clear();
    // 给reservation创造一个空开头，这样表示这个row的初始值
    reservations.push_back(caracal_version(0));
    reservations[0].written.store(true);

    tmp_reservations = new std::vector<uint64_t>[g_thread_cnt];
}

RC Row_caracal::add_reservation(uint64_t batch_id,uint64_t return_id,uint64_t txn_id,uint64_t thd_id) {
    // 先加锁
    bool insert = false;
    uint64_t key = get_batch_key(batch_id,return_id,txn_id);
    if (pthread_mutex_trylock(latch) != 0) {
        return WAIT;
    }
    // 然后遍历reservations，找到合适的位置插入；
    // 从后往前插入
    int i = reservations.size() - 1;
    for (i = reservations.size() - 1; i >= 0; i--) {
        if (key == reservations[i].key) {
            // 已经存在，直接返回 / 不重复插入
            insert = true;
            break;
        }
        if (key > reservations[i].key) {
            // 插到第一个比 key 小的元素后面
            reservations.insert(reservations.begin() + i + 1, caracal_version(key));
            insert = true;
            break;
        }
    }
    if (!insert) {
    // key 比所有现有元素都小，插到最前面
        reservations.insert(reservations.begin(), caracal_version(key));
    }
    DEBUG_WRK("thd_id %ld Caracal %ld,%ld Add Reservation Row %ld Version at %d\n",thd_id,batch_id,txn_id,_row->get_primary_key(),i + 1); // 这个reservations.size() - 1是打印下标
    pthread_mutex_unlock(latch);
    return RCOK;
}

RC Row_caracal::add_reservation_to_waitlist(uint64_t batch_id,uint64_t return_id,uint64_t txn_id, uint64_t thd_id) {
    bool insert = false;
    uint64_t key = get_batch_key(batch_id,return_id,txn_id);
    tmp_reservations[thd_id].push_back(key);
    DEBUG_WRK("thd_id %ld Caracal %ld,%ld Add Reservation to waitlist Row %ld Version\n",thd_id,batch_id,txn_id,_row->get_primary_key());
    return RCOK;
}

RC Row_caracal::batch_append(uint64_t thd_id) {
    // 把这个线程的临时reservation追加到正式的reservation里
    pthread_mutex_lock(latch);
    for (uint64_t key : tmp_reservations[thd_id]) {
        bool insert = false;
        std::vector<uint64_t> k = split_batch_key(key);
        int i;
        for (i = reservations.size() - 1; i >= 0; i--) {
            if (key == reservations[i].key) {
                // 已经存在，直接返回 / 不重复插入
                insert = true;
                break;
            }
            if (key > reservations[i].key) {
                // 插到第一个比 key 小的元素后面
                reservations.insert(reservations.begin() + i + 1, caracal_version(key));
                insert = true;
                break;
            }
        }
        if (!insert) {
        // key 比所有现有元素都小，插到最前面
            reservations.insert(reservations.begin(), caracal_version(key));
        }
        DEBUG_WRK("thd_id %ld txn %ld,%ld Add Reservation Row %ld Version at %d\n",thd_id,k[0],k[2],_row->get_primary_key(),i + 1); // 这个reservations.size() - 1是打印下标
    }
    tmp_reservations[thd_id].clear();
    pthread_mutex_unlock(latch);
    return RCOK;
}

void Row_caracal::assert_reservation_append() {
    for (int i = 0; i < g_thread_cnt; i++) {
        assert(tmp_reservations[i].empty());
    }
}

caracal_version* Row_caracal::get_reservation(uint64_t batch_id, uint64_t return_id, uint64_t txn_id, access_t type, uint64_t thd_id) {
    uint64_t key = get_batch_key(batch_id, return_id, txn_id);

    assert_reservation_append();

    // 找第一个 reservations[i].key >= key 的位置
    auto it = std::lower_bound(
        reservations.begin(),
        reservations.end(),
        key,
        [](const caracal_version& v, uint64_t target_key) {
            return v.key < target_key;
        }
    );

    caracal_version* result = nullptr;
    if (type == RD || type == SCAN) {
        // read 找前一个版本：max(version.key < key)
        if (it != reservations.begin()) {
            --it;
            result = &(*it);

            std::vector<uint64_t> k = split_batch_key(it->key);
            DEBUG_WRK("thd_id %ld Caracal %ld,%ld Read Row %ld Version %ld txn %ld,%ld written %d\n",thd_id,batch_id,txn_id,_row->get_primary_key(),it - reservations.begin(), k[0],k[2], it->written.load()); // 这个it是打印下标
        } else {
            assert(false); // 读操作不应该找不到版本，因为最开始就有一个初始版本
        }
    } else {
        // write 找自己的版本：version.key == key
        if (it != reservations.end() && it->key == key) {
            result = &(*it);
            std::vector<uint64_t> k = split_batch_key(it->key);
            DEBUG_WRK("thd_id %ld Caracal %ld,%ld Write Row %ld Version %ld txn %ld,%ld written %d\n",thd_id,batch_id,txn_id,_row->get_primary_key(),it - reservations.begin(), k[0], k[2], it->written.load()); // 这个it是打印下标
        } else {
            assert(false); // 写操作应该总能找到自己的版本，因为add_reservation阶段就插入了
        }
    }
    // pthread_mutex_unlock(latch);
    return result;
}

RC Row_caracal::access(TxnManager * txn, access_t type, row_t * local_row, uint64_t thd_id) {
    if (type == RD || type == SCAN) {
        // 读操作，检查写reservation，看看自己读的，有没有完成，如果没有，就等待
        caracal_version* reservation = get_reservation(txn->get_batch_id(), txn->return_id, txn->get_txn_id(), type,thd_id);
        if (!reservation->written.load()) {
            // 没有找到reservation，或者reservation还没有完成，等待
            //! 这里之后再看是死等，还是WAIT跳出去
            return WAIT;
        }
    } else if (type == WR) {
        // 写操作，检查读操作和写操作，即处理写写冲突和写读冲突
        caracal_version* write_v = get_reservation(txn->get_batch_id(), txn->return_id, txn->get_txn_id(), WR, thd_id);
        write_v->written.store(true);
    } else {
        // 其他操作
        assert(false);
    }
    return RCOK;
}

RC Row_caracal::clean(uint64_t thd_id) {
    // return RCOK;
    // 清理掉对应的reservation
    pthread_mutex_lock(latch);
    DEBUG_WRK("thd_id %ld Caracal Clean Row %ld, current reservations size %lu\n",thd_id,_row->get_primary_key(), reservations.size());
    // 清理掉除了开头的那一个初始版本以外的所有版本
    reservations.erase(reservations.begin() + 1, reservations.end());
    for (int i = 0; i < g_thread_cnt; i++) {
        tmp_reservations[i].clear();
    }
    pthread_mutex_unlock(latch);
    return RCOK;
}

#endif 