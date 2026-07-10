/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "row_sdpcc_2.h"

void Row_sdpcc_2::init(row_t* row) {
    _row = row;
    latch = new pthread_mutex_t();
    pthread_mutex_init(latch, NULL);
    reservations.reserve(10); // 预先分配一些空间，避免频繁扩容
    reservations.clear();
    // 给reservation创造一个空开头，这样表示这个row的初始值
    reservations.push_back(sdpcc_version(0));
    reservations[0].written.store(true);

    tmp_reservations = new std::vector<uint64_t>[g_thread_cnt];
}



RC Row_sdpcc_2::add_reservation(uint64_t batch_id,uint64_t return_id,uint64_t txn_id,uint64_t thd_id) {
    // 先加锁
    bool insert = false;
    uint64_t key = get_batch_key(batch_id,return_id,txn_id);

    pthread_mutex_lock(latch);
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
            reservations.insert(reservations.begin() + i + 1, sdpcc_version(key));
            insert = true;
            break;
        }
    }
    if (!insert) {
    // key 比所有现有元素都小，插到最前面
        reservations.insert(reservations.begin(), sdpcc_version(key));
    }
    DEBUG_WRK("thd_id %ld SDPCC_2 %ld,%ld Add Reservation Row %s-%ld Version at %d\n",thd_id,batch_id,txn_id,_row->get_table_name(),_row->get_primary_key(),i + 1); // 这个reservations.size() - 1是打印下标
    pthread_mutex_unlock(latch);
    return RCOK;
}


sdpcc_version* Row_sdpcc_2::get_reservation(uint64_t batch_id, uint64_t return_id, uint64_t txn_id, access_t type, uint64_t thd_id) {
    uint64_t key = get_batch_key(batch_id, return_id, txn_id);

    // assert_reservation_append();
    pthread_mutex_lock(latch);
    // 找第一个 reservations[i].key >= key 的位置
    auto it = std::lower_bound(
        reservations.begin(),
        reservations.end(),
        key,
        [](const sdpcc_version& v, uint64_t target_key) {
            return v.key < target_key;
        }
    );

    sdpcc_version* result = nullptr;
    if (type == RD || type == SCAN) {
        // read 找前一个版本：max(version.key < key)
        if (it != reservations.begin()) {
            --it;
            result = &(*it);

            std::vector<uint64_t> k = split_batch_key(it->key);
            DEBUG_WRK("thd_id %ld SDPCC_2 %ld,%ld Read Row %s-%ld Version %ld txn %ld,%ld written %d\n",thd_id,batch_id,txn_id,_row->get_table_name(),_row->get_primary_key(),it - reservations.begin(), k[0],k[2], it->written.load()); // 这个it是打印下标
        } else {
            assert(false); // 读操作不应该找不到版本，因为最开始就有一个初始版本
        }
    } else {
        // write 找自己的版本：version.key == key
        if (it != reservations.end() && it->key == key) {
            result = &(*it);
            std::vector<uint64_t> k = split_batch_key(it->key);
            DEBUG_WRK("thd_id %ld SDPCC_2 %ld,%ld Write Row %s-%ld Version %ld txn %ld,%ld written %d\n",thd_id,batch_id,txn_id,_row->get_table_name(),_row->get_primary_key(),it - reservations.begin(), k[0], k[2], it->written.load()); // 这个it是打印下标
        } else {
            assert(WORKLOAD == TPCC); // TPCC的写操作有些找不到自己的版本，就当过了吧
            DEBUG_WRK("thd_id %ld SDPCC_2 %ld,%ld Write Row %s-%ld Version not found\n",thd_id,batch_id,txn_id,_row->get_table_name(),_row->get_primary_key());
            return nullptr;
        }
    }
    pthread_mutex_unlock(latch);
    return result;
}

RC Row_sdpcc_2::access(TxnManager * txn, access_t type, row_t * local_row, uint64_t thd_id) {
    bool found = false;
    if (type == RD || type == SCAN) {
        // 读操作，检查写reservation，看看自己读的，有没有完成，如果没有，就等待
        while (!found && !simulation->is_done()) {
            sdpcc_version* reservation = get_reservation(txn->get_batch_id(), txn->return_id, txn->get_txn_id(), type,thd_id);
            if (!reservation->written.load()) {
            //     //! 这里之后再看是死等，还是WAIT跳出去
                // return WAIT;
            } else {
                found = true;
            }
        }
    } else if (type == WR) {
        // 写操作，检查读操作和写操作，即处理写写冲突和写读冲突
        sdpcc_version* write_v = get_reservation(txn->get_batch_id(), txn->return_id, txn->get_txn_id(), WR, thd_id);
        if (write_v == nullptr) {
            // 没有找到reservation，说明这个写操作没有成功加reservation，直接返回WAIT
            return RCOK;
        }
        write_v->written.store(true);
    } else {
        // 其他操作
        assert(false);
    }
    return RCOK;
}