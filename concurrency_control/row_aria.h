#ifndef ROW_ARIA_H
#define ROW_ARIA_H

#include "row.h"
#include <vector>

class Row_aria {
public:
    uint64_t read_reseration;
    uint64_t write_reseration;
    // 需要维护一个连续的读写。。。。。reseration
    pthread_mutex_t * latch;
    // 这里还是存calvin_key吧
    std::vector<uint64_t> read_reserations;
    std::vector<uint64_t> write_reserations;

    void init(row_t * row);

    void reset() {
        read_reseration = UINT64_MAX;
        write_reseration = UINT64_MAX;
        read_reserations.clear();
        write_reserations.clear();
    }

    RC access(TxnManager * txn, access_t type, row_t * local_row);
    RC check(TxnManager * txn, access_t type, row_t * local_row);
    RC clean(TxnManager * txn, access_t type);

    // 写一个将事务号，存储到reserations中的函数，要按照事务号的顺序排序
    bool add_reseration(std::vector<uint64_t>& reserations, uint64_t batch_id,uint64_t return_id,uint64_t txn_id) {
        // 先加锁
        bool insert = false;
        uint64_t key = get_calvin_key(batch_id,return_id,txn_id);
        pthread_mutex_lock(latch);
        // 然后遍历reserations，找到合适的位置插入
        for (size_t i = 0; i < reserations.size(); i++) {
            if (key < reserations[i]) {
                reserations.insert(reserations.begin() + i, key);
                insert = true;
                break;
            } else if (key == reserations[i]) {
                // 已经存在，直接返回
                insert = true;
                break;
            }
        }
        pthread_mutex_unlock(latch);
        return insert;
    }

    bool clean_reseration(std::vector<uint64_t>& reserations, uint64_t batch_id,uint64_t return_id,uint64_t txn_id) {
        bool removed = false;
        uint64_t key = get_calvin_key(batch_id,return_id,txn_id);
        pthread_mutex_lock(latch);
        for (size_t i = 0; i < reserations.size(); i++) {
            if (reserations[i] == key) {
                reserations.erase(reserations.begin() + i);
                removed = true;
                break;
            }
        }
        pthread_mutex_unlock(latch);
        return removed;
    }

private:
    row_t * _row;
};

#endif
