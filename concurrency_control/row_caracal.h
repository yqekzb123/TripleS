/* 
   版权所有 (c) 2026 山东大学
   许可协议: Apache License, Version 2.0
   你可以在遵循许可协议的前提下使用和修改本文件。
*/

#ifndef ROW_CARACAL_H
#define ROW_CARACAL_H
#if CC_ALG == CARACAL
#include "txn.h"
#include "row.h"
#include <atomic>
#include <cstring>

struct caracal_version {
    uint64_t key;
    std::atomic<bool> written;
    char data[4];
    caracal_version(uint64_t k):key(k) {
        written.store(false);
        memset(data, 0, sizeof(data));
    }
    // make copyable and movable: std::atomic is not copy-assignable/movable by default,
    // so provide explicit copy/move constructors and assignment operators that
    // transfer the logical boolean value using load()/store().
    caracal_version(const caracal_version &o) : key(o.key), written(o.written.load()) {
        memcpy(data, o.data, sizeof(data));
    }
    caracal_version& operator=(const caracal_version &o) {
        if (this != &o) {
            key = o.key;
            written.store(o.written.load());
            memcpy(data, o.data, sizeof(data));
        }
        return *this;
    }
    caracal_version(caracal_version &&o) noexcept : key(o.key), written(o.written.load()) {
        memcpy(data, o.data, sizeof(data));
    }
    caracal_version& operator=(caracal_version &&o) noexcept {
        if (this != &o) {
            key = o.key;
            written.store(o.written.load());
            memcpy(data, o.data, sizeof(data));
        }
        return *this;
    }
};

class Row_caracal {
public:
    pthread_mutex_t * latch;
    // 主版本链
    std::vector<caracal_version> reservations;
    // 临时版本链
    std::vector<uint64_t>* tmp_reservations;

    void init(row_t * row);

    RC access(TxnManager * txn, access_t type, row_t * local_row, uint64_t thd_id);
    RC clean(uint64_t thd_id);

    // init阶段
    RC add_reservation( uint64_t batch_id,uint64_t return_id,uint64_t txn_id, uint64_t thd_id);
    RC add_reservation_to_waitlist(uint64_t batch_id,uint64_t return_id,uint64_t txn_id, uint64_t thd_id);
    RC batch_append(uint64_t thd_id);

    // execution阶段
    caracal_version* get_reservation(uint64_t batch_id,uint64_t return_id,uint64_t txn_id, access_t type, uint64_t thd_id);

private:
    row_t * _row;
    void assert_reservation_append();
};
#endif // 
#endif // 
