/* 
   版权所有 (c) 2026 山东大学
   许可协议: Apache License, Version 2.0
   你可以在遵循许可协议的前提下使用和修改本文件。
*/

#ifndef ROW_SDOCC_H
#define ROW_SDOCC_H
#if CC_ALG == SDOCC
#include "txn.h"
#include "row.h"

class Row_sdocc {
public:
    pthread_mutex_t * read_latch;
    pthread_mutex_t * write_latch;
    // 这里还是存calvin_key吧
    std::vector<uint64_t> read_reservations;
    std::vector<uint64_t> write_reservations;

    void init(row_t * row);

    void reset() {
        // read_reservation = UINT64_MAX;
        // write_reservation = UINT64_MAX;
        read_reservations.clear();
        write_reservations.clear();
    }

    RC access(TxnManager * txn, access_t type, row_t * local_row);
    RC check(TxnManager * txn, access_t type, row_t * local_row, Access *a);
    RC clean(TxnManager * txn, access_t type);

    // 写一个将事务号，存储到reservations中的函数，要按照事务号的顺序排序
    bool add_reservation(std::vector<uint64_t>& reservations, pthread_mutex_t * latch, uint64_t batch_id,uint64_t return_id,uint64_t txn_id);

    bool clean_reservation(std::vector<uint64_t>& reservations, pthread_mutex_t * latch, uint64_t batch_id,uint64_t return_id,uint64_t txn_id);

    // 根据calvin_key获取这个事务能读取到的最新的reservation，只获得最新的那一个
    uint64_t get_reservations(std::vector<uint64_t>& reservations, pthread_mutex_t * latch, uint64_t batch_id,uint64_t return_id,uint64_t txn_id);

private:
    row_t * _row;
};
#endif // CC_ALG == SDOCC
#endif // ROW_SDOCC_H
