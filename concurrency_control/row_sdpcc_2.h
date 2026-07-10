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
#include "txn.h"
#include "row.h"
#include <atomic>
#include <cstring>

#ifndef ROW_SDPCC_H_2
#define ROW_SDPCC_H_2
struct sdpcc_version {
    uint64_t key;
    std::atomic<bool> written;
    char data[4];
    sdpcc_version(uint64_t k):key(k) {
        written.store(false);
        memset(data, 0, sizeof(data));
    }
    // make copyable and movable: std::atomic is not copy-assignable/movable by default,
    // so provide explicit copy/move constructors and assignment operators that
    // transfer the logical boolean value using load()/store().
    sdpcc_version(const sdpcc_version &o) : key(o.key), written(o.written.load()) {
        memcpy(data, o.data, sizeof(data));
    }
    sdpcc_version& operator=(const sdpcc_version &o) {
        if (this != &o) {
            key = o.key;
            written.store(o.written.load());
            memcpy(data, o.data, sizeof(data));
        }
        return *this;
    }
    sdpcc_version(sdpcc_version &&o) noexcept : key(o.key), written(o.written.load()) {
        memcpy(data, o.data, sizeof(data));
    }
    sdpcc_version& operator=(sdpcc_version &&o) noexcept {
        if (this != &o) {
            key = o.key;
            written.store(o.written.load());
            memcpy(data, o.data, sizeof(data));
        }
        return *this;
    }
};

class Row_sdpcc_2 {
public:
    pthread_mutex_t * latch;
    // 主版本链
    std::vector<sdpcc_version> reservations;
    // 临时版本链
    std::vector<uint64_t>* tmp_reservations;

    void init(row_t * row);

    RC access(TxnManager * txn, access_t type, row_t * local_row, uint64_t thd_id);
    // RC clean(TxnManager* txn, access_t type) ;

    // init阶段
    RC add_reservation( uint64_t batch_id,uint64_t return_id,uint64_t txn_id, uint64_t thd_id);

    // execution阶段
    sdpcc_version* get_reservation(uint64_t batch_id,uint64_t return_id,uint64_t txn_id, access_t type, uint64_t thd_id);

    uint64_t get_version_cnt() {
        return reservations.size();
    }
private:
    row_t * _row;
    void assert_reservation_append();
};

#endif
