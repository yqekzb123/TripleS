"""Compile the actual row implementation with minimal workload/txn adapters.
No replacement implementation of the chains, waiter logic, or GC is used.
"""
from pathlib import Path
import re, shutil, subprocess, sys

repo = Path(sys.argv[1])
out = Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
for name in ['row_sdmvcc.h', 'sdmvcc_index.h']:
    shutil.copyfile(repo/'concurrency_control'/name, out/name)
source = (repo/'concurrency_control/row_sdmvcc.cpp').read_text()
source = re.sub(r'^#include "[^\n]+"\n', '', source, flags=re.M)
(out/'row_impl.inc').write_text(source)
(out/'global.h').write_text(r'''
#pragma once
#include <pthread.h>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <atomic>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <random>
#include <thread>
#define SDMVCC 9
#define CC_ALG SDMVCC
#ifndef SDMVCC_LAZY_READ_INTENT
#define SDMVCC_LAZY_READ_INTENT 0
#endif
#define SDMVCC_INTENT_GC 1
#define SDMVCC_LONG_READ_GUARD 0
#define SDMVCC_LONG_READ_GUARD_BITS 262144
#define SDMVCC_LONG_READ_GUARD_HASHES 4
#define SDMVCC_UNSAFE_L1_NO_INTENT 0
#define SDMVCC_BLIND_WRITE 0
#define SDMVCC_EARLY_VERSION_PUBLISH 0
#define ATOM_CAS(v,a,b) __sync_bool_compare_and_swap(&(v),(a),(b))
enum RC {RCOK, WAIT, Abort};
enum access_t {RD, WR, SCAN};
uint64_t minSid = 0;
uint64_t get_sys_clock() { return 0; }
struct Catalog {
    uint32_t get_field_size(uint32_t) { return 8; }
    uint32_t get_field_index(uint32_t) { return 0; }
};
struct row_t {
    uint64_t value = 0;
    Catalog schema;
    char *get_data() { return reinterpret_cast<char *>(&value); }
    uint32_t get_tuple_size() { return sizeof(value); }
    Catalog *get_schema() { return &schema; }
};
struct Alloc {
    void *alloc(size_t n) { return malloc(n); }
    void free(void *p, size_t) { ::free(p); }
} mem_allocator;
struct SDMVCCEntry;
struct TxnManager {
    uint64_t sid;
    int lr = 1;
    bool lock_ready = false, released = false, registered = false;
    bool lazy_armed = false;
    SDMVCCEntry *intent = nullptr, *version = nullptr;
    explicit TxnManager(uint64_t s) : sid(s) {}
    uint64_t sdmvcc_snapshot() { return sid; }
    int register_sdmvcc_access(row_t *, access_t) {
        if (registered) return 0;
        registered = true; return 1;
    }
    bool is_sdmvcc_blind_write(row_t *, access_t) { return false; }
    bool uses_sdmvcc_long_read_guard() { return false; }
    bool uses_sdmvcc_unsafe_l1_no_intent() { return false; }
    void sdmvcc_set_intent_node(row_t *, SDMVCCEntry *e) { intent = e; }
    void sdmvcc_set_version_node(row_t *, SDMVCCEntry *e) { version = e; }
    SDMVCCEntry *sdmvcc_intent_node(row_t *) { return released ? nullptr : intent; }
    void incr_lr() { __sync_add_and_fetch(&lr, 1); }
    int decr_lr() { return __sync_sub_and_fetch(&lr, 1); }
    uint64_t get_txn_id() { return sid; }
    uint64_t get_batch_id() { return 0; }
    bool arm_sdmvcc_lazy_access(row_t *, bool &fresh) {
        if (lazy_armed) return false;
        lazy_armed = true; fresh = true; return true;
    }
};
struct TxnTable {
    std::atomic<unsigned> notifications{0};
    void restart_txn(uint64_t, uint64_t, uint64_t) { ++notifications; }
} txn_table;
''')
(out/'test.cpp').write_text(r'''
#include "global.h"
#define private public
#include "row_sdmvcc.h"
#undef private
#include "row_impl.inc"

void admit(Row_sdmvcc &row, TxnManager &t) {
    row.arm_read(&t, t.sid);
    if (t.decr_lr() == 0) ATOM_CAS(t.lock_ready, false, true);
}
void release(Row_sdmvcc &row, TxnManager &t) {
    auto p = t.intent; t.intent = nullptr; t.released = true;
    row.release_intent(p, minSid);
}
void poll() { for (unsigned i=0; i<512; ++i) Row_sdmvcc::poll_gc(minSid, i); }
void check(Row_sdmvcc &row) {
    row.validate_locked("test");
    unsigned count=0;
    for (auto v=row._sentinel.nextWrite; v!=&row._sentinel; v=v->nextWrite) {
        assert(row.find_version_locked(v->sid)==v);
        assert(v->nextWrite->prevWrite==v);
        ++count;
    }
    assert(count==row._version_cnt);
}
void destroy(Row_sdmvcc &row) {
    // Drained rows have no deferred candidates; test fixtures own row lifetime.
    assert(!row._deferred.node.linked);
    for (auto v=row._sentinel.nextWrite; v!=&row._sentinel;) {
        auto next=v->nextWrite; free_version_data(v); free_entry(v); v=next;
    }
}

void randomized() {
    for (unsigned seed=0; seed<100; ++seed) {
        minSid=0;
        row_t data; Row_sdmvcc row; row.init(&data);
        const unsigned n=200;
        std::vector<TxnManager *> txns;
        std::vector<unsigned> order;
        std::mt19937 rng(seed);
        for(unsigned i=0;i<n;++i) { txns.push_back(new TxnManager(i+1)); order.push_back(i); }
        std::shuffle(order.begin(), order.end(), rng);
        for(auto i:order) row.register_access(i%3 ? WR : RD,txns[i]);
        check(row);
        minSid=n+2;
        for(auto t:txns) admit(row,*t);
        uint64_t expected=0;
        for(unsigned i=0;i<n;++i) {
            auto &t=*txns[i]; assert(t.lr==0 && t.lock_ready);
            row_t local; assert(row.read(&t,t.sid,&local)==RCOK);
            assert(local.value==expected);
            release(row,t);
            if(i%3) {
                if(i%7==0) row.abort_write(t.sid,0,t.version);
                else {
                    local.value=t.sid;
                    row.stage_write(t.sid,&local,t.version);
                    row.publish_write(t.sid,0,false,t.version);
                    expected=t.sid;
                }
            }
            check(row);
        }
        poll(); assert(row._version_cnt==1);
        for(auto t:txns) delete t;
        destroy(row);
    }
    puts("PASS randomized out-of-order registration/RMW/abort/read values");
}

void deferred_and_pin() {
    minSid=0; row_t data; Row_sdmvcc row; row.init(&data);
    TxnManager a(10); row.register_access(WR,&a); admit(row,a);
    release(row,a);
    row_t local; local.value=10;
    row.stage_write(10,&local,a.version); row.publish_write(10,0,false,a.version);
    assert(row._version_cnt==2 && row._deferred.node.linked);
    Row_sdmvcc::pin_snapshot(10); minSid=100;
    poll(); assert(row._version_cnt==2);
    Row_sdmvcc::unpin_snapshot(10);
    poll(); assert(row._version_cnt==1 && !row._deferred.node.linked);
    destroy(row);
    puts("PASS watermark-only deferred progress and snapshot pin");
}

void abort_transfer() {
    minSid=0; row_t data; Row_sdmvcc row; row.init(&data);
    TxnManager w1(1), w3(3), r5(5), r4(4);
    row.register_access(RD,&r5); row.register_access(WR,&w1);
    row.register_access(RD,&r4); row.register_access(WR,&w3);
    minSid=10;
    admit(row,w1); admit(row,w3); admit(row,r4); admit(row,r5);
    assert(r4.intent->myVersion==w3.version && r5.lr==1);
    // A pending predecessor aborts; only its waiters transfer, lr unchanged.
    row.abort_write(3,0,w3.version); assert(r5.lr==1);
    assert(r5.intent->myVersion==w1.version);
    release(row,w1);
    row_t local; local.value=1;
    row.stage_write(1,&local,w1.version); row.publish_write(1,0,false,w1.version);
    assert(w3.lr==0 && r4.lr==0 && r5.lr==0);
    release(row,w3); release(row,r4); release(row,r5);
    poll(); check(row); destroy(row);
    puts("PASS abort moves only actual waiters without double notification");
}

void local_work() {
    minSid=0; row_t data; Row_sdmvcc row; row.init(&data);
    std::vector<TxnManager *> writers, readers;
    for(unsigned i=0;i<1000;++i) {
        auto w=new TxnManager(2*i+1), r=new TxnManager(2*i+2);
        writers.push_back(w); readers.push_back(r);
        row.register_access(WR,w); row.register_access(RD,r);
    }
    minSid=3000;
    for(auto w:writers) admit(row,*w);
    for(auto r:readers) admit(row,*r);
    for(auto w:writers) {
        assert(w->lr==0); release(row,*w);
        row_t local; local.value=w->sid;
        row.stage_write(w->sid,&local,w->version);
        row.publish_write(w->sid,0,false,w->version);
    }
    auto before=g_gc_local_checks.load();
    release(row,*readers[500]);
    assert(g_gc_local_checks.load()-before==1);
    for(unsigned i=0;i<1000;++i) if(i!=500) release(row,*readers[i]);
    poll(); check(row); assert(row._version_cnt==1);
    for(auto w:writers) delete w;
    for(auto r:readers) delete r;
    destroy(row);
    puts("PASS releasing one intent checks one pair with 1000 live versions");
}

void concurrent_arm_publish() {
    for(unsigned round=0;round<200;++round) {
        minSid=100; row_t data; Row_sdmvcc row; row.init(&data);
        TxnManager writer(1), reader(2);
        row.register_access(WR,&writer); row.register_access(RD,&reader);
        admit(row,writer); release(row,writer);
        row_t local; local.value=7;
        row.stage_write(1,&local,writer.version);
        std::thread a([&]{admit(row,reader);});
        std::thread b([&]{row.publish_write(1,0,false,writer.version);});
        std::thread c([&]{poll();});
        a.join();b.join();c.join();
        assert(reader.lr==0 && reader.lock_ready);
        assert(row.read(&reader,2,&local)==RCOK && local.value==7);
        release(row,reader); poll(); destroy(row);
    }
    puts("PASS concurrent Arm/publish/deferred polling");
}

int main() {
    randomized(); deferred_and_pin(); abort_transfer(); local_work();
    concurrent_arm_publish();
    assert(g_active_read_intents.load()==0);
    puts("ALL METADATA TESTS PASSED");
}
''')
subprocess.run(['g++','-std=c++11','-g','-O1','-fsanitize=address,undefined',
                '-fno-omit-frame-pointer','-pthread',str(out/'test.cpp'),
                '-o',str(out/'test')],check=True)
subprocess.run([str(out/'test')],check=True)
