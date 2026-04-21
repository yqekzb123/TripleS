#ifndef _CIRCLE_LIST_H
#define _CIRCLE_LIST_H

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include <functional>
#include <iostream>
#include "global.h"
#include "txn.h"
#include "txn.h"
// 写一个放大因子
#define CIRCLE_LIST_CAP_FACTOR 10
// #define MAX_TRACE_CNT 200
#define MAX_TRACE_CNT UINT64_MAX

class circle_node_entry {
public:
    /* data */
    uint64_t key; // 这里的key是事务号 (txn->get_batch_id() << 32) + (txn->return_id << 24) + txn->get_txn_id() + 1;
    TxnManager * txn; // 可选的消息指针
    bool is_valid; // 标记这个entry是否有效，false表示被take走了或者被mark_consumed了

    // 加一个锁，保护这个entry的修改，主要是为了在try_take的时候，避免多个线程同时take同一个entry
    std::mutex mtx;

    std::atomic<bool> ready{true}; // 用来作为锁

    circle_node_entry() : circle_node_entry(0, nullptr, false) {}
    circle_node_entry(uint64_t k, TxnManager * m, bool v) : key(k), txn(m), is_valid(v) {
    }
    circle_node_entry(const circle_node_entry &entry) : circle_node_entry(entry.key, entry.txn, entry.is_valid) {}
    ~circle_node_entry() {}
};

class CircleList {
public:
    CircleList(uint64_t load_factor, uint64_t node_cnt) {
        capacity = load_factor * node_cnt * CIRCLE_LIST_CAP_FACTOR;
        list = new circle_node_entry[capacity];
        count.store(0, std::memory_order_relaxed);
    }

    ~CircleList() {
        delete[] list;
    }

    // 对于这个循环数组来说，插入应该是根据key，插入到对应位置的，key % capacity，这样就不需要移动元素了
    bool insert(uint64_t thd_id, uint64_t key, TxnManager * txn) {
        uint64_t index = key % capacity;
        list[index].mtx.lock();
        if (list[index].is_valid) {
            // Collision occurred, list is full
            assert(false && "CircleList collision: list is full or load factor is too high");
            return false;
        }
        list[index].key = key; 
        list[index].txn = txn; 
        list[index].is_valid = true;
        list[index].mtx.unlock();
        list[index].ready.store(true, std::memory_order_relaxed);
        count.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // 获取的时候，则是从head开始往后找，函数体里面放两个条件函数，第一个是可取出的条件，第二个是判断是否可以继续遍历的条件
    bool try_take(std::function<bool(circle_node_entry&)> cond1, TxnManager*& out, uint64_t thd_id) {
        uint64_t current_head = head.load(std::memory_order_relaxed);
        uint64_t tail = minSid % capacity;
        uint64_t trace_cnt = 0;
        while (current_head != tail && trace_cnt < MAX_TRACE_CNT) {
            circle_node_entry& entry = list[current_head];
            trace_cnt++;
            if (!entry.is_valid) {
                //  || !cond1(entry)) {
                current_head = (current_head + 1) % capacity;
                continue;
            }
            entry.mtx.lock();
            if (entry.is_valid && cond1(entry)) {
                // Mark as taken
                entry.is_valid = false;
                out = entry.txn; // copy out the entry
                entry.mtx.unlock();
                INC_STATS(thd_id,small_lock_trace_cnt,trace_cnt);
                count.fetch_sub(1, std::memory_order_relaxed);
                // head.store((current_head + 1) % capacity, std::memory_order_release);
                return true;
            }
            entry.mtx.unlock();
            current_head = (current_head + 1) % capacity;
        }
        INC_STATS(thd_id,small_lock_trace_cnt,trace_cnt);
        return false; // not found
    }

    // 需要设置head，就是从head开始，把head改成之后第一个valid的位置。
    void mark_head() {
        uint64_t current_head = head.load(std::memory_order_relaxed);
        uint64_t tail = minSid % capacity;
        while (current_head != tail) {
            circle_node_entry& entry = list[current_head];
            if (entry.is_valid)  {
                // head怎么赋值？
                head.store(current_head);
                break;
            }
            current_head = (current_head + 1) % capacity;
        }
    }

    void DEBUG_PRINT_LIST_LENGTH() {
        size_t sz = count.load(std::memory_order_relaxed); 
        // size_t actual_sz = actual_size();
        DEBUG_TIME("[CircleList] DEBUG_PRINT_LIST_LENGTH size=%zu\n", sz);
    }

private:
    circle_node_entry * list; // 用一个定长的数组实现
    // 固定长度就是 load * node_cnt，理论上不应该超过这个长度
    uint64_t capacity;
    std::atomic<uint64_t> head{0}; // 指向第一个有效元素
    // std::atomic<uint64_t> tail{0}; // 指向下一个插入位置

    std::atomic<size_t> count{0};
};

#endif // _CIRCLE_LIST_H