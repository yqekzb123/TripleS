#ifndef _SMALL_LOCK_LIST_H
#define _SMALL_LOCK_LIST_H

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include <functional>
#include <iostream>
#include "global.h"
#include "txn.h"

#define PRINT_VISIT_LIST false


// 写一个带key或者水印时间的，包括事务TxnManager的结构体
struct list_node_entry
{
public:
    /* data */
    uint64_t key; // 这里的key是事务号 (txn->get_batch_id() << 32) + (txn->return_id << 24) + txn->get_txn_id() + 1;
    TxnManager * txn;
    // snapshot fields for scheduler predicate (do not dereference txn in predicate)
    std::atomic<int> snapshot_lock_ready_cnt;
    std::atomic<int> snapshot_dep_count;
    list_node_entry(uint64_t k, TxnManager * t) : key(k), txn(t), snapshot_lock_ready_cnt(0), snapshot_dep_count(0) {}
    ~list_node_entry() {}
};


template<typename T>
class LockList {
    enum NodeStatus : int { NODE_AVAILABLE = 0, NODE_TAKEN = 1, NODE_REMOVED = 2 };
    struct Node {
        T data;
        Node* next;
        std::mutex mtx; // per-node lock for hand-over-hand locking
        NodeStatus status;
        Node(const T& d) : data(d), next(nullptr), status(NODE_AVAILABLE) {}
    };

    Node* head;
    Node* tail;
    std::atomic<size_t> count{0};
    std::atomic<size_t> actual_count{0};

    // structural lock for list modifications (cleanup/clear)
    std::mutex list_mtx;
    // garbage nodes waiting for reclamation (protected by garbage_mtx)
    std::vector<Node*> garbage_nodes;
    std::mutex garbage_mtx;

public:
    LockList() {
        head = new Node(T());
        tail = head;
        count.store(0, std::memory_order_relaxed);
        actual_count.store(0, std::memory_order_relaxed);
    }

    ~LockList() {
        clear();
    }

    // Try to take a node matching cond1 and cond2. Thread-safe via hand-over-hand locking.
    bool try_take(std::function<bool(T)> cond1, std::function<bool(T)> cond2, T& out, uint64_t thd_id) {
        Node* prev = head;
        // 获取prev锁
        prev->mtx.lock();
        Node* curr = prev->next;
        #if PRINT_VISIT_LIST
            std::vector<uint64_t> visited_keys;
            std::string visit_log;
        #endif
        DEBUG_LOCKFREE("[LockList] thd %ld try_take start size=%zu-%zu minSid=%lu\n", thd_id, size(), actual_size(), minSid);
        while (curr) {
            // 获取curr锁
            curr->mtx.lock();
            list_node_entry * value_cast = static_cast<list_node_entry *>(curr->data);
            #if PRINT_VISIT_LIST
                visited_keys.push_back(value_cast->key);
                visit_log += std::to_string(value_cast->key) + "->";
            #endif
            // 如果满足条件，取出该节点
            if (cond1(curr->data) && cond2(curr->data)) {
                // take it
                out = curr->data;
                count.fetch_sub(1, std::memory_order_relaxed);
                prev->next = curr->next;
                if (curr == tail) tail = prev;
                // {
                //     std::lock_guard<std::mutex> gg(garbage_mtx);
                //     garbage_nodes.push_back(curr);
                // }
                curr->next = nullptr;

                actual_count.fetch_sub(1, std::memory_order_relaxed);
                // unlock and return
                curr->mtx.unlock();
                prev->mtx.unlock();
                #if PRINT_VISIT_LIST
                    std::string result = "[LockList] thd " + std::to_string(thd_id) + " try_take visited keys: " + visit_log + "| TAKE key=" + std::to_string(value_cast->key);
                    std::cout << result << std::endl;
                #endif
                #if DEBUG_LOCKFREE_LIST
                    extern uint64_t minSid;
                    DEBUG_LOCKFREE("[LockList] thd %ld try_take key=%lu txn=[%ld,%ld] size=%zu-%zu minSid=%lu\n", thd_id, value_cast->key, value_cast->txn->get_batch_id(), value_cast->txn->get_txn_id(), size(), actual_size(), minSid);
                #endif
                return true;
            } else {
                DEBUG_LOCKFREE("[LockList] thd %ld try_take skip key=%lu txn=[%ld,%ld] size=%zu-%zu minSid=%lu\n", thd_id, value_cast->key, value_cast->txn->get_batch_id(), value_cast->txn->get_txn_id(), size(), actual_size(), minSid);
            }
            // move forward: unlock prev, advance
            prev->mtx.unlock();
            prev = curr;
            curr = curr->next;
            // keep prev locked for next iteration
        }
        // unlock last prev if locked
        prev->mtx.unlock();
        DEBUG_LOCKFREE("[LockList] thd %ld try_take failed size=%zu-%zu minSid=%lu\n", thd_id, size(), actual_size(), minSid);
        #if PRINT_VISIT_LIST
            std::string result = "[LockList] thd " + std::to_string(thd_id) + " try_take visited keys: " + visit_log + "| NO TAKE";
            std::cout << result << std::endl;
        #endif
        return false;
    }


    // Append at tail (protected by tail node's lock and list_mtx for safety)
    void insert(const T& value, uint64_t thd_id) {
        Node* node = new Node(value);
        std::lock_guard<std::mutex> g(list_mtx);
        Node* orig_tail = nullptr;
        do {
            orig_tail = tail;
        } while (orig_tail->mtx.try_lock() == false);
        
        tail->next = node;
        tail = node;
        count.fetch_add(1, std::memory_order_relaxed);
        actual_count.fetch_add(1, std::memory_order_relaxed);
        orig_tail->mtx.unlock();
        #if DEBUG_LOCKFREE_LIST
            extern uint64_t minSid;
            list_node_entry * value_cast = static_cast<list_node_entry *>(value);
            DEBUG_LOCKFREE("[LockList] thd %ld insert_tail key=%lu txn=[%ld,%ld] size=%zu-%zu minSid=%lu\n", thd_id, value_cast->key, value_cast->txn->get_batch_id(), value_cast->txn->get_txn_id(), size(), actual_size(), minSid);
        #endif
    }

    

    // Physically remove nodes that have been logically removed. Coarse-grained with list_mtx.
    void remove_consumed() {
        std::lock_guard<std::mutex> g(list_mtx);
        Node* prev = head;
        Node* curr = prev->next;
        while (curr) {
            if (curr->status == NODE_REMOVED) {
                prev->next = curr->next;
                if (curr == tail) tail = prev;
                {
                    std::lock_guard<std::mutex> gg(garbage_mtx);
                    garbage_nodes.push_back(curr);
                }
                curr = prev->next;
                continue;
            }
            prev = curr;
            curr = curr->next;
        }
        // reclaim garbage nodes now (safe because we hold list_mtx)
        {
            std::lock_guard<std::mutex> gg(garbage_mtx);
            for (Node* n : garbage_nodes) {
                delete n;
            }
            garbage_nodes.clear();
        }
    }

    // Clear entire list
    void clear() {
        std::lock_guard<std::mutex> g(list_mtx);
        Node* node = head;
        while (node) {
            Node* next = node->next;
            delete node;
            node = next;
        }
        head = nullptr;
        tail = nullptr;
        count.store(0, std::memory_order_relaxed);
        actual_count.store(0, std::memory_order_relaxed);
    }

    size_t size() const { return count.load(std::memory_order_relaxed); }
    size_t actual_size() const { return actual_count.load(std::memory_order_relaxed); }

    void print() {
        std::lock_guard<std::mutex> g(list_mtx);
        Node* current = head->next;
        while (current) {
            list_node_entry * value_cast = static_cast<list_node_entry *>(current->data);
            std::cout << value_cast->key << " -> ";
            current = current->next;
        }
        std::cout << "nullptr" << std::endl;
    }

    void DEBUG_PRINT_LIST_LENGTH() {
        size_t sz = size();
        size_t actual_sz = actual_size();
        DEBUG_TIME("[LockList] DEBUG_PRINT_LIST_LENGTH size=%zu actual_size=%zu\n", sz, actual_sz);
    }
};

#endif // _LOCK_FREE_LIST_H