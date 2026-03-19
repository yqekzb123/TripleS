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

enum class ENTRY_TYPE { TYPE_TXN, TYPE_MSG };
// 写一个带key或者水印时间的，包括事务TxnManager的结构体
struct list_node_entry
{
public:
    /* data */
    uint64_t key; // 这里的key是事务号 (txn->get_batch_id() << 32) + (txn->return_id << 24) + txn->get_txn_id() + 1;

    // 下面两个指针二选一使用
    
    ENTRY_TYPE entry_type;
    TxnManager * txn; // 可选的事务指针
    Message * msg; // 可选的消息指针

    list_node_entry() : key(0), txn(nullptr), msg(nullptr) {}
    list_node_entry(uint64_t k, TxnManager * t) : key(k), txn(t), msg(nullptr) {}
    list_node_entry(uint64_t k, Message * m) : key(k), txn(nullptr), msg(m) {}
    ~list_node_entry() {}
};

template<typename T>
class LockList {
public:
    ListNode<T>* head;
    ListNode<T>* tail;
    std::atomic<size_t> count{0};
    std::atomic<size_t> actual_count{0};

    // structural lock for list modifications (cleanup/clear)
    std::mutex list_mtx;
    // garbage nodes waiting for reclamation (protected by garbage_mtx)
    std::vector<ListNode<T>*> garbage_nodes;
    std::mutex garbage_mtx;

    // For Debuging
    std::string name;

public:
    LockList() {
        head = new ListNode<T>(T());
        tail = head;
        count.store(0, std::memory_order_relaxed);
        actual_count.store(0, std::memory_order_relaxed);
        name="";
    }

    LockList(std::string list_name) : name(list_name) {
        head = new ListNode<T>(T());
        tail = head;
        count.store(0, std::memory_order_relaxed);
        actual_count.store(0, std::memory_order_relaxed);
    }

    ~LockList() {
        clear();
    }

    // Append at tail (protected by tail node's lock and list_mtx for safety)
    ListNode<T>* insert(const T& value, uint64_t thd_id) {
        ListNode<T>* node = new ListNode<T>(value);
        std::lock_guard<std::mutex> g(list_mtx);
        ListNode<T>* orig_tail = nullptr;
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
            // std::string debug_str = generate_debug_string(value_cast);
            std::string debug_str = "";
            DEBUG_LOCKFREE("[LockList:%s] thd %ld insert_tail key=%lu %s size=%zu-%zu minSid=%lu\n", name.c_str(), thd_id, value_cast->key, debug_str.c_str(), size(), actual_size(), minSid);
        #endif
        return node;
    }

    // 这个加锁，得是细粒度的锁，参考try_take函数
    bool mark_consumed(std::function<bool(T)> cond, uint64_t thd_id) {
        ListNode<T>* prev = head;
        // lock prev node first (head)
        prev->mtx.lock();
        ListNode<T>* curr = prev->next;
        while (curr) {
            // lock current node
            curr->mtx.lock();
            if (cond(curr->data)) {
                if (curr->status == NODE_AVAILABLE) {
                    // mark removed under curr lock
                    curr->status = NODE_REMOVED;
                    count.fetch_sub(1, std::memory_order_relaxed);
                    #if DEBUG_LOCKFREE_LIST
                        extern uint64_t minSid;
                        list_node_entry * value_cast = static_cast<list_node_entry *>(curr->data);
                        // std::string debug_str = generate_debug_string(value_cast);
                        std::string debug_str = "";
                        DEBUG_LOCKFREE("[LockList:%s] thd %ld mark_consumed key=%lu %s size=%zu-%zu minSid=%lu\n", name.c_str(), thd_id, value_cast->key, debug_str.c_str(), size(), actual_size(), minSid);
                    #endif
                    // release locks
                    curr->mtx.unlock();
                    prev->mtx.unlock();
                    return true;
                } else {
                    // already taken or removed
                    curr->mtx.unlock();
                    prev->mtx.unlock();
                    return false;
                }
            }
            // move forward: unlock prev, advance prev and curr (prev stays locked for next iteration)
            prev->mtx.unlock();
            prev = curr;
            curr = curr->next;
            // keep prev locked for next loop
        }
        // unlock last prev (head or last node) if locked
        prev->mtx.unlock();
        return false; // not found
    }

    bool mark_consumed_by_key(uint64_t key, uint64_t thd_id) {
        return mark_consumed([key](T data) {
            list_node_entry * value_cast = static_cast<list_node_entry *>(data);
            return value_cast->key == key;
        }, thd_id);
    }

    bool mark_consumed_by_pointer(ListNode<T>* target_node, uint64_t thd_id) {
        target_node->mtx.lock();
        if (target_node->status == NODE_AVAILABLE) {
            target_node->status = NODE_REMOVED;
            count.fetch_sub(1, std::memory_order_relaxed);
            // #if DEBUG_LOCKFREE_LIST
            //     list_node_entry * value_cast = static_cast<list_node_entry *>(target_node->data);
            //     std::string debug_str = generate_debug_string(value_cast);
            //     DEBUG_LOCKFREE("[LockList:%s] thd %ld mark_consumed_by_pointer key=%lu %s size=%zu-%zu\n", name.c_str(), thd_id, value_cast->key, debug_str.c_str(), size(), actual_size());
            // #endif
        }
        target_node->mtx.unlock();
        return true;
    }
    // Physically remove nodes that have been logically removed. Coarse-grained with list_mtx.
    void remove_consumed() {
        std::lock_guard<std::mutex> g(list_mtx);
        ListNode<T>* prev = head;
        ListNode<T>* curr = prev->next;
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
            for (ListNode<T>* n : garbage_nodes) {
                delete n;
            }
            garbage_nodes.clear();
        }
    }
    
    // Clear entire list
    void clear() {
        std::lock_guard<std::mutex> g(list_mtx);
        ListNode<T>* node = head;
        while (node) {
            ListNode<T>* next = node->next;
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
        ListNode<T>* current = head->next;
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
        DEBUG_TIME("[LockList:%s] DEBUG_PRINT_LIST_LENGTH size=%zu actual_size=%zu\n", name.c_str(), sz, actual_sz);
    }
};

// 通用的Locklist
class TxnMsgLockList : public LockList<list_node_entry*> {
public:
    TxnMsgLockList() : LockList<list_node_entry*>("TxnMsgLockList") {}
    TxnMsgLockList(std::string list_name) : LockList<list_node_entry*>(list_name) {}
    ~TxnMsgLockList() {}

public:
    std::string generate_debug_string(list_node_entry * entry) {
        if (entry->entry_type == ENTRY_TYPE::TYPE_TXN) {
            return "txn=[" + std::to_string(entry->txn->get_batch_id()) + "," + std::to_string(entry->txn->get_txn_id()) + "]";
        } else if (entry->entry_type == ENTRY_TYPE::TYPE_MSG) {
            return "msg[" + std::to_string(entry->msg->get_batch_id()) + "," + std::to_string(entry->msg->get_txn_id()) + "]";
        }
        return "unknown_entry_type";
    }

    // Try to take a node matching cond1 and cond2. Thread-safe via hand-over-hand locking.
    bool try_take(std::function<bool(list_node_entry*)> cond1, std::function<bool(list_node_entry*)> cond2, list_node_entry*& out, uint64_t thd_id) {
        ListNode<list_node_entry*>* prev = head;
        // 获取prev锁
        prev->mtx.lock();
        ListNode<list_node_entry*>* curr = prev->next;
        std::string debug_str;
        #if PRINT_VISIT_LIST
            std::vector<uint64_t> visited_keys;
            std::string visit_log;
        #endif
        DEBUG_LOCKFREE("[LockList:%s] thd %ld try_take start size=%zu-%zu minSid=%lu\n", name.c_str(), thd_id, size(), actual_size(), minSid);
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
                curr->next = nullptr;

                actual_count.fetch_sub(1, std::memory_order_relaxed);
                // unlock and return
                curr->mtx.unlock();
                prev->mtx.unlock();
                #if PRINT_VISIT_LIST
                    std::string result = "[LockList" + name + "] thd " + std::to_string(thd_id) + " try_take visited keys: " + visit_log + "| TAKE key=" + std::to_string(value_cast->key);
                    std::cout << result << std::endl;
                #endif
                #if DEBUG_LOCKFREE_LIST
                    extern uint64_t minSid;
                    debug_str = generate_debug_string(value_cast);
                    DEBUG_LOCKFREE("[LockList:%s] thd %ld try_take key=%lu %s size=%zu-%zu minSid=%lu\n", name.c_str(), thd_id, value_cast->key, debug_str.c_str(), size(), actual_size(), minSid);
                #endif
                return true;
            } else {
                debug_str = generate_debug_string(value_cast);
                DEBUG_LOCKFREE("[LockList:%s] thd %ld try_take skip key=%lu %s size=%zu-%zu minSid=%lu\n", name.c_str(), thd_id, value_cast->key, debug_str.c_str(), size(), actual_size(), minSid);
            }
            // move forward: unlock prev, advance
            prev->mtx.unlock();
            prev = curr;
            curr = curr->next;
            // keep prev locked for next iteration
        }
        // unlock last prev if locked
        prev->mtx.unlock();
        DEBUG_LOCKFREE("[LockList:%s] thd %ld try_take failed size=%zu-%zu minSid=%lu\n", name.c_str(), thd_id, size(), actual_size(), minSid);
        #if PRINT_VISIT_LIST
            std::string result = "[LockList" + name + "] thd " + std::to_string(thd_id) + " try_take visited keys: " + visit_log + "| NO TAKE";
            std::cout << result << std::endl;
        #endif
        return false;
    }
};

#endif // _LOCK_FREE_LIST_H