#ifndef _LOCK_FREE_LIST_H
#define _LOCK_FREE_LIST_H

#include <atomic>
#include <memory>
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
class LockFreeList {
    enum NodeStatus : int {
        NODE_AVAILABLE = 0,
        NODE_TAKEN = 1,
        NODE_REMOVED = 2
    };
    struct Node {
        T data;
        std::atomic<Node*> next;
        std::atomic<NodeStatus> status;
        // 加一个风险标记，代表下一个节点正在被删除
        std::atomic<bool> removing{false};
        Node(const T& d) : data(d), next(nullptr), status(NODE_AVAILABLE) {}
    };

    std::atomic<Node*> head;
    std::atomic<Node*> tail;
    std::atomic<size_t> count{0};

    // 还是加一个要被删掉的节点队列，放那些需要删但是还没删除的节点
    std::vector<Node*> garbage_nodes;
public:
    LockFreeList() {
        Node* dummy = new Node(T());
        head.store(dummy);
        tail.store(dummy);
        count.store(0, std::memory_order_relaxed);
    }

    ~LockFreeList() {
        clear();
    }

    // 多生产者插入（尾插）
    void insert(const T& value, uint64_t thd_id) {
        Node* node = new Node(value);
        Node* prev_tail;
        while (true) {
            prev_tail = tail.load();
            Node* null_ptr = nullptr;
            if (prev_tail->next.compare_exchange_weak(null_ptr, node)) {
                // 插入成功，更新tail
                tail.compare_exchange_strong(prev_tail, node);
                break;
            } else {
                // 有其他线程插入了，推进tail
                tail.compare_exchange_strong(prev_tail, prev_tail->next.load());
            }
        }
        count.fetch_add(1, std::memory_order_relaxed);
        // DEBUG_LOCKFREE: 入链表
        #if DEBUG_LOCKFREE_LIST
            extern uint64_t minSid;
            list_node_entry * value_cast = static_cast<list_node_entry *>(value);
            DEBUG_LOCKFREE("[LockFreeList] thd %ld insert_tail key=%lu txn=[%ld,%ld] size=%zu minSid=%lu\n", thd_id, value_cast->key, value_cast->txn->get_batch_id(), value_cast->txn->get_txn_id(), size(), minSid);
        #endif
    }

    // 多消费者遍历并取出满足条件的内容
    bool try_take(std::function<bool(const T&)> cond, T& out, uint64_t thd_id) {
        Node* curr = head.load()->next.load();
        Node* prev = head.load();
        #if PRINT_VISIT_LIST
            std::vector<uint64_t> visited_keys;
            std::string visit_log;
        #endif
        while (curr) {
            // 跳过已被删除的节点和危险节点
            if (curr->status.load() == NODE_REMOVED || curr->status.load() == NODE_TAKEN) {
                curr = curr->next.load();
                continue;
            }
            list_node_entry * value_cast = static_cast<list_node_entry *>(curr->data);
            #if PRINT_VISIT_LIST
                visited_keys.push_back(value_cast->key);
                visit_log += std::to_string(value_cast->key) + "->";
            #endif
            NodeStatus expected = NODE_AVAILABLE;
            if (curr->status.load() == NODE_AVAILABLE && cond(curr->data)) {
                if (curr->status.compare_exchange_strong(expected, NODE_TAKEN)) {
                    // 标记为taken，执行器可安全处理
                    out = curr->data;
                    count.fetch_sub(1, std::memory_order_relaxed);

                    // 打印遍历过的key顺序
                    
                    #if PRINT_VISIT_LIST
                        std::string result = "[LockFreeList] thd " + std::to_string(thd_id) + " try_take visited keys: " + visit_log + "| TAKE key=" + std::to_string(value_cast->key);
                        std::cout << result << std::endl;
                    #endif
                    #if DEBUG_LOCKFREE_LIST
                        extern uint64_t minSid;
                        DEBUG_LOCKFREE("[LockFreeList] thd %ld try_take key=%lu txn=[%ld,%ld] size=%zu minSid=%lu\n", thd_id, value_cast->key, value_cast->txn->get_batch_id(), value_cast->txn->get_txn_id(), size(), minSid);
                    #endif
                    
                    // 处理完后，调用者应将status设为NODE_REMOVED
                    curr->status.store(NODE_REMOVED);
                    return true;
                }
            }
            prev = curr;
            curr = curr->next.load();
        }
        // 打印遍历过的key顺序
        
        #if PRINT_VISIT_LIST
            std::string result = "[LockFreeList] thd " + std::to_string(thd_id) + " try_take visited keys: " + visit_log + "| NO TAKE";
            std::cout << result << std::endl;
        #endif
        #if DEBUG_LOCKFREE_LIST
            DEBUG_LOCKFREE("[LockFreeList] thd %ld try_take find NULL\n", thd_id);
        #endif
        return false;
    }

    // 回收所有已被移除的节点（不包括dummy头节点）
    void remove_consumed() {
        Node* prev = head.load();
        Node* curr = prev->next.load();
        while (curr) {
            Node* next = curr->next.load();
            if (curr->status.load() == NODE_REMOVED) {
                // 标记prev为危险，防止try_take并发访问
                prev->removing.store(true);
                if (prev->next.compare_exchange_strong(curr, next)) {
                    // delete curr;
                    garbage_nodes.push_back(curr);
                    curr = next;
                    prev->removing.store(false);
                    continue;
                }
                prev->removing.store(false);
                curr = prev->next.load();
                continue;
            }
            prev = curr;
            curr = next;
        }
    }

    // 清空整个链表（包括dummy头节点）
    void clear() {
        Node* node = head.load();
        while (node) {
            Node* next = node->next.load();
            delete node;
            node = next;
        }
        head.store(nullptr);
        tail.store(nullptr);
        count.store(0, std::memory_order_relaxed);
    }

    // 获取链表节点数
    size_t size() const {
        return count.load(std::memory_order_relaxed);
    }

    // 打印链表（只打印key）
    void print() {
        Node* current = head.load()->next.load();
        while (current) {
            list_node_entry * value_cast = static_cast<list_node_entry *>(current->data);
            std::cout << value_cast->key << " -> ";
            current = current->next.load();
        }
        std::cout << "nullptr" << std::endl;
    }
};

#endif // _LOCK_FREE_LIST_H