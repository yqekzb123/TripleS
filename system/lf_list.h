#include <atomic>
#include <functional>

template<typename T>
class LockFreeList {
    struct Node {
        T data;
        std::atomic<Node*> next;
        std::atomic<bool> taken;
        Node(const T& d) : data(d), next(nullptr), taken(false) {}
    };

    std::atomic<Node*> head;
    std::atomic<Node*> tail;

public:
    LockFreeList() {
        Node* dummy = new Node(T());
        head.store(dummy);
        tail.store(dummy);
    }

    ~LockFreeList() {
        clear();
    }

    // 回收所有已消费节点（不包括dummy头节点）
    void remove_consumed() {
        Node* prev = head.load();
        Node* curr = prev->next.load();
        while (curr) {
            Node* next = curr->next.load();
            if (curr->taken.load()) {
                // 物理删除curr节点
                if (prev->next.compare_exchange_strong(curr, next)) {
                    delete curr;
                    curr = next;
                    continue;
                }
                // CAS失败，重试
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
    }

    // 多生产者插入
    void insert(const T& value) {
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
    }

    // 多消费者取出满足条件的内容
    bool try_take(std::function<bool(const T&)> cond, T& out) {
        Node* curr = head.load()->next.load();
        while (curr) {
            if (!curr->taken.load() && cond(curr->data)) {
                bool expected = false;
                if (curr->taken.compare_exchange_strong(expected, true)) {
                    out = curr->data;
                    return true;
                }
            }
            curr = curr->next.load();
        }
        return false;
    }
    // ...existing code...
};