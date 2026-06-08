// 单向有序链表（升序），用于按给定的 Compare 插入并快速从头弹出满足条件的元素
// T: 存储的值类型
// Compare: 比较器，默认为 std::less<T>，表示 a < b
#ifndef ORDERED_LIST_H
#define ORDERED_LIST_H

#include <functional>
#include <cstddef>
#include <utility>
#include <vector>

template <typename T, typename Compare = std::less<T>, typename CompareWater = Compare>
class OrderedList {
public:
	struct Node {
		T value;
		Node* next;
		explicit Node(const T& v) : value(v), next(nullptr) {}
		explicit Node(T&& v) : value(std::move(v)), next(nullptr) {}
	};

	explicit OrderedList(const Compare& cmp = Compare(), const CompareWater& cmp2 = CompareWater()) noexcept
		: head(nullptr), sz(0), comp(cmp), comp2(cmp2) {}

	// non-copyable for simplicity (can be added if needed)
	OrderedList(const OrderedList&) = delete;
	OrderedList& operator=(const OrderedList&) = delete;

	OrderedList(OrderedList&& other) noexcept
		: head(other.head), sz(other.sz), comp(std::move(other.comp)) {
		other.head = nullptr; other.sz = 0;
	}

	OrderedList& operator=(OrderedList&& other) noexcept {
		if (this != &other) {
			clear();
			head = other.head; sz = other.sz; comp = std::move(other.comp);
			other.head = nullptr; other.sz = 0;
		}
		return *this;
	}

	~OrderedList() { clear(); }

	bool empty() const noexcept { return head == nullptr; }
	std::size_t size() const noexcept { return sz; }

	// Insert while keeping ascending order according to Compare.
	// Complexity: O(n) to find position, O(1) insertion.
	void insert(const T& v) {
		Node* node = new Node(v);
		insert_node(node);
	}

	void insert(T&& v) {
		Node* node = new Node(std::move(v));
		insert_node(node);
	}

	template <class... Args>
	void emplace(Args&&... args) {
		Node* node = new Node(T(std::forward<Args>(args)...));
		insert_node(node);
	}

	// 从头部弹出所有满足 comp(node->value, watermark) == true 的元素（即 node->value < watermark）
	// 返回已弹出的元素值的 vector（按原链表顺序）
	std::vector<T> pop_less_than(const uint64_t watermark) noexcept {
		std::vector<T> out;
		while (head && comp2(head->value, watermark)) {
			Node* n = head;
			head = head->next;
			out.push_back(std::move(n->value));
			delete n;
			--sz;
		}
		return out;
	}

	bool get_next(const uint64_t watermark, T& out) noexcept {
		// debug_print();
		if (head && comp2(head->value, watermark)) {
			Node* n = head;
			head = head->next;
			out = std::move(n->value);
			delete n;
			--sz;
			return true;
		} else {
			return false;
		}
	}

	// 清空链表
	void clear() noexcept {
		Node* cur = head;
		while (cur) {
			Node* tmp = cur->next;
			delete cur;
			cur = tmp;
		}
		head = nullptr;
		sz = 0;
	}

	// 仅用于调试/遍历 - 返回 head 指针
	Node* front_node() const noexcept { return head; }

private:
	Node* head;
	std::size_t sz;
	Compare comp;
    CompareWater comp2; // 用来给watermark用的

	// helper: insert node into proper position (node must be allocated)
	void insert_node(Node* node) {
		if (!head) {
			head = node; node->next = nullptr; sz = 1; 
			// debug_print();
			return;
		}

		// if node should be inserted before head
		if (comp(node->value, head->value)) {
			node->next = head;
			head = node;
			++sz;
			// debug_print();
			return;
		}

		// find previous node such that prev->value <= node->value and (prev->next == nullptr || node < prev->next)
		Node* prev = head;
		while (prev->next && !comp(node->value, prev->next->value)) {
			prev = prev->next;
		}

		node->next = prev->next;
		prev->next = node;
		++sz;
		// debug_print();
	}

	void debug_print() const noexcept {
		std::ostringstream oss;
		Node* cur = head;
		while (cur) {
			oss << *cur->value << " -> ";
			cur = cur->next;
		}
		oss << "nullptr\n";
		std::cout << oss.str();
	}
};

#endif
