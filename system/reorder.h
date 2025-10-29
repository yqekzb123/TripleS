#ifndef _REORDER_H_
#define _REORDER_H_
#include "global.h"
#include "message.h"
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <list>
#include "thread.h"

struct BloomFilter {
	std::vector<uint8_t> bits;
	size_t m_bits = 0;
	size_t k_hashes = 0;
	BloomFilter() = default;
	void init(size_t m_bits_, size_t k_) {
		m_bits = m_bits_;
		k_hashes = k_;
		bits.assign((m_bits + 7) / 8, 0);
	}
	inline void set_bit(size_t pos) {
		bits[pos >> 3] |= (1u << (pos & 7));
	}
	inline bool test_bit(size_t pos) const {
		return bits[pos >> 3] & (1u << (pos & 7));
	}
	void add_key(uint64_t key) {
		if (m_bits == 0 || k_hashes == 0) return;
		std::hash<uint64_t> hfn;
		uint64_t h1 = hfn(key);
		uint64_t h2 = (h1 >> 33) ^ (h1 << 11) ^ 0x9e3779b97f4a7c15ULL;
		for (size_t i = 0; i < k_hashes; ++i) {
			uint64_t combined = h1 + (uint64_t)i * h2;
			size_t pos = (size_t)(combined % (uint64_t)m_bits);
			set_bit(pos);
		}
	}
	bool maybe_contains_key(uint64_t key) const {
		if (m_bits == 0 || k_hashes == 0) return false;
		std::hash<uint64_t> hfn;
		uint64_t h1 = hfn(key);
		uint64_t h2 = (h1 >> 33) ^ (h1 << 11) ^ 0x9e3779b97f4a7c15ULL;
		for (size_t i = 0; i < k_hashes; ++i) {
			uint64_t combined = h1 + (uint64_t)i * h2;
			size_t pos = (size_t)(combined % (uint64_t)m_bits);
			if (!test_bit(pos)) return false;
		}
		return true;
	}
};

struct SentEntry {
	BloomFilter bf_read;
	BloomFilter bf_write;
	uint64_t txn_id = 0;
	bool valid = false;
};

class SlidingWindowReorder {
public:
	SlidingWindowReorder(int delta_, uint64_t thd_id_, int max_delay_ = -1)
	: delta(delta_), thd_id(thd_id_), max_delay(max_delay_), last_move_time_ns(0), next_parent_marker(1) {
		if (max_delay <= 0) max_delay = delta;
		delay_queues.resize((size_t)delta + 1);
		sent_buffer.resize((size_t)delta);
		sent_head = 0;
		last_move_time_ns = 0;
	}

	// extract keys (workload-specific)
	void extract_msg_keys(Message *m, std::vector<uint64_t> &out_read_keys, std::vector<uint64_t> &out_write_keys);

	void compute_bloom_params(size_t n, double p, size_t &m_bits, size_t &k_hashes);

	void build_bloom_for_msg(Message *m, BloomFilter &bf_read, BloomFilter &bf_write);

	bool msg_conflicts_with_sent(Message *m);

	void send_and_register(Message *m);

	void trigger_window_move();

	// If enough time has elapsed since last move, trigger a window move.
	void maybe_timeout_move(uint64_t now_ns, uint64_t timeout_ns);

	// API used from run(): process a new message (returns after handling)
	void process_new_msg(Message *m);

private:
	int delta;
	uint64_t thd_id;
	std::vector<std::list<Message*>> delay_queues;
	std::vector<SentEntry> sent_buffer;
	size_t sent_head;
	uint64_t last_move_time_ns;
	// track per-txn delay counts (keyed by txn_id for safety)
	std::unordered_map<uint64_t, int> delay_counts;
	int max_delay;

	// Next parent marker id generator for grouping child messages created by reorder
	uint64_t next_parent_marker;

	uint64_t temp_txn_id;
};

class ReorderThread : public Thread {
public:
	// ReorderThread(uint64_t thd_id) : Thread(thd_id) {}
	void setup();
	RC run();
private:
	// int delta;
	uint64_t thd_id;
};

#endif
