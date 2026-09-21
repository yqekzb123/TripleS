#ifndef SDMVCC_INDEX_H
#define SDMVCC_INDEX_H

#include <cstdint>
#include <cassert>

// Intrusive treap: storage belongs to the row/version, never allocated while
// holding its latch. Equal thresholds are ordered by node address.
struct SDMVCCIndexNode {
    SDMVCCIndexNode *left, *right, *parent;
    uint64_t key;
    uint32_t priority;
    bool linked;
    void init(uint64_t k) {
        left = right = parent = nullptr;
        key = k;
        uint64_t h = reinterpret_cast<uintptr_t>(this) ^ k;
        h ^= h >> 30; h *= 0xbf58476d1ce4e5b9ULL;
        h ^= h >> 27; h *= 0x94d049bb133111ebULL;
        priority = static_cast<uint32_t>(h ^ (h >> 31));
        linked = false;
    }
};

class SDMVCCIndex {
    SDMVCCIndexNode *root_;
    static bool less(const SDMVCCIndexNode *a, const SDMVCCIndexNode *b) {
        return a->key < b->key || (a->key == b->key &&
            reinterpret_cast<uintptr_t>(a) < reinterpret_cast<uintptr_t>(b));
    }
    void rotate_up(SDMVCCIndexNode *n) {
        SDMVCCIndexNode *p = n->parent, *g = p->parent;
        if (p->left == n) {
            p->left = n->right;
            if (n->right) n->right->parent = p;
            n->right = p;
        } else {
            p->right = n->left;
            if (n->left) n->left->parent = p;
            n->left = p;
        }
        p->parent = n; n->parent = g;
        if (!g) root_ = n;
        else if (g->left == p) g->left = n;
        else g->right = n;
    }
public:
    SDMVCCIndex() : root_(nullptr) {}
    SDMVCCIndexNode *first() const {
        SDMVCCIndexNode *n = root_;
        if (n) while (n->left) n = n->left;
        return n;
    }
    SDMVCCIndexNode *lower_bound(uint64_t key) const {
        SDMVCCIndexNode *n = root_, *result = nullptr;
        while (n) {
            if (n->key >= key) { result = n; n = n->left; }
            else n = n->right;
        }
        return result;
    }
    SDMVCCIndexNode *before(uint64_t key) const {
        SDMVCCIndexNode *n = root_, *result = nullptr;
        while (n) {
            if (n->key < key) { result = n; n = n->right; }
            else n = n->left;
        }
        return result;
    }
    void insert(SDMVCCIndexNode *n) {
        assert(!n->linked);
        n->left = n->right = n->parent = nullptr;
        SDMVCCIndexNode **slot = &root_, *parent = nullptr;
        while (*slot) {
            parent = *slot;
            slot = less(n, parent) ? &parent->left : &parent->right;
        }
        *slot = n; n->parent = parent; n->linked = true;
        while (n->parent && n->priority < n->parent->priority) rotate_up(n);
    }
    void erase(SDMVCCIndexNode *n) {
        if (!n->linked) return;
        while (n->left || n->right) {
            SDMVCCIndexNode *child = !n->right ? n->left :
                (!n->left ? n->right :
                 (n->left->priority < n->right->priority ? n->left : n->right));
            rotate_up(child);
        }
        if (!n->parent) root_ = nullptr;
        else if (n->parent->left == n) n->parent->left = nullptr;
        else n->parent->right = nullptr;
        n->parent = nullptr; n->linked = false;
    }
};
#endif
