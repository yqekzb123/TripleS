// #ifndef LOCK_FREE_LINKED_LIST_HPP
// #define LOCK_FREE_LINKED_LIST_HPP

// #include <stdio.h>
// #include <utility>
// #include <mutex>
// #include <iostream>
// #include <cstdint>
// #include <tuple>
// #include <global.h>

// #define keytype uint64_t
// #define eps 1
// // std::mutex mtx;

// // User-defined atomic compare-and-swap function

// struct Successor{
//     public:
//     unsigned long long packed_succ=0;
//     void set_right(struct NodeBase *right){
//         unsigned long long val=(unsigned long long)right&(~(((unsigned long long)0xffff)<<48));
//         packed_succ|=val;
//     }
//     void set_mark(bool mark){
//         if(mark){
//             packed_succ|=(((unsigned long long)0x0001)<<48);
//         }else{
//             packed_succ&=(~(((unsigned long long)0x0001)<<48));
//         }
//     }
//     void set_flag(bool flag){
//          if(flag){
//             packed_succ|=(((unsigned long long)0x0002)<<48);
//         }else{
//             packed_succ&=(~(((unsigned long long)0x0002)<<48));
//         }
//     }
//     struct NodeBase* get_right()
//     {
//         unsigned long long val=packed_succ&(~(((unsigned long long)0xffff)<<48));
//         return (struct NodeBase*)val;
//     }
//     bool get_mark()
//     {
//         return packed_succ & (((unsigned long long)0x0001)<<48);
//     }
//     bool get_flag()
//     {
//         return packed_succ & (((unsigned long long)0x0002)<<48);
//     }

//     Successor(NodeBase* ptr,int mrk, int flg){
//         set_right(ptr);
//         set_flag(flg);
//         set_mark(mrk);
//     }
//     Successor(unsigned long long val) {
//         packed_succ = val;
//     }
//     Successor(){}
// };

// struct NodeBase {
//     public:
//     keytype key;
//     NodeBase* backlink;
//     Successor succ;
//     NodeBase(keytype k)
//     {
//         key=k;
//     }
//     NodeBase(){}
// };

// template<typename T>
// struct Node : public NodeBase {
//     public:
//     T element;
//     Node(keytype k, T e) : NodeBase(k)
//     {
//         element=e;
//     }
//     Node(){}
// };

// template<typename T>
// T CAS(T* address, T expected, T newValue) {
//     T result;
//     asm volatile (
//         "mov %2, %%rax;"
//         "lock cmpxchg %3, %1;"   // Compare and exchange oldVal with the value at address
//         "mov %%rax, %0;"               // Set result to 1 if successful (ZF flag is set), 0 otherwise
//         : "=r" (result), "+m" (*address) // Output operands (result, address)
//         : "r" (expected), "r" (newValue)    // Input operands (oldVal, newVal)
//         : "rax", "memory"          // Clobbers (condition codes and memory)
//     );
//     return result;
// }

// template<typename T>
// class LockFreeLinkedList {
// public:
//     LockFreeLinkedList() {
//         // Create head node with key of negative infinity
//         head = new Node<T>();
//         head->key = 0;
//         head->succ=Successor();
//         head->backlink=NULL;

//         // Create tail node with key of positive infinity
//         tail = new Node<T>();
//         tail->key = __UINT64_MAX__;
//         tail->succ=Successor();
//         tail->backlink=NULL;

//         // Connect head and tail
//         head->succ.set_right(tail);
//     }

//     Node<T>* search(keytype k);
//     Node<T>* insert(keytype k, T e);
//     Node<T>* remove(keytype k);
//     Node<T>* get_head();
//     void HelpMarked(Node<T>* prevNode, Node<T>* delNode);

// private:
//     Node<T>* head;
//     Node<T>* tail;
//     // std::mutex mtx;

//     std::pair<Node<T>*, Node<T>*> SearchFrom(keytype k, Node<T>* currNode);
//     std::pair<Node<T>*, bool> TryFlag(Node<T>* prevNode, Node<T>* targetNode);
//     void HelpFlagged(Node<T>* prevNode, Node<T>* delNode);
//     void TryMark(Node<T>* delNode);
// };

// #endif

#ifndef LOCK_FREE_LINKED_LIST_HPP
#define LOCK_FREE_LINKED_LIST_HPP

#include<stdio.h>
#include<utility>
#include <mutex>
#include<iostream>
#include<cstdint>
#include<tuple>

#define keytype uint64_t
#define eps 1
// std::mutex mtx;

// User-defined atomic compare-and-swap function

struct Successor{
    public:
    // struct Node* right=nullptr;
    // bool mark=0;
    // bool flag=0;
    unsigned long long packed_succ=0;
    void set_right(struct Node *right){
        // std::cout<<right<<"\n";
        unsigned long long val=(unsigned long long)right&(~(((unsigned long long)0xffff)<<48));
        packed_succ|=val;
        // std::cout<<packed_succ<<"\n";
    }
    void set_mark(bool mark){
        if(mark){
            packed_succ|=(((unsigned long long)0x0001)<<48);
        }else{
            packed_succ&=(~(((unsigned long long)0x0001)<<48));
        }
    }
    void set_flag(bool flag){
         if(flag){
            packed_succ|=(((unsigned long long)0x0002)<<48);
        }else{
            packed_succ&=(~(((unsigned long long)0x0002)<<48));
        }
    }
    struct Node* get_right()
    {
        // std::cout<<packed_succ<<"\n";
        unsigned long long val=packed_succ&(~(((unsigned long long)0xffff)<<48));
        return (struct Node*)val;
    }
    bool get_mark()
    {
        if(packed_succ & (((unsigned long long)0x0001)<<48)) return true;
        else return false;
    }
    bool get_flag()
    {
        if(packed_succ & (((unsigned long long)0x0002)<<48)) return true;
        else return false;
    }

    Successor(Node* ptr,int mrk, int flg){
        // right=ptr;
        // mark=mrk;
        // flag=flg;
        set_right(ptr);
        set_flag(flg);
        set_mark(mrk);
    }
    Successor(){}
    // bool operator==(const Successor other) const {
    //     return (this->right==other.right && this->mark==other.mark && this->flag==other.flag);
    // }
};

struct Node {
    public:
    keytype key;
    void * element;
    Node* backlink;
    Successor succ;
    Node(keytype k, void * e)
    {
        key=k;
        element=e;
    }
    Node(){}
};

template<typename T>
T CAS(T* address, T expected, T newValue) {
    T result;
    asm volatile (
        "mov %2, %%rax;"
        "lock cmpxchg %3, %1;"   // Compare and exchange oldVal with the value at address
        "mov %%rax, %0;"               // Set result to 1 if successful (ZF flag is set), 0 otherwise
        : "=r" (result), "+m" (*address) // Output operands (result, address)
        : "r" (expected), "r" (newValue)    // Input operands (oldVal, newVal)
        : "rax", "memory"          // Clobbers (condition codes and memory)
    );
    return result;
}

class LockFreeLinkedList {
public:
    LockFreeLinkedList() {
        // Create head node with key of negative infinity
        head = new Node();
        head->key = 0;
        head->succ=Successor();
        head->backlink=NULL;

        // Create tail node with key of positive infinity
        tail = new Node();
        tail->key = __UINT64_MAX__;;
        tail->succ=Successor();
        tail->backlink=NULL;

        head->succ.set_right(tail);
    }

    Node* search(keytype k);
    Node* insert(keytype k, void * e);
    Node* remove(keytype k);
    Node* get_head() {return head;}
    void HelpMarked(Node* prevNode, Node* delNode);

private:
    Node* head;
    Node *tail;
    // std::mutex mtx;

    std::pair<Node*, Node*> SearchFrom(keytype k, Node* currNode);
    std::pair<Node*, bool> TryFlag(Node* prevNode, Node* targetNode);
    void HelpFlagged(Node* prevNode, Node* delNode);
    void TryMark(Node* delNode);
};

#endif
