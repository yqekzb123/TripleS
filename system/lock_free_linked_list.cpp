// #include "lock_free_linked_list.h"
// #include "sim_manager.h"
// #include <tuple>

// Successor compareAndSwap(Successor* address, Successor expected, Successor newValue)
// {
//     unsigned long long result=CAS(&address->packed_succ,expected.packed_succ,newValue.packed_succ);
//     Successor temp_succ=Successor();
//     temp_succ.packed_succ=result;
//     return temp_succ;
// }

// template<typename T>
// std::pair<Node<T>*, Node<T>*> LockFreeLinkedList<T>::SearchFrom(keytype k, Node<T>* currNode) {
//     Node<T>* nextNode = static_cast<Node<T>*>(currNode->succ.get_right());
//     while (nextNode->key <= k) {
//         while (nextNode->succ.get_mark() == 1 && (currNode->succ.get_mark() == 0 || currNode->succ.get_right() != nextNode)) {
//             if (currNode->succ.get_right() == nextNode) {
//                 HelpMarked(currNode, nextNode);
//             }
//             nextNode = static_cast<Node<T>*>(currNode->succ.get_right());
//         }
//         if (nextNode->key <= k) {
//             currNode = nextNode;
//             nextNode = static_cast<Node<T>*>(currNode->succ.get_right());
//         }
//     }
//     return std::make_pair(currNode, nextNode);
// }

// template<typename T>
// Node<T>* LockFreeLinkedList<T>::search(keytype k) {
//     std::pair<Node<T>*,Node<T>*> pair=SearchFrom(k-eps,head);
//     Node<T>* currNode=pair.first;
//     Node<T>* nextNode=pair.second;
//     if (currNode->key == k) {
//         return currNode;
//     } else {
//         return NULL; // NO_SUCH_KEY
//     }
// }

// template<typename T>
// void LockFreeLinkedList<T>::HelpMarked(Node<T>* prevNode, Node<T>* delNode) {
//     Node<T>* nextNode=static_cast<Node<T>*>(delNode->succ.get_right());
//     Successor sc1=Successor(); sc1.set_right(delNode);sc1.set_mark(0);sc1.set_flag(1);
//     Successor sc2=Successor(); sc2.set_right(nextNode);sc2.set_mark(0);sc2.set_flag(0);
//     compareAndSwap(&(prevNode->succ),sc1,sc2);
// }

// template<typename T>
// Node<T>* LockFreeLinkedList<T>::remove(keytype k) {
//     std::pair<Node<T>*,Node<T>*> pair=SearchFrom(k-eps,head);
//     Node<T>* prevNode=pair.first;
//     Node<T>* delNode=pair.second;
//     if(delNode->key!=k){
//         return NULL;
//     }
//     bool result;
//     std::pair<Node<T>*,bool> pair2=TryFlag(prevNode,delNode);
//     prevNode=pair2.first;
//     result=pair2.second;
//     if(prevNode!=NULL){
//         HelpFlagged(prevNode,delNode);
//     }   
//     if(result==false){
//         return NULL;
//     }
//     return delNode;
// }

// // If want to use head outside, do not update it.
// template<typename T>
// Node<T>* LockFreeLinkedList<T>::get_head() {return head;}

// template<typename T>
// void LockFreeLinkedList<T>::HelpFlagged(Node<T>* prevNode, Node<T>* delNode) {
//     delNode->backlink=prevNode;
//     if(delNode->succ.get_mark()==0){
//         TryMark(delNode);
//     }
//     HelpMarked(prevNode,delNode);
// }

// template<typename T>
// void LockFreeLinkedList<T>::TryMark(Node<T>* delNode) {
//     do{
//         Node<T>* nextNode=static_cast<Node<T>*>(delNode->succ.get_right());
//         Successor result=compareAndSwap(&(delNode->succ),Successor(nextNode,0,0),Successor(nextNode,1,0));
//         if(result.get_flag()==1){
//             HelpFlagged(delNode,static_cast<Node<T>*>(result.get_right()));
//         }
//     }while(delNode->succ.get_mark()==0);
// }

// template<typename T>
// std::pair<Node<T>*, bool> LockFreeLinkedList<T>::TryFlag(Node<T>* prevNode, Node<T>* targetNode) {
//     while (true) {
//         if (prevNode->succ.get_flag() == 1) {  //Predecessor is already flagged
//             return std::make_pair(prevNode, false);
//         }
//         Successor result=compareAndSwap(&(prevNode->succ), Successor(targetNode, 0, 0), Successor(targetNode, 0, 1));
//         if (result.get_right()==targetNode && result.get_mark()==0 && result.get_flag()==0) {   //Successful flagging
//             return std::make_pair(prevNode, true);
//         }
//         if (result.get_right()==targetNode && result.get_mark()==0 && result.get_flag()==1) {   //Successful flagging
//             return std::make_pair(prevNode, false);
//         }
//         while (prevNode->succ.get_mark() == 1) {
//             prevNode = static_cast<Node<T>*>(prevNode->backlink);
//         }
//         std::pair<Node<T>*, Node<T>*> searchResult = SearchFrom(targetNode->key - eps, prevNode);
//         if (searchResult.second != targetNode) {
//             return std::make_pair<Node<T>*, bool>(NULL, false);
//         }
//     }
// }

// template<typename T>
// Node<T>* LockFreeLinkedList<T>::insert(keytype k, T e) {
//     std::pair<Node<T>*,Node<T>*> pair=SearchFrom(k,head);
//     NodeBase * prevNode = (NodeBase *) pair.first;
//     NodeBase * nextNode = (NodeBase *) pair.second;
//     // Node<T>* prevNode=pair.first;
//     // Node<T>* nextNode=pair.second;
//     if(prevNode->key==k){
//         return NULL;
//     }
//     Node<T>* newNode= new Node<T>(k,e);
//     while(true)
//     {
//         if(prevNode->succ.get_flag()==1){
//             HelpFlagged(static_cast<Node<T>*>(prevNode),static_cast<Node<T>*>(prevNode->succ.get_right()));
//         }
//         else{
//             newNode->succ=Successor(); 
//             newNode->succ.set_right(nextNode);
//             newNode->succ.set_mark(0);
//             newNode->succ.set_flag(0);
//             Successor result=compareAndSwap(&(prevNode->succ),Successor(nextNode,0,0),Successor(newNode,0,0));
//             if(result.get_right()==nextNode && result.get_mark()==0 && result.get_flag()==0){
//                 return newNode;
//             }
//             else{
//                 if(result.get_flag()==1){
//                     HelpFlagged(static_cast<Node<T>*>(prevNode),static_cast<Node<T>*>(result.get_right()));
//                 }
//                 while(prevNode->succ.get_mark()==1){
//                     prevNode=static_cast<Node<T>*>(prevNode->backlink);
//                 }
//             }
//         }
//         std::pair<Node<T>*,Node<T>*> pair=SearchFrom(k,head);
//         Node<T>* prevNode=pair.first;
//         Node<T>* nextNode=pair.second;
//         if(prevNode->key==k){
//             delete newNode;
//             return NULL;
//         } 
//     }
// }

// template class LockFreeLinkedList<TxnManager*>;

#include "lock_free_linked_list.h"
#include <tuple>

Successor compareAndSwap(Successor* address, Successor expected, Successor newValue)
{
    unsigned long long result=CAS(&address->packed_succ,expected.packed_succ,newValue.packed_succ);
    Successor temp_succ=Successor();
    temp_succ.packed_succ=result;
    return temp_succ;
}



std::pair<Node*, Node*> LockFreeLinkedList::SearchFrom(keytype k, Node* currNode) {
    // std::cout<<"Debug inside the SearchFrom and calling\n";
    Node* nextNode = currNode->succ.get_right();
    // std::cout<<"Debug:"<< currNode<<" inside the SearchFrom and got nextNode\n";
    // std::cout<<"Debug:"<< nextNode<<" inside the SearchFrom and got nextNode\n";
    // std::cout<<"Debug:"<< nextNode->key<<" inside the SearchFrom and got nextNode\n";
    while (nextNode->key <= k) {
    // std::cout<<"Debug inside the SearchFrom and inside loop\n";
        while (nextNode->succ.get_mark() == 1 && (currNode->succ.get_mark() == 0 || currNode->succ.get_right() != nextNode)) {
            if (currNode->succ.get_right() == nextNode) {
                HelpMarked(currNode, nextNode);
            }
            nextNode = currNode->succ.get_right();
        }
        if (nextNode->key <= k) {
            currNode = nextNode;
            nextNode = currNode->succ.get_right();
        }
    }
    // std::cout<<"Debug inside the SearchFrom and outside loop\n";
    return std::make_pair(currNode, nextNode);
}

Node* LockFreeLinkedList::search(keytype k) {
    std::pair<Node*,Node*> pair=SearchFrom(k-eps,head);
    Node* currNode=pair.first;
    Node* nextNode=pair.second;
    if (currNode->key == k) {
        return currNode;
    } else {
        return NULL; // NO_SUCH_KEY
    }
}

void LockFreeLinkedList::HelpMarked(Node* prevNode, Node* delNode) {
    Node* nextNode=delNode->succ.get_right();
    Successor sc1=Successor(); sc1.set_right(delNode);sc1.set_mark(0);sc1.set_flag(1);
    Successor sc2=Successor(); sc2.set_right(nextNode);sc2.set_mark(0);sc2.set_flag(0);
    compareAndSwap(&(prevNode->succ),sc1,sc2);
}

Node* LockFreeLinkedList::remove(keytype k) {
    std::pair<Node*,Node*> pair=SearchFrom(k-eps,head);
    Node* prevNode=pair.first;
    Node* delNode=pair.second;
    if(delNode->key!=k){
        return NULL;
    }
    bool result;
    std::pair<Node*,bool> pair2=TryFlag(prevNode,delNode);
    prevNode=pair2.first;
    result=pair2.second;
    if(prevNode!=NULL){
        HelpFlagged(prevNode,delNode);
    }   
    if(result==false){
        return NULL;
    }
    return delNode;
}

void LockFreeLinkedList::HelpFlagged(Node* prevNode, Node* delNode) {
    delNode->backlink=prevNode;
    if(delNode->succ.get_mark()==0){
        TryMark(delNode);
    }
    HelpMarked(prevNode,delNode);
}

void LockFreeLinkedList::TryMark(Node* delNode) {
    do{
        auto nextNode=delNode->succ.get_right();
        // auto result=delNode->succ;
        auto result=compareAndSwap(&(delNode->succ),Successor(nextNode,0,0),Successor(nextNode,1,0));
        if(result.get_flag()==1){
            HelpFlagged(delNode,result.get_right());
        }
    }while(delNode->succ.get_mark()==0);
}

std::pair<Node*, bool> LockFreeLinkedList::TryFlag(Node* prevNode, Node* targetNode) {
    while (true) {
        if (prevNode->succ.get_flag() == 1) {  //Predecessor is already flagged
            return std::make_pair(prevNode, false);
        }
        // auto result =prevNode->succ;
        auto result=compareAndSwap(&(prevNode->succ), Successor(targetNode, 0, 0), Successor(targetNode, 0, 1));
        if (result.get_right()==targetNode && result.get_mark()==0 && result.get_flag()==0) {   //Successful flagging
            return std::make_pair(prevNode, true);
        }
        if (result.get_right()==targetNode && result.get_mark()==0 && result.get_flag()==1) {   //Successful flagging
            return std::make_pair(prevNode, false);
        }
        while (prevNode->succ.get_mark() == 1) {
            prevNode = prevNode->backlink;
        }
        auto searchResult = SearchFrom(targetNode->key - eps, prevNode);
        if (searchResult.second != targetNode) {
            return std::make_pair<Node *, bool>(NULL, false);
        }
    }
}

Node* LockFreeLinkedList::insert(keytype k, void * e) {
    // int z=7;
    // int y=compareAndSwap(&z,7,6);
    // std::cout<<y<<" "<<z<<std::endl;
    // int val1=3, val2=4,val3=5;
    // std::tuple t=compareAndSwap2(&val1,&val2,&val3, 3,4,5,8,9,10);
    // std::cout<<std::get<0>(t)<<" "<<std::get<1>(t)<<" "<<std::get<2>(t)<<" "<<val1<<" "<<val2<<" "<<val3<<std::endl;
    // std::cout<<"debug Inside the insert and calling searchFrom\n";
    std::pair<Node*,Node*> pair=SearchFrom(k,head);
    // std::cout<<"debug Inside the insert and called searchFrom\n";
    Node* prevNode=pair.first;
    Node* nextNode=pair.second;
    // std::cout<<"prevNode "<<prevNode->key<<" nextNode "<<nextNode->key<<"\n";
    if(prevNode->key==k){
        return NULL;
    }
    auto newNode= new Node(k,e);
    while(true)
    {
        if(prevNode->succ.get_flag()==1){
            HelpFlagged(prevNode,prevNode->succ.get_right());
        }
        else{
            newNode->succ=Successor(); newNode->succ.set_right(nextNode);newNode->succ.set_mark(0);newNode->succ.set_flag(0);
            // auto result=newNode->succ;
            auto result=compareAndSwap(&(prevNode->succ),Successor(nextNode,0,0),Successor(newNode,0,0));
            // std::cout<<"prevNode "<<prevNode->succ.right<<" nextNode "<<nextNode<<" result "<<result.right<<" newNode "<<newNode<<"\n";
            if(result.get_right()==nextNode && result.get_mark()==0 && result.get_flag()==0){
                return newNode;
            }
            else{
                if(result.get_flag()==1){
                    HelpFlagged(prevNode,result.get_right());
                }
                while(prevNode->succ.get_mark()==1){
                    prevNode=prevNode->backlink;
                }
            }
        }
        std::pair<Node*,Node*> pair=SearchFrom(k,head);
        Node* prevNode=pair.first;
        Node* nextNode=pair.second;
        if(prevNode->key==k){
            delete newNode;
            return NULL;
        } 
    }
}