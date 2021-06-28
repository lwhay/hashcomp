//
// Created by iclab on 6/28/21.
//

#ifndef HASHCOMP_CONCISELRUALGORITHM_H
#define HASHCOMP_CONCISELRUALGORITHM_H

#define LRU_NEXT_BITS 0xfffffllu
#define LRU_NEXT_MASK 0xfffffffffff00000llu
#define LRU_LEFT_BITS 0xfffff00000llu
#define LRU_LEFT_MASK 0xffffff00000fffffllu
#define LRU_RIGHT_BITS 0x0fffff0000000000llu
#define LRU_RIGHT_MASK 0xf00000ffffffffffllu

class CommonItem {
private:
    uint32_t item;
    uint64_t points; // 0-19: *next, 20-39: *left, 40:59 *right, 60:63 reserved.
public:
    void setItem(uint32_t ihash) { this->item = ihash; }

    uint32_t getItem() { return item; }

    void setLeft(int offset) {
        points &= LRU_LEFT_MASK;
        points |= ((uint64_t) offset << 20);
    }

    int getLeft() { return ((points & LRU_LEFT_BITS) >> 20); }

    void setRight(int offset) {
        points &= LRU_RIGHT_MASK;
        points |= ((uint64_t) offset << 40);
    }

    int getRight() { return ((points & LRU_RIGHT_BITS) >> 40); }

    void setNext(int offset) {
        points &= LRU_NEXT_MASK;
        points |= offset;
    }

    int getNext() {
        return (points & LRU_NEXT_BITS);
    }

};

class ConciseLRUAlgorithm {
protected:
    CommonItem *lrulist;
    CommonItem *head, *tail;
    uint32_t *hashtable;
    size_t count, capacity, _size, hashsize;

public:
    ConciseLRUAlgorithm(int K) : lrulist(new CommonItem[K + 2]), head(&lrulist[1]), tail(&lrulist[1]), _size(K + 1),
                                 count(0), capacity(0), hashsize(3 * _size) {
        hashtable = new uint32_t[hashsize];
        std::memset(lrulist, 0, sizeof(CommonItem) * (2 + _size));
        std::memset(hashtable, 0, sizeof(uint32_t) * hashsize);
        for (int i = 1; i <= this->_size; i++) {
            this->lrulist[i].setRight(i % _size + 1);
            this->lrulist[i].setLeft((i == 1) ? _size : i - 1);
        }
    }

    ~ConciseLRUAlgorithm() {
        delete[] lrulist;
        delete[] hashtable;
    }

    void reset() {
        std::memset(lrulist, 0, sizeof(CommonItem) * _size);
        head = &this->lrulist[1];
        tail = &this->lrulist[1];
        // count = 0;  // accumulate it
        capacity = 0;
        for (int i = 1; i <= this->_size; i++) {
            this->lrulist[i].setRight(i % _size + 1);
            this->lrulist[i].setLeft((i == 1) ? _size : i - 1);
        }
    }

    void verify() {
        for (int i = 0; i < this->hashsize; i++)
            assert(this->hashtable[i] != 0 && (lrulist + this->hashtable[i])->getRight() != 0);
    }

    inline uint32_t put(uint32_t item) {
        uint32_t hashval;
        CommonItem *hashptr;

        uint32_t ret = item;
        this->lrulist->setItem(0);
        hashval = (item) % (hashsize - 1) + 1;
        hashptr = lrulist + this->hashtable[hashval];

        while (hashptr != lrulist) {
            count++;
            if (hashptr->getItem() == item) {
                if (head != hashptr) {
                    if (tail == hashptr) tail = lrulist + tail->getRight();
                    // pick out hashptr
                    (lrulist + hashptr->getLeft())->setRight(hashptr->getRight());
                    (lrulist + hashptr->getRight())->setLeft(hashptr->getLeft());
                    // replace tail by hashptr
                    hashptr->setRight(head - lrulist);
                    hashptr->setLeft(head->getLeft());
                    (lrulist + head->getLeft())->setRight(hashptr - lrulist);
                    head->setLeft(hashptr - lrulist);
                    // std::cout << "--------------------------------------" << std::endl;
                } else if (tail == hashptr) {
                    tail = (lrulist + tail->getRight());
                    head = tail;
                }
                return ret;
            } else hashptr = (lrulist + hashptr->getNext());
        }
        if (count > 1 && head == tail) {
            ret = tail->getItem();
            uint32_t hashret = ret % (hashsize - 1) + 1;
            CommonItem *cur = lrulist + this->hashtable[hashret], *prev = nullptr;
            bool asroot = true;
            while (cur != lrulist) {
                count++;
                if (cur == tail) {
                    this->capacity--;
                    if (asroot) {
                        //if (tail->getNext() != 0) (lrulist +tail->getNext())->setPrev(0);
                        this->hashtable[hashret] = tail->getNext();
                    } else {
                        //if (tail->getNext() != 0) (lrulist +tail->getNext())->setPrev(tail->getPrev());
                        //tail->getPrev()->setNext(tail->getNext());
                        prev->setNext(tail->getNext());
                    }
                    break;
                } else asroot = false;
                prev = cur;
                cur = lrulist + cur->getNext();
            }
            tail = lrulist + tail->getRight();
        }

        if (this->hashtable[hashval] != 0) {
            head->setNext((lrulist + this->hashtable[hashval])->getNext());
            //this->hashtable[hashval]->setPrev(head);
        } else head->setNext(0);
        //head->setPrev(nullptr);
        this->hashtable[hashval] = head - lrulist;
        head->setItem(item);
        head = lrulist + head->getRight();
        capacity++;

        return ret;
    }

    size_t getCount() { return count; }

    void print() {
        CommonItem *cur = head;
        using namespace std;
        char *normal = "\033[0m";
        int retrieved = 0;
        cout << (head - this->lrulist) << "-" << (tail - this->lrulist) << "&";
        do {
            cout << "\033[34m" << (((cur->getItem() + 1) & 0x7fffffff) - 1) << normal << ":"
                 << "\033[33m" << cur->getItem() % (this->hashsize - 1) + 1 << normal << ":"
                 << "\033[31m" << cur->getNext() << normal << ":" << cur->getLeft()
                 << "\033[32m" << cur->getRight() << normal << ":" << (cur - this->lrulist) << "->";
            cur = lrulist + cur->getRight();
            if (retrieved++ > this->_size) exit(-1);
        } while (cur != head);
        std::cout << std::endl;
    }

    CommonItem *find(uint32_t item) {
        uint32_t hashval = item % (hashsize - 1) + 1;
        CommonItem *hashptr = lrulist + hashtable[hashval];
        while (hashptr != lrulist) {
            assert(hashptr != nullptr);
            if (hashptr->getItem() == item) return hashptr;
            else {
                assert(hashptr->getNext() <= _size && hashptr->getNext() >= 0);
                hashptr = lrulist + hashptr->getNext();
            }
        }
        return nullptr;
    }

    bool contains(uint32_t k) { return this->find(k) != nullptr; }

    uint32_t add(uint32_t k) { return this->put(k); }

    uint32_t moveToBack(uint32_t item) { removePage(item, false); }

    size_t getSize() { return this->capacity; }

    uint32_t removePage(uint32_t item, bool remove = true) {
        uint32_t hashval;
        CommonItem *hashptr, *prev = nullptr;
        uint32_t ret = (uint32_t) -1;
        this->lrulist->setItem(0);
        hashval = item % (hashsize - 1) + 1;
        hashptr = lrulist + this->hashtable[hashval];

        while (hashptr != lrulist) {
            count++;
            if (hashptr->getItem() == item) {
                if (remove) {
                    this->capacity--;
                    if (prev == nullptr) {
                        //if (hashptr->getNext() != nullptr) hashptr->getNext()->setPrev(nullptr);
                        this->hashtable[hashval] = hashptr->getNext();
                    } else {
                        //if (hashptr->getNext() != nullptr) hashptr->getNext()->setPrev(hashptr->getPrev());
                        prev->setNext(hashptr->getNext());
                    }
                    if (head->getLeft() == (hashptr - lrulist)) head = lrulist + head->getLeft();
                    else if (head != hashptr) {
                        //if (tail == hashptr) tail = tail->getRight();
                        // pick out hashptr
                        (lrulist + hashptr->getLeft())->setRight(hashptr->getRight());
                        (lrulist + hashptr->getRight())->setLeft(hashptr->getLeft());
                        // replace tail by hashptr
                        hashptr->setRight(head - lrulist);
                        hashptr->setLeft(head->getLeft());
                        (lrulist + head->getLeft())->setRight(hashptr - lrulist);
                        head->setLeft(hashptr - lrulist);
                        head = hashptr;
                    }
                } else {
                    if (tail == hashptr) tail = lrulist + tail->getRight();
                    if (head != hashptr) {
                        // pick out hashptr
                        (lrulist + hashptr->getLeft())->setRight(hashptr->getRight());
                        (lrulist + hashptr->getRight())->setLeft(hashptr->getLeft());
                        // replace tail by hashptr
                        hashptr->setRight(head - lrulist);
                        hashptr->setLeft(head->getLeft());
                        (lrulist + head->getLeft())->setRight(hashptr - lrulist);
                        head->setLeft(hashptr - lrulist);
                    } else {
                        head = lrulist + head->getRight();
                    }
                }
                ret = item;
                break;
            } else {
                prev = hashptr;
                hashptr = lrulist + hashptr->getNext();
            }
        }
        return ret;
    }
};

#endif //HASHCOMP_CONCISELRUALGORITHM_H
