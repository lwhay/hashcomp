//
// Created by Michael on 6/23/21.
//

#ifndef STREAM_ARCALGORITHM_H
#define STREAM_ARCALGORITHM_H

#include <vector>

template<typename IT>
class ARCAlgorithm {
private:
//creating a hash file through array
    size_t hashSize = 10000;
    uint64_t *Hash;

//we use vector(dynamic array) data structures to represent queues.
/*
A ARC Cache consisting of 4 Queues
 mrug (B1)- Most Recently Used Ghost
 mru (T1) - Most Recently Used
 mfu (T2) - Most Frequently Used
 mfug (B2) - Most Frequently Used Ghost
*/

    std::vector<IT> mrug, mru, mfu, mfug;

    float p = 0.0;
    size_t c, cnt = 0;
    size_t HitCount = 0, MissCount = 0;
//Global cache size, taken as input by user
    unsigned int cacheSize;

//A function to check whether Page x is available in 'v' queue
    int check(std::vector<IT> v, IT x) {
        unsigned int l = v.size(), i;
        for (i = 0; i < l; i++) {
            if (v[i] == x)
                return 1;
        }
        return 0;
    }

//A function to insert page 'i' in 'v' queue.
    void queue_insert(std::vector<IT> &v, IT i) {
        if (v.size() == cacheSize)
            v.erase(v.begin());
        v.push_back(i);
    }

//function to pop LRU element from queue 'v'
    void queue_delete(std::vector<IT> &v) {
        if (v.size() > 0)
            v.erase(v.begin());
    }


//function to move a particular page from one queue to another, 'x' from queue 'v' to queue 'w'
    void movefrom(std::vector<IT> &v, std::vector<IT> &w, IT x) {
        int i, j, l = v.size();
        for (i = 0; i < l; i++)
            if (v[i] == x) {
                v.erase(v.begin() + i);
                break;
            }

        if (w.size() == cacheSize)
            w.erase(w.begin());
        w.push_back(x);
    }

/*
Replace subroutine as specified in the reference paper
This function is called when a page with given 'pageNumber' is to be moved from
T2 to B2 or T1 to B1. Basically this function is used to move the elements out from
one list and add it to the another list beginning.
*/
    void Replace(const IT i, const float p) {
        if ((mru.size() >= 1) && ((mru.size() > p) || (check(mfug, i)) && (p == mru.size()))) {
            if (mru.size() > 0) {
                movefrom(mru, mrug, mru[0]);
            }
        } else {
            if (mfu.size() > 0) {
                movefrom(mfu, mfug, mfu[0]);
            }
        }
    }

public:
    ARCAlgorithm(size_t capacity) : cacheSize(capacity), c(capacity), hashSize(3 * capacity), Hash(new IT[hashSize]) {
        std::memset(Hash, 0, sizeof(IT) * hashSize);
        // std::cout << cacheSize << " " << hashSize << std::endl;
    }

    ~ARCAlgorithm() { delete[] Hash; }

    size_t getMiss() { return MissCount; }

    size_t getHit() { return HitCount; }

//function to look object through given key.
    void arc_lookup(IT i) {
        if (Hash[i % hashSize] > 0) {
            //Case 1: Page found in MRU
            if (check(mru, i)) {
                HitCount++;
                movefrom(mru, mfu, i);
            }
                //Case 1: Part B:Page found in MFU
            else if (check(mfu, i)) {
                HitCount++;
                movefrom(mfu, mfu, i);
            }
                //Case 2: Page found in MRUG
            else if (check(mrug, i)) {
                MissCount++;
                p = (float) std::min((float) c, (float) (p + std::max((mfug.size() * 1.0) / mrug.size(), 1.0)));
                Replace(i, p);
                movefrom(mrug, mfu, i);
            }
                //Case 3: Page found in MFUG
            else if (check(mfug, i)) {
                MissCount++;
                p = (float) std::max((float) 0.0, (float) (p - std::max((mrug.size() * 1.0) / mfug.size(), 1.0)));
                Replace(i, p);
                movefrom(mfug, mfu, i);
            }

                //Case 4:  Page not found in any of the queues.
            else {
                MissCount++;
                //Case 4: Part A: When L1 has c pages
                if ((mru.size() + mrug.size()) == c) {
                    if (mru.size() < c) {
                        Hash[mrug[0] % hashSize]--;


                        queue_delete(mrug);
                        Replace(i, p);
                    } else {

                        Hash[mru[0] % hashSize]--;

                        queue_delete(mru);
                    }
                }
                    // Case 4: Part B: L1 has less than c pages
                else if ((mru.size() + mrug.size()) < c) {
                    if ((mru.size() + mfu.size() + mrug.size() + mfug.size()) >= c) {
                        if ((mru.size() + mfu.size() + mrug.size() + mfug.size()) == (2 * c)) {

                            Hash[mfug[0] % hashSize]--;

                            queue_delete(mfug);
                        }
                        Replace(i, p);
                    }

                }
                //Move the page to the most recently used position
                queue_insert(mru, i);
                Hash[i % hashSize]++;
            }
        } else {
            //Page not found, increase miss count
            MissCount++;

            //Case 4: Part A: L1 has c pages
            if ((mru.size() + mrug.size()) == c) {
                if (mru.size() < c) {
                    Hash[mrug[0] % hashSize]--;

                    queue_delete(mrug);
                    Replace(i, p);
                } else {
                    size_t idx = mru[0];
                    Hash[mru[0] % hashSize]--;
                    queue_delete(mru);
                }
            }

                //Case 4: Part B: L1 less than c pages
            else if ((mru.size() + mrug.size()) < c) {
                if ((mru.size() + mfu.size() + mrug.size() + mfug.size()) >= c) {
                    if ((mru.size() + mfu.size() + mrug.size() + mfug.size()) == 2 * c) {
                        Hash[mfug[0] % hashSize]--;

                        queue_delete(mfug);
                    }
                    Replace(i, p);
                }
            }

            //Move the page to the most recently used position
            queue_insert(mru, i);
            Hash[i % hashSize]++;
        }
    }
};

#endif //STREAM_ARCALGORITHM_H
