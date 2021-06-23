//
// Created by Michael on 6/23/21.
//

#ifndef STREAM_DEFAULTLRUALGORITHM_H
#define STREAM_DEFAULTLRUALGORITHM_H

template<typename IT>
class DefaultLRUAlgorithm {
private:
    size_t MissCount = 0, HitCount = 0, cacheSize;

public:
    DefaultLRUAlgorithm(size_t capacity) : cacheSize(capacity) {}

/*
Function to implement LRU page referencing algorithm
*/
    void findAndReplace(std::vector<IT> &LRU_queue, long pageNum) {
        size_t trav, l = LRU_queue.size(), pos;
        int flag = 0;
        for (pos = 0; pos < l; pos++) {
            if (LRU_queue[pos] == pageNum) {
                flag = 1;
                break;
            }
        }

        if (!flag) {
            if (l == cacheSize) {
                LRU_queue.pop_back();
            }
            LRU_queue.insert(LRU_queue.begin(), pageNum);
            MissCount++;
        } else {
            unsigned int temp = LRU_queue[pos];
            LRU_queue.erase(LRU_queue.begin() + pos);
            LRU_queue.insert(LRU_queue.begin(), temp);
            HitCount++;
        }
    }

    size_t getMiss() { return MissCount; }

    size_t getHit() { return HitCount; }
};

#endif //STREAM_DEFAULTLRUALGORITHM_H
