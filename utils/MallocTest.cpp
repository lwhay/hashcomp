//
// Created by iclab on 2/10/20.
//

#include <iostream>
#include <queue>
#include <pthread.h>
#include "tracer.h"

#define USE_STD_QUEUE 1

size_t total_count = (1 << 20);
size_t total_round = (1 << 4);
size_t thread_number = 1;
size_t queue_enabled = 1;

size_t *thread_time_malloc;
size_t *thread_time_free;

class kvpair {
private:
    size_t key;
    size_t value;
public:
    kvpair(size_t key_, size_t value_) : key(key_), value(value_) {}
};

template<typename T>
class myqueue {
private:
    struct node {
        T element;
        node *next;
    };

    node *nodecache[1 << 12];

    size_t ncursor = 0, ucursor = 0;

    size_t size_ = 0;

    node *head = nullptr, *tail = nullptr;

public:
    myqueue<T>() {
        for (int i = 0; i < (1 << 12); i++) {
            nodecache[i] = new node;
        }
        ncursor = (1 << 12) - 1;
        head = new node;
        head->next = nullptr;
        tail = head;
    }

    ~myqueue<T>() {
        while (head->next != nullptr) {
            node *newhead = head->next;
            delete head;
            head = newhead;
        }
    }

    inline void push(T e) {
        node *n;
        if (ncursor != ucursor) {
            n = nodecache[ucursor];
            ucursor = ++ucursor % (1 << 12);
        } else {
            std::cout << "Seems to be wrong!" << std::endl;
            n = new node();
        }
        n->element = e;
        n->next = nullptr;
        tail->next = n;
        tail = n;
        size_++;
    }

    inline T &front() {
        return head->next->element;
    }

    inline void pop() {
        node *oldhead = head->next;
        head->next = oldhead->next;
        if (ncursor != ucursor) {
            nodecache[ncursor] = oldhead;
            ncursor = ++ncursor % (1 << 12);
        } else {
            std::cout << "Seems to be wrong!" << std::endl;
            delete oldhead;
        }
        size_--;
    }

    inline size_t size() { return size_; }
};

kvpair ***workloads;
size_t malloc_time = 0, free_time = 0;

void *measureWorker(void *args) {
    int tid = *(int *) args;
#if USE_STD_QUEUE
    std::queue<kvpair *> cache;
#else
    myqueue<kvpair *> cache;
#endif
    Tracer tracer;
    for (int r = 0; r < total_round; r++) {
        tracer.startTime();
        for (int i = 0; i < total_count; i++) {
            workloads[tid][i] = new kvpair(i, i);
        }
        thread_time_malloc[tid] += tracer.getRunTime();
        tracer.startTime();
        for (int i = 0; i < total_count; i++) {
            if (queue_enabled != 0) {
                cache.push(workloads[tid][i]);
                if (cache.size() > (1 << 10)) {
                    delete cache.front();
                    cache.pop();
                }
            } else {
                delete workloads[tid][i];
            }
        }
        for (int i = 0; i < cache.size(); i++) {
            delete cache.front();
            cache.pop();
        }
        thread_time_free[tid] += tracer.getRunTime();
    }
}

void *ptest() {
    pthread_t *workers = new pthread_t[thread_number];
    workloads = new kvpair **[thread_number];
    thread_time_malloc = new size_t[thread_number];
    thread_time_free = new size_t[thread_number];
    int *tids = new int[thread_number];

    for (int i = 0; i < thread_number; i++) {
        tids[i] = i;
        thread_time_malloc[i] = 0;
        thread_time_free[i] = 0;
        workloads[i] = new kvpair *[total_count];
        pthread_create(&workers[i], nullptr, measureWorker, tids + i);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
        delete[] workloads[i];
    }
    for (int i = 0; i < thread_number; i++) {
        malloc_time += thread_time_malloc[i];
        free_time += thread_time_free[i];
    }
    delete[] tids;
    delete[] thread_time_malloc;
    delete[] thread_time_free;
    delete[] workloads;
    delete[] workers;
}

int main(int argc, char **argv) {
    if (argc > 4) {
        total_count = std::atol(argv[1]);
        total_round = std::atol(argv[2]);
        thread_number = std::atol(argv[3]);
        queue_enabled = std::atol(argv[4]);
    }
    ptest();
    std::cout << "Total: " << total_count << " round: " << total_round << " thread: " << thread_number
              << " malloc throughput: "
              << (double) total_count * total_round * thread_number * thread_number / malloc_time
              << " free throughput: " << (double) total_count * total_round * thread_number * thread_number / free_time
              << std::endl;
    std::cout << malloc_time << " " << free_time << std::endl;
}