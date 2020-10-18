//
// Created by iclab on 10/18/20.
//

#include <thread>
#include <vector>
#include <atomic>
#include <iostream>
#include <semaphore.h>

#include <mutex>
#include <condition_variable>

class Semaphore {
public:
    Semaphore(int count_ = 0)
            : count(count_) {}

    inline void notify() {
        std::unique_lock<std::mutex> lock(mtx);
        count++;
        cv.notify_one();
    }

    inline void wait() {
        std::unique_lock<std::mutex> lock(mtx);

        while (count == 0) {
            cv.wait(lock);
        }
        count--;
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    int count;
};

using namespace std;

constexpr int THREAD_NUMBER = 8;
constexpr int TOTAL_COUNTER = (1 << 12);

atomic<unsigned long long> mark(1);

atomic<bool> term(false);

void naiveTest() {
    vector<thread> arraytrd;
    atomic<bool> active[THREAD_NUMBER];
    atomic<bool> full[THREAD_NUMBER];
    // Producer
    for (int i = 0; i < THREAD_NUMBER; i++) {
        active[i].store(false);
        full[i].store(false);
        arraytrd.push_back(thread([](atomic<bool> *active, atomic<bool> *full, int tid) {
            cout << tid << endl;
            unsigned long long count = 0;
            while (true) {
                full->store(true);
                bool expect;
                do {
                    expect = true;
                    /*if (tid % THREAD_NUMBER == 0)
                        cout << tid << ":" << count << ":" << active->load() << ":" << full->load() << endl;*/
                } while (!active->compare_exchange_strong(expect, false));
                full->store(false);
                if (count++ == TOTAL_COUNTER) break;
            }
        }, active + i, full + i, i));
    }
    // Consumer
    for (int i = 0; i < THREAD_NUMBER; i++) {
        arraytrd.push_back(thread([](atomic<bool> *active, atomic<bool> *full, int tid) {
            cout << tid << endl;
            unsigned long long count = 0;
            while (true) {
                this_thread::sleep_for(chrono::nanoseconds(10000000));
                // if (count == TOTAL_COUNTER) break;
                if (full->load() /*&& count % 10 == 0*/) {
                    active->store(true);
                }
                if (tid % THREAD_NUMBER == 0)
                    cout << tid << ":" << count << ":" << full->load() << ":" << active->load() << endl;
                count++;
                if (term.load()) break;
            }
        }, active + i, full + i, THREAD_NUMBER + i));
    }
    int tc = 0;
    for (auto &t: arraytrd) {
        t.join();
        cout << "<" << tc << endl;
        if (tc++ == THREAD_NUMBER - 1) term.store(true);
    }
    cout << mark << endl;
}

void csemTest() {
    vector<thread> arraytrd;
    sem_t active[THREAD_NUMBER];
    atomic<bool> full[THREAD_NUMBER];
    // Producer
    for (int i = 0; i < THREAD_NUMBER; i++) {
        sem_init(active + i, 0, 1);
        full[i].store(false);
        arraytrd.push_back(thread([](sem_t *active, atomic<bool> *full, int tid) {
            cout << tid << endl;
            unsigned long long count = 0;
            while (true) {
                full->store(true);
                bool expect;
                sem_wait(active);
                full->store(false);
                if (count++ == TOTAL_COUNTER) break;
            }
        }, active + i, full + i, i));
    }
    // Consumer
    for (int i = 0; i < THREAD_NUMBER; i++) {
        arraytrd.push_back(thread([](sem_t *active, atomic<bool> *full, int tid) {
            cout << tid << endl;
            unsigned long long count = 0;
            while (true) {
                this_thread::sleep_for(chrono::nanoseconds(10000000));
                // if (count == TOTAL_COUNTER) break;
                if (full->load() /*&& count % 10 == 0*/) {
                    sem_post(active);
                }
                if (tid % THREAD_NUMBER == 0)
                    cout << tid << ":" << count << ":" << full->load() << endl;
                count++;
                if (term.load()) break;
            }
        }, active + i, full + i, THREAD_NUMBER + i));
    }
    int tc = 0;
    for (auto &t: arraytrd) {
        t.join();
        cout << "<" << tc << endl;
        if (tc++ == THREAD_NUMBER - 1) term.store(true);
    }
    cout << mark << endl;
}

void psemTest() {
    vector<thread> arraytrd;
    Semaphore active[THREAD_NUMBER];
    atomic<bool> full[THREAD_NUMBER];
    // Producer
    for (int i = 0; i < THREAD_NUMBER; i++) {
        full[i].store(false);
        arraytrd.push_back(thread([](Semaphore *active, atomic<bool> *full, int tid) {
            cout << tid << endl;
            unsigned long long count = 0;
            while (true) {
                full->store(true);
                bool expect;
                active->wait();
                full->store(false);
                if (count++ == TOTAL_COUNTER) break;
            }
        }, active + i, full + i, i));
    }
    // Consumer
    for (int i = 0; i < THREAD_NUMBER; i++) {
        arraytrd.push_back(thread([](Semaphore *active, atomic<bool> *full, int tid) {
            cout << tid << endl;
            unsigned long long count = 0;
            while (true) {
                this_thread::sleep_for(chrono::nanoseconds(10000000));
                // if (count == TOTAL_COUNTER) break;
                if (full->load() /*&& count % 10 == 0*/) {
                    active->notify();
                }
                if (tid % THREAD_NUMBER == 0)
                    cout << tid << ":" << count << ":" << full->load() << endl;
                count++;
                if (term.load()) break;
            }
        }, active + i, full + i, THREAD_NUMBER + i));
    }
    int tc = 0;
    for (auto &t: arraytrd) {
        t.join();
        cout << "<" << tc << endl;
        if (tc++ == THREAD_NUMBER - 1) term.store(true);
    }
    cout << mark << endl;
}

int main() {
    // naiveTest();
    // csemTest();
    psemTest();
    return 0;
}
