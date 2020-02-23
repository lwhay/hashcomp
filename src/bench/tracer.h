#pragma once

#include <atomic>
#include <iostream>
#include <random>
#include <chrono>
#include <fstream>
#include <thread>
#include <pthread.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include "../../stream/src/quantile/generator.h"

#define EPS 0.0001f

#define DEFAULT_STORE_BASE 1000000000

#define TEST_LOOKUP        1

#define WITH_STRING        2   // 0: unstable char*, 1: naive string, 2: fixed-length string

#define UNIT_SIZE          (8)

#define INPUT_METHOD 2 //0: duplicated; 1: stepping; 2: segmented

#define FUZZY_BOUND 0

using namespace std;

#define WITH_NUMA   0

#if WITH_NUMA != 0
unsigned num_cpus = std::thread::hardware_concurrency();
cpu_set_t default_cpuset;
atomic<bool> setcpus{false};

void fixedThread(size_t tid, pthread_t &thread) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(tid % num_cpus, &cpuset);
    int rc = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
}

void maskThread(size_t tid, pthread_t &thread) {
    if (tid == 0) {
        CPU_ZERO(&default_cpuset);
        for (size_t t = 1; t < num_cpus; t++)
            CPU_SET(t, &default_cpuset);
        setcpus.store(true);
    } else {
        while (!setcpus.load()) std::this_thread::yield();
    }
    int rc = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &default_cpuset);
    if (rc != 0) {
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
}
#endif

class Tracer {
    timeval begTime;

    timeval endTime;

    long duration;

public:
    void startTime() {
        gettimeofday(&begTime, nullptr);
    }

    long getRunTime() {
        gettimeofday(&endTime, nullptr);
        //cout << endTime.tv_sec << "<->" << endTime.tv_usec << "\t";
        duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
        begTime = endTime;
        return duration;
    }

    long fetchTime() {
        gettimeofday(&endTime, nullptr);
        duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
        return duration;
    }
};

const double default_timer_range = 30;

class Timer {
public:
    void start() {
        m_StartTime = std::chrono::system_clock::now();
        m_bRunning = true;
    }

    void stop() {
        m_EndTime = std::chrono::system_clock::now();
        m_bRunning = false;
    }

    double elapsedMilliseconds() {
        std::chrono::time_point<std::chrono::system_clock> endTime;

        if (m_bRunning) {
            endTime = std::chrono::system_clock::now();
        } else {
            endTime = m_EndTime;
        }

        return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - m_StartTime).count();
    }

    double elapsedSeconds() {
        return elapsedMilliseconds() / 1000.0;
    }

private:
    std::chrono::time_point<std::chrono::system_clock> m_StartTime;
    std::chrono::time_point<std::chrono::system_clock> m_EndTime;
    bool m_bRunning = false;
};

const char *existingFilePath = "./testfile.dat";

namespace ycsb {
char *loadpath = "./load.dat";

char *runpath = "./run.dat";

char *YCSB_command[5] = {"READ", "INSERT", "DELETE", "UPDATE", "SCAN"};

enum YCSB_operator {
    lookup = 0,
    insert,
    erease,
    update,
    scan
};

class YCSB_request {
private:
    YCSB_operator op;
    char *key;
    size_t ks;
    char *val;
    size_t vs;
public:
    YCSB_request(YCSB_operator _op, char *_key, size_t _ks, char *_val = nullptr, size_t _vs = 0) : op(_op), key(_key),
                                                                                                    ks(_ks), val(_val),
                                                                                                    vs(_vs) {}

    ~YCSB_request() {
        delete key;
        delete val;
    }

    YCSB_operator getOp() { return op; }

    char *getKey() { return key; }

    size_t keyLength() { return ks; }

    char *getVal() { return val; }

    size_t valLength() { return vs; }
};

class YCSBLoader {
private:
    char *inputpath;
    size_t numberOfRequests;
    size_t limitOfRequests;

    void supersplit(const std::string &s, std::vector<std::string> &v, const std::string &c,
                    size_t n = std::numeric_limits<size_t>::max()) {
        std::string::size_type pos1, pos2;
        size_t len = s.length();
        pos2 = s.find(c);
        pos1 = 0;
        size_t found = 0;
        while (std::string::npos != pos2) {
            if (found++ == n) {
                v.emplace_back(s.substr(pos1));
                break;
            }
            v.emplace_back(s.substr(pos1, pos2 - pos1));
            pos1 = pos2 + c.size();
            pos2 = s.find(c, pos1);
        }
        if (pos1 != len)
            v.emplace_back(s.substr(pos1));
    }

public:
    YCSBLoader(char *path, size_t number = std::numeric_limits<size_t>::max()) : inputpath(path),
                                                                                 numberOfRequests(0),
                                                                                 limitOfRequests(number) {}

    std::vector<YCSB_request *> load() {
        std::vector<YCSB_request *> requests;
        std::fstream lf(inputpath, ios::in);
        string line;
        while (std::getline(lf, line)) {
            std::vector<std::string> fields;
            supersplit(line, fields, " ", 3);
            if (fields.size() < 2) continue;
            for (int i = 0; i < 5; i++) {
                if (fields[0].compare(YCSB_command[i]) == 0) {
                    if (i % 2 == 0) {
                        requests.push_back(
                                new YCSB_request(static_cast<YCSB_operator>(i), strdup(fields[2].substr(4).c_str()),
                                                 fields[2].length()));
                    } else {
                        requests.push_back(
                                new YCSB_request(static_cast<YCSB_operator>(i), strdup(fields[2].substr(4).c_str()),
                                                 fields[2].length(), strdup(fields[3].c_str()), fields[3].length()));
                    }
                    if (++numberOfRequests == limitOfRequests) goto complete;
                }
            }
        }
        complete:
        lf.close();
        return requests;
    }

    size_t size() { return numberOfRequests; }
};
}

template<typename R>
class RandomGenerator {
public:
    static inline void generate(R *array, size_t range, size_t count, double skew = 0.0) {
        struct stat buffer;
        if (stat(existingFilePath, &buffer) == 0) {
            cout << "read generation" << endl;
            FILE *fp = fopen(existingFilePath, "rb+");
            fread(array, sizeof(R), count, fp);
            fclose(fp);
        } else {
            if (skew < zipf_distribution<R>::epsilon) {
                std::default_random_engine engine(
                        static_cast<R>(chrono::steady_clock::now().time_since_epoch().count()));
                std::uniform_int_distribution<size_t> dis(0, range + FUZZY_BOUND);
                for (size_t i = 0; i < count; i++) {
                    array[i] = static_cast<R>(dis(engine));
                }
            } else {
                zipf_distribution<R> engine(range, skew);
                mt19937 mt;
                for (size_t i = 0; i < count; i++) {
                    array[i] = engine(mt);
                }
            }
            FILE *fp = fopen(existingFilePath, "wb+");
            fwrite(array, sizeof(R), count, fp);
            fclose(fp);
            cout << "write generation" << endl;
        }
    }

    static inline void generate(R *array, size_t len, size_t range, size_t count, double skew = 0.0) {
        uint64_t *tmp = (uint64_t *) malloc(sizeof(uint64_t) * count);
        RandomGenerator<uint64_t>::generate(tmp, range, count, skew);
        char buf[256];
        for (int i = 0; i < count; i++) {
            memset(buf, '0', 256);
            std::sprintf(buf, "%llu", tmp[i]);
            memcpy(&array[i * len], buf, len);
            //printf("%llu\t%s\t", tmp[i], buf);
            //memcpy(buf, &array[i * len], len);
            //printf("%s\n", buf);
        }
        free(tmp);
    }
};