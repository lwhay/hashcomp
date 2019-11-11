#pragma once

#include <atomic>
#include <iostream>
#include <random>
#include <chrono>
#include <fstream>
#include <pthread.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include "../../stream/src/quantile/generator.h"

#define EPS 0.0001f

#define FUZZY_BOUND 0

using namespace std;

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
                zipf_distribution<R> engine(std::numeric_limits<R>::max(), skew);
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