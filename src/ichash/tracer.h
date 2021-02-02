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
