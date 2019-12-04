//
// Created by iclab on 12/3/19.
//

#include <chrono>
#include <iostream>
#include <thread>
#include "tracer.h"

using namespace std;

size_t step = (1llu << 20);
size_t total = (1llu << 30);
size_t degree = (1llu << 2);
size_t iteration = (1llu << 4);

double *workloads;

void generate() {
    workloads = new double[total];
    std::default_random_engine engine(static_cast<double>(chrono::steady_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<size_t> dis(0, std::numeric_limits<uint32_t>::max());
    Tracer tracer;
    tracer.startTime();
    for (size_t i = 0; i < total; i++) {
        workloads[i] = static_cast<double>(dis(engine)) / std::numeric_limits<uint32_t>::max();
    }
    cout << tracer.getRunTime() << endl;
}

void pcompute() {
    std::thread threads[degree];
    double avg = 0;
    double sum[degree];
    long times[degree];
    long time;
    Tracer tracer;
    tracer.getRunTime();
    for (size_t t = 0; t < degree; t++) {
        sum[t] = 0;
        threads[t] = std::thread([](size_t tid, double &stat, long &time) {
            Tracer tracer;
            tracer.startTime();
            for (size_t r = 0; r < iteration; r++)
                for (size_t i = tid; i < total / degree; i += degree) stat += workloads[((i + tid) * step) % total];
            time = tracer.getRunTime();
        }, t, std::ref(sum[t]), std::ref(times[t]));
    }
    for (size_t t = 0; t < degree; t++) {
        threads[t].join();
        avg += sum[t];
        time += times[t];
    }
    cout << tracer.getRunTime() << endl;
    cout << "\t" << avg / degree << " " << total * degree / time << endl;
}

int main(int argc, char **argv) {
    if (argc > 3) {
        degree = std::atol(argv[1]);
        total = std::atol(argv[2]);
        step = std::atol(argv[3]);
    }
    generate();
    pcompute();
    return 0;
}