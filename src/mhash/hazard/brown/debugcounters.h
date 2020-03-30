/**
 * C++ implementation of lock-free chromatic tree using LLX/SCX and DEBRA(+).
 * This file implements per-thread counters used for debugging.
 * 
 * Copyright (C) 2016 Trevor Brown
 * Contact (tabrown [at] cs [dot] toronto [dot edu]) with any questions or comments.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef DEBUGCOUNTERS_H
#define DEBUGCOUNTERS_H

#include <string>
#include <sstream>
#include "globals.h"

using namespace std;

class debugCounter {
private:
    const int NUM_PROCESSES;
    volatile long long *data; // data[tid*PREFETCH_SIZE_WORDS] = count for thread tid (padded to avoid false sharing)
public:
    void add(const int tid, const long long val) {
        data[tid * PREFETCH_SIZE_WORDS] += val;
    }

    void inc(const int tid) {
        add(tid, 1);
    }

    long long get(const int tid) {
        return data[tid * PREFETCH_SIZE_WORDS];
    }

    long long getTotal() {
        long result = 0;
        for (int tid = 0; tid < NUM_PROCESSES; ++tid) {
            result += get(tid);
        }
        return result;
    }

    void clear() {
        for (int tid = 0; tid < NUM_PROCESSES; ++tid) {
            data[tid * PREFETCH_SIZE_WORDS] = 0;
        }
    }

    debugCounter(const int numProcesses) : NUM_PROCESSES(numProcesses) {
        data = new long long[numProcesses * PREFETCH_SIZE_WORDS];
        clear();
    }

    ~debugCounter() {
        delete[] data;
    }
};

class debugCounters {
public:
    const int NUM_PROCESSES;
    debugCounter *const llxSuccess;
    debugCounter *const llxFail;
    debugCounter *const insertSuccess;
    debugCounter *const insertFail;
    debugCounter *const eraseSuccess;
    debugCounter *const eraseFail;
    debugCounter *const findSuccess;
    debugCounter *const findFail;
    debugCounter *const rqSuccess;
    debugCounter *const rqFail;
    debugCounter *garbage;

    void clear() {
        llxSuccess->clear();
        llxFail->clear();
        insertSuccess->clear();
        insertFail->clear();
        eraseSuccess->clear();
        eraseFail->clear();
        findSuccess->clear();
        findFail->clear();
        rqSuccess->clear();
        rqFail->clear();
        garbage->clear();
    }

    debugCounters(const int numProcesses) :
            NUM_PROCESSES(numProcesses),
            llxSuccess(new debugCounter(numProcesses)),
            llxFail(new debugCounter(numProcesses)),
            insertSuccess(new debugCounter(numProcesses)),
            insertFail(new debugCounter(numProcesses)),
            eraseSuccess(new debugCounter(numProcesses)),
            eraseFail(new debugCounter(numProcesses)),
            findSuccess(new debugCounter(numProcesses)),
            findFail(new debugCounter(numProcesses)),
            rqSuccess(new debugCounter(numProcesses)),
            rqFail(new debugCounter(numProcesses)),
            garbage(new debugCounter(numProcesses)) {}

    ~debugCounters() {
        delete llxSuccess;
        delete llxFail;
        delete insertSuccess;
        delete insertFail;
        delete eraseSuccess;
        delete eraseFail;
        delete findSuccess;
        delete findFail;
        delete rqSuccess;
        delete rqFail;
        delete garbage;
    }
};

#endif /* DEBUGCOUNTERS_H */