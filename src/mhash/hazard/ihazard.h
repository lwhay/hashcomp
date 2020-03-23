//
// Created by iclab on 11/24/19.
//

#ifndef HASHCOMP_IHAZARD_H
#define HASHCOMP_IHAZARD_H

class ihazard {
protected:
    size_t thread_number = 0;

public:
    virtual void initThread() = 0;

    virtual uint64_t allocate(size_t tid) = 0;

    virtual void registerThread() = 0;

    virtual uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) = 0;

    virtual void read(size_t tid) = 0;

    virtual bool free(uint64_t ptr) = 0;

    virtual const char *info() = 0;
};

#endif //HASHCOMP_IHAZARD_H
