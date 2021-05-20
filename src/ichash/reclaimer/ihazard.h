//
// Created by iclab on 11/24/19.
//

#ifndef HASHCOMP_IHAZARD_H
#define HASHCOMP_IHAZARD_H

template<class T, class D = T>
class ihazard {
protected:
    size_t thread_number = 0;
public:
    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter);

    virtual void initThread(int tid = 0) = 0;
    virtual void registerThread() = 0;

    virtual D * allocate(int tid,uint64_t len) = 0;
    virtual bool deallocate(int tid, storeType * ptr) = 0;

    virtual bool startOp(int tid) = 0;
    virtual void endOp(int tid) = 0;

    virtual uint64_t read(int tid,atomic<uint64_t> & a) = 0;
    virtual void clear(int tid) = 0;

    virtual const char *info() = 0;

};

#endif //HASHCOMP_IHAZARD_H
