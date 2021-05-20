#ifndef MY_RECLAIMER_ALLOCATOR_NEW_H
#define MY_RECLAIMER_ALLOCATOR_NEW_H


/*A simple allocator using malloc/free
 * exit when deallocating a nullptr */
class AllocatorNew {
public:
    AllocatorNew() {};

    void *allocate(uint64_t len) {
        void *tp = malloc(len);
        return tp;
    };

    void deallocate(void *ptr) {
        if (ptr != nullptr) {
            free(ptr);
        } else {
            cerr << "try free a nullptr" << endl;
            exit(-1);
        }
        return;
    };
};

#endif //MY_RECLAIMER_ALLOCATOR_NEW_H
