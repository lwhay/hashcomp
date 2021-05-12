#ifndef CZL_CUCKOO_ENTRY_DEF_H
#define CZL_CUCKOO_ENTRY_DEF_H

namespace libcuckoo {

using partial_t = uint16_t;
using thread_mark_t = uint16_t;
using KickInfo = uint64_t;

static const uint64_t empty_entry = 0ll;

static const int partial_offset = 48;
static const uint64_t partial_mask = 0xffffull << partial_offset;
static const int thread_mark_offset = 48;
static const uint64_t thread_mark_mask = 0xffffull << thread_mark_offset;

static const uint64_t kick_mark_mask = 1ull;
static const uint64_t ptr_mask = 0xfffffffffffeull; //lower 47bit

static const uint64_t kickinfo_bucket_mask = 0xffffffffffull << 24;
static const uint64_t kickinfo_slot_mask = 0xff << 16;
static const uint64_t kickinfo_partial_mask = 0xffff;

inline bool is_kick_marked(uint64_t entry) {
    return entry & kick_mark_mask;
}

inline static Item *extract_ptr(uint64_t entry) {
    return (Item *) (entry & ptr_mask & ~kick_mark_mask);
}

inline static partial_t extract_partial(uint64_t entry) {
    debug ASSERT(!is_kick_marked(entry), "try extract partial from an entry marked")
    return static_cast<partial_t>((entry & partial_mask) >> partial_offset);
}

inline static thread_mark_t extract_kick_thread_mark(uint64_t entry) {
    debug ASSERT(is_kick_marked(entry), "try extract kick thread mark from an entry not marked")
    return static_cast<partial_t>((entry & thread_mark_mask) >> thread_mark_offset);
}

inline uint64_t merge_entry(partial_t partial, Item *ptr) {
    return (((uint64_t) partial << partial_offset) | (uint64_t) ptr | 0ul);
}

inline uint64_t mark_entry(uint64_t entry, int tid) {
    debug ASSERT(entry != empty_entry, "should not mark empty entry")
    debug ASSERT(!is_kick_marked(entry), "try mark an marked antry")
    return ((((uint64_t) tid) << thread_mark_offset)
            | (uint64_t) extract_ptr(entry)
            | 1ul
    );
}

inline KickInfo construct_kickinfo(size_type bucket_ind, size_type slot, partial_t partial) {
    return (bucket_ind << 24) | (slot << 16) | partial;
}

inline size_type kickinfo_extract_bucket(KickInfo kickInfo) {
    return (kickInfo & kickinfo_bucket_mask) >> 24;
}

inline size_type kickinfo_extract_slot(KickInfo kickInfo) {
    return (kickInfo & kickinfo_slot_mask) >> 16;
}

inline partial_t kickinfo_extract_partial(KickInfo kickInfo) {
    return (kickInfo & kickinfo_partial_mask);
}


}

#endif //CZL_CUCKOO_ENTRY_DEF_H

