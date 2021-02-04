#ifndef BUCKET_CONTAINER_H
#define BUCKET_CONTAINER_H

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>
#include "ihazard.h"
//#include "brown_reclaim.h"
#include "cuckoohash_util.hh"

#include "item.h"
#include "brown_reclaim.h"

namespace libcuckoo {

    static const int ATOMIC_ALIGN_RATIO = 1;

    thread_local int cuckoo_thread_id;

#define alloc allocator_new
#define pool pool_perthread_and_shared
typedef brown_reclaim<Item , alloc<Item>, pool<>, reclaimer_ebr_token<>> brown6;

template <std::size_t SLOT_PER_BUCKET>
class bucket_container {
public:


    using size_type =size_t;
public:

  class bucket {
  public:
    bucket() {
        for (int i = 0; i < SLOT_PER_BUCKET * ATOMIC_ALIGN_RATIO; i++) values_[i].store((uint64_t)nullptr);
    }

    //TODO can't define under kick lock is occpupied or not
//    inline bool occupied(size_type ind){return values_[ind].load() != (uint64_t) nullptr;}

  //private:
    friend class bucket_container;

    uint64_t get_item_ptr(size_t i){
        return values_[i * ATOMIC_ALIGN_RATIO].load();
    }

    bool empty(){
        for(size_type i = 0; i < SLOT_PER_BUCKET * ATOMIC_ALIGN_RATIO ; i ++){
            if(values_[i].load() != (uint64_t) nullptr)
                return false;
        }
        return true;
    }

    //TODO align

    std::array<std::atomic<uint64_t>,SLOT_PER_BUCKET * ATOMIC_ALIGN_RATIO> values_;

  };

  bucket_container(size_type hp,int cuckoo_thread_num):hashpower_(hp),ready_to_destory(false){
      buckets_ = new bucket[size()]();
      deallocator = new brown6 (cuckoo_thread_num);
  }

  bucket_container(size_type hp):hashpower_(hp),ready_to_destory(false){
        buckets_ = new bucket[size()]();
    }
  ~bucket_container() noexcept { destroy_buckets(); }

  void destroy_buckets() noexcept {
        bool still_have_item = false;
        for (size_type i = 0; i < size(); ++i) {
            for(size_type j = 0; j < SLOT_PER_BUCKET ;j++){
                if(buckets_[i].values_[j].load() != (uint64_t) nullptr){
                    still_have_item = true;
                    break;
                }
            }
        }
        ASSERT(!(ready_to_destory && still_have_item ) ,"bucket still have item!");
        delete[]buckets_;
  }

    void swap(bucket_container &bc) noexcept {
        size_t bc_hashpower = bc.hashpower();
        bc.hashpower(hashpower());
        hashpower(bc_hashpower);
        std::swap(buckets_, bc.buckets_);
    }

    void swap_first(bucket_container &bc) noexcept {
        size_t bc_hashpower = bc.hashpower();
        bc.hashpower(hashpower());
        hashpower(bc_hashpower);
        this->deallocator = bc.deallocator;
        std::swap(buckets_, bc.buckets_);
    }

  size_type hashpower() const {
    return hashpower_.load();
  }

  void hashpower(size_type val) {
    hashpower_.store(val);
  }

  size_type size() const { return size_type(1) << hashpower(); }

  bucket &operator[](size_type i) { return buckets_[i]; }
  const bucket &operator[](size_type i) const { return buckets_[i]; }


  inline size_type read_from_slot( bucket &b,size_type slot){
      return deallocator->load(cuckoo_thread_id,b.values_[slot * ATOMIC_ALIGN_RATIO]);
      //return b.values_[slot * ATOMIC_ALIGN_RATIO].load();
  }

  inline size_type read_from_bucket_slot(size_type ind,size_type slot){
      return deallocator->load(cuckoo_thread_id,std::ref( buckets_[ind].values_[slot * ATOMIC_ALIGN_RATIO]));
      //return buckets_[ind].values_[slot * ATOMIC_ALIGN_RATIO].load();
  }

  inline atomic<size_t> & get_atomic_par_ptr(size_type ind,size_type slot){
      return buckets_[ind].values_[slot * ATOMIC_ALIGN_RATIO];
  }


  // ptr has been packaged with partial
  bool try_insertKV(size_type ind, size_type slot, uint64_t insert_ptr) {
        bucket &b = buckets_[ind];
        uint64_t old = b.values_[slot * ATOMIC_ALIGN_RATIO].load();
        if(old != (uint64_t)nullptr) return false;
        return b.values_[slot * ATOMIC_ALIGN_RATIO].compare_exchange_strong(old,insert_ptr);
  }

  bool try_updateKV(size_type ind, size_type slot,uint64_t old_ptr,uint64_t update_ptr) {
        bucket &b = buckets_[ind];
        uint64_t old = b.values_[slot * ATOMIC_ALIGN_RATIO].load();
        if(old != old_ptr) return false;
        if(b.values_[slot * ATOMIC_ALIGN_RATIO].compare_exchange_strong(old,update_ptr)){
            deallocator->free(old);
            return true;
        }else{
            return false;
        }
  }

  bool try_eraseKV(size_type ind, size_type slot,uint64_t erase_ptr) {
        bucket &b = buckets_[ind];
        uint64_t old = b.values_[slot * ATOMIC_ALIGN_RATIO].load();
        if(old != erase_ptr) return false;
        if(b.values_[slot * ATOMIC_ALIGN_RATIO].compare_exchange_strong(old,(uint64_t) nullptr)){
            deallocator->free(old);
            return true;
        }else{
            return false;
        }

  }

    Item * allocate_item(char * key,size_t key_len,char * value,size_t value_len){
        //Item * p = (Item * )malloc(key_len + value_len + 2 * sizeof(uint32_t));
        Item * p =  (Item *) deallocator->allocate(cuckoo_thread_id);;
        ASSERT(p!= nullptr,"malloc failure");
        p->key_len = key_len;
        p->value_len = value_len;
        memcpy(ITEM_KEY(p),key,key_len);
        memcpy(ITEM_VALUE(p),value,value_len);
        return p;
    }



  //only for kick and rehash
  //par_ptr holding kick lock
  void set_ptr(size_type ind, size_type slot,uint64_t par_ptr){
      bucket &b = buckets_[ind];
      b.values_[slot * ATOMIC_ALIGN_RATIO].store(par_ptr);
  }

  uint64_t get_item_num(){
      table_mtx.lock();
      uint64_t count = 0;
      for(size_t i = 0; i < size(); i++ ){
           bucket &b = buckets_[i];
           for(int j =0; j< SLOT_PER_BUCKET;j++){
               if(b.values_[j * ATOMIC_ALIGN_RATIO].load() != (uint64_t) nullptr) {
                   count++;
               }
           }
      }
      total_count = count;
      table_mtx.unlock();
      return count;
  }

  void get_key_position_info(vector<double> & kpv){
      table_mtx.lock();
      ASSERT(kpv.size() == SLOT_PER_BUCKET, "key_position_info length error");
      vector<uint64_t> count_vtr(4);
      for(size_type i = 0 ; i < size() ; i++){
          bucket &b = buckets_[i];
          for(int j =0; j< SLOT_PER_BUCKET;j++){
              if(b.values_[j * ATOMIC_ALIGN_RATIO].load() != (uint64_t) nullptr) {
                  count_vtr[j]++;
              }
          }
      }

      for(size_type i = 0; i < SLOT_PER_BUCKET; i++){
          kpv[i] = count_vtr[i] * 1.0 / total_count;
      }
      table_mtx.unlock();
  }



  void set_ready_to_destory(){
      ready_to_destory = true;
  }

    ihazard<Item> *deallocator;
private:
    bool ready_to_destory;

  std::atomic<size_type> hashpower_;

  bucket *  buckets_;

  std::mutex table_mtx;

  uint64_t total_count;

  //ihazard<Item> *deallocator;

};

}  // namespace libcuckoo

#endif // BUCKET_CONTAINER_H
