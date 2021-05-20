/*

Copyright 2017 University of Rochester

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 

*/



#ifndef HAZARD_TRACKER_HPP
#define HAZARD_TRACKER_HPP

#ifndef _REENTRANT
#define _REENTRANT
#endif

#include <queue>
#include <list>
#include <vector>
#include <atomic>
#include "ConcurrentPrimitives.hpp"
#include "arraylist.h"
//#include "RAllocator.hpp"

#include "BaseTracker.hpp"


template<class T>
class HazardTracker: public BaseTracker<T>{
private:
	int task_num;
	int slotsPerThread;
	int freq;
	bool collect;

	//RAllocator* mem;
    AtomicArrayList<T> **announce;
    PAD;

	paddedAtomic<T*>* slots;
	padded<int>* cntrs;
	padded<std::list<T*>>* retired; // TODO: use different structure to prevent malloc locking....

	void empty(int tid){
		std::list<T*>* myTrash = &(retired[tid].ui);
		for (typename std::list<T*>::iterator iterator = myTrash->begin(), end = myTrash->end(); iterator != end; ) {
			auto ptr = *iterator;
			bool danger = false;
			if (!danger){
            for (int otherTid = 0; otherTid < this->task_num; ++otherTid) {
                int sz = announce[otherTid]->size();
                for (int ixHP = 0; ixHP < sz; ++ixHP) {
                    if(ptr == announce[otherTid]->get(ixHP)){
                        danger = true;
                        break;
                    }
                }

            }

			}
			if(!danger){
				this->reclaim(ptr);
				this->dec_retired(tid);
				iterator = myTrash->erase(iterator);
			}
			else{++iterator;}
		}
		return;
	}

public:
	~HazardTracker(){};
	HazardTracker(int task_num, int slotsPerThread = 0, int emptyFreq = 100, bool collect=true):BaseTracker<T>(task_num){

        announce = new AtomicArrayList<T> *[task_num];

	    this->task_num = task_num;
		this->slotsPerThread = slotsPerThread;
		this->freq = emptyFreq;
		slots = new paddedAtomic<T*>[task_num*slotsPerThread];
		for (int i = 0; i<task_num*slotsPerThread; i++){
			slots[i]=NULL;
		}
		retired = new padded<std::list<T*>>[task_num];
		cntrs = new padded<int>[task_num];
		for (int i = 0; i<task_num; i++){
            announce[i] = new AtomicArrayList<T>(MAX_HAZARDPTRS_PER_THREAD);
			cntrs[i]=0;
			retired[i].ui = std::list<T*>();
		}
		this->collect = collect;
	}
	HazardTracker(int task_num, int slotsPerThread, int emptyFreq):
		HazardTracker(task_num, slotsPerThread, emptyFreq, true){}

    void initThread(int tid = 0){};
    bool startOp(int tid){};
    void endOp(int tid){};

	uint64_t read(int tid,std::atomic<uint64_t>& atomic_entry){
        uint64_t entry = 0, entry_check = 0;
        do {
            entry = atomic_entry.load(std::memory_order_relaxed);
            if (entry == 0) break;
            announce[tid]->add((T*)libcuckoo::con_hp_store(entry));
            entry_check= atomic_entry.load(std::memory_order_relaxed);
        } while (entry != entry_check ) ;
        return entry;
	}

    void clear(int tid){
        announce[tid]->clear();
	};

	void reserve_slot(T* ptr, int slot, int tid){
		slots[tid*slotsPerThread+slot] = ptr;
	}
	void reserve_slot(T* ptr, int slot, int tid, T* node){
		slots[tid*slotsPerThread+slot] = ptr;
	}
	void clearSlot(int slot, int tid){
		slots[tid*slotsPerThread+slot] = NULL;
	}
	void clearAll(int tid){
		for(int i = 0; i<slotsPerThread; i++){
			slots[tid*slotsPerThread+i] = NULL;
		}
	}
	void clear_all(int tid){
		clearAll(tid);
	}

    inline storeType * allocate(int tid, uint64_t len){
        return static_cast<storeType *>(malloc(len));
    };

    bool deallocate(int tid, storeType * ptr){
		if(ptr== nullptr){return false;}
		std::list<T*>* myTrash = &(retired[tid].ui);

		myTrash->push_back(ptr);	
		if(collect && cntrs[tid]==freq){
			cntrs[tid]=0;
			empty(tid);
		}
		cntrs[tid].ui++;
	}

	bool collecting(){return collect;}
	
};


#endif
