#ifndef RESEARCH_ASSERT_MSG_H
#define RESEARCH_ASSERT_MSG_H

#include <assert.h>
#include <iostream>

#define ASSERT(exp_, msg_) if(!(exp_)) {std::cerr<<"ASSERT FALSE : "<<msg_<<std::endl;assert(false);}

#ifndef DEBUG
#define debug if(0)
#else
#define debug ;
#endif

#ifndef TRACE
#define TRACE 1
#endif

#ifndef RECORD
#define record if(0)
#else
#define record ;
#endif

#define NAME(var) #var

struct Log {
    size_t find_success, find_failure;
    size_t set_insert, set_update;
    size_t delete_success, delete_failure;

    size_t kick_triggered; //times which run_cuckoo function called
    size_t kick_performed; //times which find-move operation performed
    size_t helper_triggered; //times helper triggered

    //move failure
    size_t kick_failure_depth0;
    size_t kick_failure_other_marked;
    size_t kick_failure_source_changed;
    size_t kick_failure_mark_cas_fail;
    size_t kick_failure_set_target_cas_fail;
    size_t kick_failure_rm_source_cas_fail;

    size_t kick_path_length_log[6];

    //redo
    size_t kick_send_redo_signal;
    size_t redo_times;

    //duplicate key


    void show() {
        cout << NAME(find_success) << " " << find_success << " " << NAME(find_failure) << " " << find_failure << endl;
        cout << NAME(set_insert) << " " << set_insert << " " << NAME(set_update) << " " << set_update << endl;
        cout << NAME(delete_success) << " " << delete_success << " " << NAME(delete_failure) << " " << delete_failure
             << endl;
        cout << endl;

        cout << NAME(kick_triggered) << " " << kick_triggered << " " << NAME(kick_performed) << " " << kick_performed
             << endl;
        cout << NAME(helper_triggered) << " " << helper_triggered << endl;

        cout << "path length log:  ";
        for (int i = 0; i < 6; i++) {
            cout << " " << i << ":" << kick_path_length_log[i] << " ";
        }
        cout << endl;

        cout << NAME(kick_failure_depth0) << " " << kick_failure_depth0 << endl;
        cout << NAME(kick_failure_other_marked) << " " << kick_failure_other_marked << endl;
        cout << NAME(kick_failure_source_changed) << " " << kick_failure_source_changed << endl;
        cout << NAME(kick_failure_mark_cas_fail) << " " << kick_failure_mark_cas_fail << endl;
        cout << NAME(kick_failure_set_target_cas_fail) << " " << kick_failure_set_target_cas_fail << endl;
        cout << NAME(kick_failure_rm_source_cas_fail) << " " << kick_failure_rm_source_cas_fail << endl;
        cout << endl;

        cout << NAME(kick_send_redo_signal) << " " << kick_send_redo_signal << endl;
        cout << NAME(redo_times) << " " << redo_times << endl;

    }

    Log operator+=(Log &other) {
        this->find_success += other.find_success;
        this->find_failure += other.find_failure;
        this->set_insert += other.set_insert;
        this->set_update += other.set_update;
        this->delete_success += other.delete_success;
        this->delete_failure += other.delete_failure;

        this->kick_triggered += other.kick_triggered;
        this->kick_performed += other.kick_performed;
        this->helper_triggered += other.helper_triggered;


        this->kick_failure_depth0 += other.kick_failure_depth0;
        this->kick_failure_other_marked += other.kick_failure_other_marked;
        this->kick_failure_source_changed += other.kick_failure_source_changed;
        this->kick_failure_mark_cas_fail += other.kick_failure_mark_cas_fail;
        this->kick_failure_set_target_cas_fail += other.kick_failure_set_target_cas_fail;
        this->kick_failure_rm_source_cas_fail += other.kick_failure_rm_source_cas_fail;

        for (int i = 0; i < 6; i++) {
            this->kick_path_length_log[i] += other.kick_path_length_log[i];
        }

        this->kick_send_redo_signal += kick_send_redo_signal;
        this->redo_times += redo_times;
    }
};

thread_local Log llog;

#endif //RESEARCH_ASSERT_MSG_H
