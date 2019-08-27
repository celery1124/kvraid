/* kvec.h
* 07/10/2019
* by Mian Qin
*/

#ifndef   _kvmirror_h_   
#define   _kvmirror_h_   

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <new>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>

#include "kvr_api.h"
#include "kvr.h"
#include "kv_device.h"

unsigned int MurmurHash2 ( const void * key, int len, unsigned int seed );

namespace kvmirror {

class KVMirror : public KVR {
private:
    int k_; // number of data nodes
    int r_;  // number of parity 

    // device
    KV_DEVICE *ssds_;

    // data volume info
    std::atomic<int64_t> data_volume_; //estimate data vol, not accurate
    int64_t get_volume() {return data_volume_.load();}

    // dev_info
    int64_t get_capacity() {return ssds_[0].get_capacity()*k_;}
    int64_t get_usage() {return int64_t((double)get_util()*get_capacity());}
    double get_util() {
        double util = 0;
        for(int i = 0; i<k_+r_; i++) util += ssds_[i].get_util();
        return util/(k_+r_);
    }
    float get_waf() {
        double waf = 0;
        for(int i = 0; i<k_+r_; i++) waf += ssds_[i].get_waf();
        return waf/(k_+r_);
    }

    int get_dev_idx (char *key, int len) {
        return MurmurHash2(key, len, 2019)%(k_+r_);
    }

public:
    KVMirror(int num_d, int num_r, KVS_CONT *conts) : k_(num_d), r_(num_r) {
        ssds_ = (KV_DEVICE *)malloc(sizeof(KV_DEVICE) * (k_+r_));
        for (int i = 0; i < (k_+r_); i++) {
            (void) new(&ssds_[i]) KV_DEVICE(i, &conts[i], 4, 256);
        }
    };
    ~KVMirror () {
        for (int i = 0; i < (k_+r_); i++) {
            ssds_[i].~KV_DEVICE();
        }
        free(ssds_);
    }

public:
	bool kvr_insert(kvr_key *key, kvr_value *value);
	bool kvr_update(kvr_key *key, kvr_value *value);
    bool kvr_delete(kvr_key *key);
	bool kvr_get(kvr_key *key, kvr_value *value);
    bool kvr_erased_get(int erased, kvr_key *key, kvr_value *value);

    bool kvr_write_batch(WriteBatch *batch);

    Iterator* NewIterator() {
        printf("NOT IMPLEMENT\n");
        return NULL;
    }
};


} // end namespace kvmirror

#endif