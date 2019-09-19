/* kvdummy.h
* 09/16/2019
* by Mian Qin
*/

#ifndef   _kvdummy_h_   
#define   _kvdummy_h_   

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

namespace kvdummy {

class KVDummy : public KVR {
private:
    int k_; // number of data nodes

    // device
    KV_DEVICE *ssds_;


    int get_dev_idx (char *key, int len) {
        return MurmurHash2(key, len, 2019)%(k_);
    }

public:
    KVDummy(int num_d, KVS_CONT *conts) : k_(num_d) {
        ssds_ = (KV_DEVICE *)malloc(sizeof(KV_DEVICE) * (k_));
        for (int i = 0; i < (k_); i++) {
            (void) new(&ssds_[i]) KV_DEVICE(i, &conts[i], 4, 64);
        }
    };
    ~KVDummy () {
        for (int i = 0; i < (k_); i++) {
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


} // end namespace kvdummy

#endif