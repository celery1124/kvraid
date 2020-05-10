/* kvmirror.cpp
* 07/10/2019
* by Mian Qin
*/
#include "kvmirror.h"

#define MAX_VAL_SIZE 8192

namespace kvmirror {

class Monitor {
public:
    std::mutex mtx_;
    std::condition_variable cv_;
    bool ready_ ;
    Monitor() : ready_(false) {}
    ~Monitor(){}
    void reset() {ready_ = false;};
    void notify() {
        std::unique_lock<std::mutex> lck(mtx_);
        ready_ = true;
        cv_.notify_one();
    }
    void wait() {
        std::unique_lock<std::mutex> lck(mtx_);
        while (!ready_) cv_.wait(lck);
    }
};

static void on_io_complete(void *args) {
    Monitor *mon = (Monitor *)args;
    mon->notify();
}

bool KVMirror::kvr_insert(kvr_key *key, kvr_value *value){
    // Hash to get start dev index
    int dev_idx = get_dev_idx(key->key, key->length);

    std::string skey(key->key, key->length);
    phy_val pval(value->val, value->length);
    Monitor *mons = new Monitor[r_+1];
    for (int i = 0; i < r_+1; i++) {
        ssds_[dev_idx].kv_astore(&skey, &pval, on_io_complete, (void *)&mons[i]);
        dev_idx = (++dev_idx)%(k_+r_);
    }

    for (int i = 0; i < r_+1; i++) {
        mons[i].wait();
    }

    // cleanup
    delete [] mons;

    return true;
}


bool KVMirror::kvr_update(kvr_key *key, kvr_value *value) {
    // Hash to get start dev index
    int dev_idx = get_dev_idx(key->key, key->length);

    std::string skey(key->key, key->length);
    // Evict from cache
    kvr_erase_cache(skey);

    phy_val pval(value->val, value->length);
    Monitor *mons = new Monitor[r_+1];
    for (int i = 0; i < r_+1; i++) {
        ssds_[dev_idx].kv_astore(&skey, &pval, on_io_complete, (void *)&mons[i]);
        dev_idx = (++dev_idx)%(k_+r_);
    }

    for (int i = 0; i < r_+1; i++) {
        mons[i].wait();
    }

    // cleanup
    delete [] mons;

    return true;
}

bool KVMirror::kvr_delete(kvr_key *key) {

    // Hash to get start dev index
    int dev_idx = get_dev_idx(key->key, key->length);

    std::string skey(key->key, key->length);
    Monitor *mons = new Monitor[r_+1];
    for (int i = 0; i < r_+1; i++) {
        ssds_[dev_idx].kv_adelete(&skey, on_io_complete, (void *)&mons[i]);
        dev_idx = (++dev_idx)%(k_+r_);
    }

    for (int i = 0; i < r_+1; i++) {
        mons[i].wait();
    }

    // cleanup
    delete [] mons;

    return true;

}

bool KVMirror::kvr_get(kvr_key *key, kvr_value *value) {
    // Hash to get start dev index
    int dev_idx = get_dev_idx(key->key, key->length);

    std::string skey(key->key, key->length);
    // First, search for cache
    Cache::Handle *h = kvr_read_cache(skey, value);

    if (h != NULL) { // hit in cache
        kvr_release_cache(h);
        return true;
    }

    value->val = (char*)malloc(MAX_VAL_SIZE);
    phy_val pval(value->val, MAX_VAL_SIZE);

    ssds_[dev_idx].kv_get(&skey, &pval);
    value->length = pval.actual_len;

    // insert to cache
    h = kvr_insert_cache(skey, value);
    kvr_release_cache(h);

    return true;
}

bool KVMirror::kvr_erased_get(int erased, kvr_key *key, kvr_value *value) {
    // Hash to get start dev index
    int dev_idx = get_dev_idx(key->key, key->length);
    // If dev erased, get from next dev
    if (dev_idx == erased) dev_idx = (dev_idx+1)%(k_+r_);

    std::string skey(key->key, key->length);
    value->val = (char*)malloc(MAX_VAL_SIZE);
    phy_val pval(value->val, MAX_VAL_SIZE);

    ssds_[dev_idx].kv_get(&skey, &pval);
    value->length = pval.actual_len;

    return true;
}

bool KVMirror::kvr_write_batch(WriteBatch *batch) {
    printf("NOT IMPLEMENT\n");
}

} // end namespace kvmirror

KVR* NewKVMirror(int num_d, int num_r, KVS_CONT *conts, Cache *c) {
    return new kvmirror::KVMirror(num_d, num_r, conts, c);
}