/* kvdummy.cpp
* 09/16/2019
* by Mian Qin
*/
#include "kvdummy.h"

#define MAX_VAL_SIZE 4096

namespace kvdummy {

bool KVDummy::kvr_insert(kvr_key *key, kvr_value *value){
    // Hash to get start dev index
    int dev_idx = get_dev_idx(key->key, key->length);

    std::string skey(key->key, key->length);
    std::string sval(value->val, value->length);
    
    ssds_[dev_idx].kv_store(&skey, &sval);

    return true;
}


bool KVDummy::kvr_update(kvr_key *key, kvr_value *value) {
    // Hash to get start dev index
    int dev_idx = get_dev_idx(key->key, key->length);

    std::string skey(key->key, key->length);
    // Evict from cache
    kvr_erase_cache(skey);
    
    std::string sval(value->val, value->length);
    
    ssds_[dev_idx].kv_store(&skey, &sval);

    return true;
}

bool KVDummy::kvr_delete(kvr_key *key) {

    return true;

}

bool KVDummy::kvr_get(kvr_key *key, kvr_value *value) {
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

bool KVDummy::kvr_erased_get(int erased, kvr_key *key, kvr_value *value) {
    

    return true;
}

bool KVDummy::kvr_write_batch(WriteBatch *batch) {
    printf("NOT IMPLEMENT\n");
}

} // end namespace kvdummy

KVR* NewKVDummy(int num_d, KVS_CONT *conts) {
    return new kvdummy::KVDummy(num_d, conts);
}