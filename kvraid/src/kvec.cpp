/* kvec.cpp
* 06/23/2019
* by Mian Qin
*/
#include "kvec.h"

namespace kvec {

static void pack_value(char *dst, kvr_value *val) {
    *((uint32_t*)dst) = val->length;
    memcpy(dst+VAL_SIZE_BYTES, val->val, val->length);
}

static void unpack_value(char *src, kvr_value *val) {
    uint32_t val_len = *((uint32_t*)src);
    
    val->length = val_len;
    memcpy(val->val, src+VAL_SIZE_BYTES, val->length);
}

void SlabQ::get_index_id(uint64_t *index) {
    std::unique_lock<std::mutex> lock(seq_mutex_);
    if (!avail_seq_.empty()) {
        *index = avail_seq_.front();
        avail_seq_.pop();
    }
    else {
        *index = seq_;
        seq_++;
    }

}

void SlabQ::reclaim_index(uint64_t index) {
    std::unique_lock<std::mutex> lock(seq_mutex_);
    avail_seq_.push(index);
}

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

bool SlabQ::slab_insert(kvr_key *key, kvr_value *value) {
    uint64_t group_id, group_offset;
    uint64_t index_id;
    bool new_group ;
    get_index_id(&index_id);
    group_id = index_id/k_;
    group_offset = index_id%k_;
    int dev_idx;

    char *data_new = (char*) calloc(slab_size_, sizeof(char));
    char **codes = (char **)malloc(r_*sizeof(char*));
    for (int i = 0; i < r_; i++) codes[i] = (char*)calloc(slab_size_, sizeof(char));
    memcpy(data_new, value->val, value->length);
    // TODO: when the whole parity group is deleted, waste get_io (new_group==false)

    // write data to device (no need finelock)
    phy_key pkey_d(sid_, group_id*k_+group_offset);
    phy_val pval_d(value->val, value->length);
    Monitor mon_d;
    dev_idx = get_dev_idx(group_id, group_offset);
    parent_->ssds_[dev_idx].kv_astore(&pkey_d, &pval_d, on_io_complete, (void *)&mon_d);

    LockEntry * l = fl_.Lock(group_id);

    {
        std::unique_lock<std::mutex> lock(bm_mutex_);
        new_group = !group_occu_.get(group_id);
        if (new_group) group_occu_.set(group_id);
    }

    if (!new_group) { // need to exist codes
        // allocate pkey, pval
        phy_key *pkeys = (phy_key *)malloc(sizeof(phy_key)*(r_));
        phy_val *pvals = (phy_val *)malloc(sizeof(phy_val)*(r_));
        Monitor *get_mons = new Monitor[r_];
        for (int i = 0; i < r_; i++) {
            (void) new (&pkeys[i]) phy_key(sid_, group_id*k_);
            (void) new (&pvals[i]) phy_val(codes[i], slab_size_);
                
            dev_idx = get_dev_idx(group_id, k_+i);
            parent_->ssds_[dev_idx].kv_aget(&pkeys[i], &pvals[i], on_io_complete, (void *)&get_mons[i]);
        }

        // wait get ios completes
        for (int i = 0; i < r_; i++) {
            get_mons[i].wait();
        }

        // cleanup
        delete [] get_mons;
        free(pkeys);
        free(pvals);
    }

    // update codes
    ec_->update(group_offset, NULL, data_new, codes, slab_size_);

    // write codes to devices

    phy_key *pkeys_c = (phy_key *)malloc(sizeof(phy_key)*(r_));
    phy_val *pvals_c = (phy_val *)malloc(sizeof(phy_val)*(r_));
    Monitor *mons_c = new Monitor[r_];
    for (int i = 0; i < r_; i++) {
        (void) new (&pkeys_c[i]) phy_key(sid_, group_id*k_);
        (void) new (&pvals_c[i]) phy_val(codes[i], slab_size_);
        dev_idx = get_dev_idx(group_id, k_+i);
        parent_->ssds_[dev_idx].kv_astore(&pkeys_c[i], &pvals_c[i], on_io_complete, (void *)&mons_c[i]);
    }

    // wait for write ios
    mon_d.wait();
    for (int i = 0; i < r_; i++) mons_c[i].wait();

    // update mapping table
    std::string skey = std::string(key->key, key->length);
    parent_->key_map_->insert(&skey, &pkey_d);

    fl_.UnLock(group_id, l);

    // cleanup
    delete [] mons_c;
    free(pkeys_c);
    free(pvals_c);
    
    free(data_new);
    for (int i = 0; i < r_; i++) free(codes[i]);
    free(codes);
}

bool SlabQ::slab_update(kvr_value *value, phy_key *pkey) {
    uint64_t index_id = pkey->get_seq();
    uint64_t group_id, group_offset;
    group_id = index_id/k_;
    group_offset = index_id%k_;
    int dev_idx;

    char *data_new = (char*) calloc(slab_size_, sizeof(char));
    char *data_old = (char*) calloc(slab_size_, sizeof(char));
    char **codes = (char **)malloc(r_*sizeof(char*));
    for (int i = 0; i < r_; i++) codes[i] = (char*)calloc(slab_size_, sizeof(char));
    memcpy(data_new, value->val, value->length);

    LockEntry * l = fl_.Lock(group_id);

    // read old data and codes
    phy_key pkey_d(sid_, group_id*k_+group_offset);
    phy_val pval_d(data_old, slab_size_);
    Monitor mon_d;
    dev_idx = get_dev_idx(group_id, group_offset);
    parent_->ssds_[dev_idx].kv_aget(&pkey_d, &pval_d, on_io_complete, (void *)&mon_d);

    phy_key *pkeys_c = (phy_key *)malloc(sizeof(phy_key)*(r_));
    phy_val *pvals_c = (phy_val *)malloc(sizeof(phy_val)*(r_));
    Monitor *mons_c = new Monitor[r_];
    for (int i = 0; i < r_; i++) {
        (void) new (&pkeys_c[i]) phy_key(sid_, group_id*k_);
        (void) new (&pvals_c[i]) phy_val(codes[i], slab_size_);
        dev_idx = get_dev_idx(group_id, k_+i);
        parent_->ssds_[dev_idx].kv_aget(&pkeys_c[i], &pvals_c[i], on_io_complete, (void *)&mons_c[i]);
    }

    // wait get ios completes
    mon_d.wait();
    for (int i = 0; i < r_; i++) mons_c[i].wait();

    // update codes
    ec_->update(group_offset, data_old, data_new, codes, slab_size_);

    // write data and codes to devices
    mon_d.reset();
    phy_val pval_d_n(value->val, value->length);
    dev_idx = get_dev_idx(group_id, group_offset);
    parent_->ssds_[dev_idx].kv_astore(&pkey_d, &pval_d_n, on_io_complete, (void *)&mon_d);

    for (int i = 0; i < r_; i++) {
        mons_c[i].reset();
        dev_idx = get_dev_idx(group_id, k_+i);
        parent_->ssds_[dev_idx].kv_astore(&pkeys_c[i], &pvals_c[i], on_io_complete, (void *)&mons_c[i]);
    }

    // wait for write ios
    mon_d.wait();
    for (int i = 0; i < r_; i++) mons_c[i].wait();

    fl_.UnLock(group_id, l);

    // cleanup
    delete [] mons_c;
    free(pkeys_c);
    free(pvals_c);
    
    free(data_new);
    for (int i = 0; i < r_; i++) free(codes[i]);
    free(codes);
}

bool SlabQ::slab_delete(kvr_key *key, phy_key *pkey) {
    uint64_t index_id = pkey->get_seq();
    uint64_t group_id, group_offset;
    group_id = index_id/k_;
    group_offset = index_id%k_;
    int dev_idx;

    char *data_new = (char*) calloc(slab_size_, sizeof(char)); // all zero
    char *data_old = (char*) calloc(slab_size_, sizeof(char));
    char **codes = (char **)malloc(r_*sizeof(char*));
    for (int i = 0; i < r_; i++) codes[i] = (char*)calloc(slab_size_, sizeof(char));

    LockEntry * l = fl_.Lock(group_id);

    // read old data and codes
    phy_key pkey_d(sid_, group_id*k_+group_offset);
    phy_val pval_d(data_old, slab_size_);
    Monitor mon_d;
    dev_idx = get_dev_idx(group_id, group_offset);
    parent_->ssds_[dev_idx].kv_aget(&pkey_d, &pval_d, on_io_complete, (void *)&mon_d);

    phy_key *pkeys_c = (phy_key *)malloc(sizeof(phy_key)*(r_));
    phy_val *pvals_c = (phy_val *)malloc(sizeof(phy_val)*(r_));
    Monitor *mons_c = new Monitor[r_];
    for (int i = 0; i < r_; i++) {
        (void) new (&pkeys_c[i]) phy_key(sid_, group_id*k_);
        (void) new (&pvals_c[i]) phy_val(codes[i], slab_size_);
        dev_idx = get_dev_idx(group_id, k_+i);
        parent_->ssds_[dev_idx].kv_aget(&pkeys_c[i], &pvals_c[i], on_io_complete, (void *)&mons_c[i]);
    }

    // wait get ios completes
    mon_d.wait();
    for (int i = 0; i < r_; i++) mons_c[i].wait();

    // update codes
    ec_->update(group_offset, data_old, data_new, codes, slab_size_);

    for (int i = 0; i < r_; i++) {
        mons_c[i].reset();
        dev_idx = get_dev_idx(group_id, k_+i);
        parent_->ssds_[dev_idx].kv_astore(&pkeys_c[i], &pvals_c[i], on_io_complete, (void *)&mons_c[i]);
    }

    // delete data from device (sync call)
    dev_idx = get_dev_idx(group_id, group_offset);
    parent_->ssds_[dev_idx].kv_delete(&pkey_d);

    // wait for write ios
    for (int i = 0; i < r_; i++) mons_c[i].wait();

    // update mapping table
    std::string skey = std::string(key->key, key->length);
    parent_->key_map_->erase(&skey);

    fl_.UnLock(group_id, l);

    // cleanup
    delete [] mons_c;
    free(pkeys_c);
    free(pvals_c);
    
    free(data_new);
    for (int i = 0; i < r_; i++) free(codes[i]);
    free(codes);

}

int KVEC::kvr_get_slab_id(int size) {
    if (size <= slab_list_[0])
		return 0;
	for (int i = 0; i < num_slab_ - 1; i++)
	{
		if (size > slab_list_[i] && size <= slab_list_[i + 1])
			return (i + 1);
	}
	printf("[kvr_get_slab] beyond maximum slab_size error \n");
	return -1;
}

bool KVEC::kvr_insert(kvr_key *key, kvr_value *value) {
    int packed_val_len = value->length + VAL_SIZE_BYTES;
    int slab_id = kvr_get_slab_id(packed_val_len);
    SlabQ *slab = &slabs_[slab_id];

    std::string skey = std::string(key->key, key->length);
    LockEntry *l = req_key_fl_.Lock(skey);

    // pack value with val_len
    char *val_buf = (char *)malloc(packed_val_len);
    pack_value(val_buf, value);
    kvr_value packed_value = {val_buf, packed_val_len};
    // insert to slab
    slab->slab_insert(key, &packed_value);

    req_key_fl_.UnLock(skey, l);
    free(val_buf);
    return true;
}
bool KVEC::kvr_update(kvr_key *key, kvr_value *value) {
    int packed_val_len = value->length + VAL_SIZE_BYTES;
    // lookup mapping table
    phy_key pkey;
    std::string skey = std::string(key->key, key->length);
    LockEntry *l = req_key_fl_.Lock(skey);

    key_map_->lookup(&skey, &pkey);

    int slab_id = kvr_get_slab_id(packed_val_len);
    // pack value with val_len
    char *val_buf = (char *)malloc(packed_val_len);
    pack_value(val_buf, value);
    kvr_value packed_value = {val_buf, packed_val_len};

    if (slab_id == pkey.get_slab_id()) { //update the slab
        SlabQ *slab = &slabs_[slab_id];
        slab->slab_update(&packed_value, &pkey);
    }
    else { // insert + delete
        SlabQ *new_slab = &slabs_[slab_id];
        SlabQ *old_slab = &slabs_[pkey.get_slab_id()];
        old_slab->slab_delete(key, &pkey);
        old_slab->reclaim_index(pkey.get_seq());
        new_slab->slab_insert(key, &packed_value);
    }

    req_key_fl_.UnLock(skey, l);

    free(val_buf);
    return true;
}
bool KVEC::kvr_delete(kvr_key *key) {
    std::string skey = std::string(key->key, key->length);
    LockEntry *l = req_key_fl_.Lock(skey);

    phy_key pkey;
    // update log->phy translation table
    bool exist = key_map_->lookup(&skey, &pkey);
    if (!exist) {
        printf("[KVRaid::kvr_delete] logical key not exist\n");
        exit(-1);
    }
    //key_map_->erase(&skey);

    // insert to delete queue
    int slab_id = pkey.get_slab_id();
    SlabQ *slab = &slabs_[slab_id];
    slab->slab_delete(key, &pkey);
    slab->reclaim_index(pkey.get_seq());

    req_key_fl_.UnLock(skey, l);
    return true;
}

bool KVEC::kvr_get(kvr_key *key, kvr_value *value) {
    std::string skey = std::string(key->key, key->length);
    LockEntry *l = req_key_fl_.Lock(skey);
    
    phy_key pkey;
    // lookup log->phy translation table
    bool exist = key_map_->lookup(&skey, &pkey);
    if (!exist) {
        printf("[KVEC::kvr_get] logical key not exist\n");
        exit(-1);
    }

    int slab_id = pkey.get_slab_id();
    int seq = pkey.get_seq();
    // careful, different dev_idx mapping to KVRaid
    int dev_idx = (((seq/k_) % (k_+r_)) + seq%k_) % (k_+r_); 

    char *get_val_buf = (char *)malloc(slab_list_[slab_id]);
    phy_val pval(get_val_buf, slab_list_[slab_id]);
    //printf("get [ssd %d] skey %s, pkey %lu\n",dev_idx, skey.c_str(), pkey.get_seq());
    ssds_[dev_idx].kv_get(&pkey, &pval);

    req_key_fl_.UnLock(skey, l);

    value->val = (char*)malloc(slab_list_[slab_id]);
    unpack_value(get_val_buf, value);
    free(get_val_buf);

    return true;
}

bool KVEC::kvr_erased_get(int erased, kvr_key *key, kvr_value *value) {
    std::string skey = std::string(key->key, key->length);
    phy_key pkey;
    // lookup log->phy translation table
    bool exist = key_map_->lookup(&skey, &pkey);
    if (!exist) {
        printf("[KVEC::kvr_get] logical key not exist\n");
        exit(-1);
    }

    int slab_id = pkey.get_slab_id();
    int seq = pkey.get_seq();
    // careful, different dev_idx mapping to KVRaid
    int dev_idx = (((seq/k_) % (k_+r_)) + seq%k_) % (k_+r_); 

    if (dev_idx == erased) {
        int group_id = seq/k_;
        int group_offset = seq%k_;
        int slab_size_ = slab_list_[slab_id];
        int dev_index = group_id % (k_+r_);

        // decode buffers
        char **data = (char **)malloc(k_*sizeof(char *));
        char **codes = (char **)malloc(r_*sizeof(char *));
        for (int i = 0; i < k_; i++) data[i] = (char *)malloc(slab_size_);
        for (int i = 0; i < r_; i++) codes[i] = (char *)malloc(slab_size_);

        // read survival data and codes
        phy_key *pkeys_c = (phy_key *)malloc(sizeof(phy_key)*(k_+r_-1));
        phy_val *pvals_c = (phy_val *)malloc(sizeof(phy_val)*(k_+r_-1));
        Monitor *mons_c = new Monitor[k_+r_-1];
        int j = 0;
        int logic_erased;
        for (int i = 0; i < (k_+r_); i++) {
            if (dev_index != dev_idx) {
                if (i < k_) {
                    (void) new (&pkeys_c[j]) phy_key(slab_id, group_id*k_+i);
                    (void) new (&pvals_c[j]) phy_val(data[i], slab_size_);
                }
                else {
                    (void) new (&pkeys_c[j]) phy_key(slab_id, group_id*k_);
                    (void) new (&pvals_c[j]) phy_val(codes[i-k_], slab_size_);
                }
                ssds_[dev_index].kv_aget(&pkeys_c[j], &pvals_c[j], on_io_complete, (void *)&mons_c[j]);
                j++;
            }
            else logic_erased = i;
            dev_index = (dev_index+1)%(k_+r_);
        }
        for (int i = 0; i < (k_+r_-1); i++) {
            mons_c[i].wait();
        }

        // EC decode
        ec_.single_failure_decode(logic_erased, data, codes, slab_size_);
        value->val = (char*)malloc(slab_size_);
        unpack_value(data[logic_erased], value);

        for (int i = 0; i < k_; i++) free(data[i]);
        for (int i = 0; i < r_; i++) free(codes[i]);
        free(data);
        free(codes);
        free(pkeys_c);
        free(pvals_c);
    }
    else {
        char *get_val_buf = (char *)malloc(slab_list_[slab_id]);
        phy_val pval(get_val_buf, slab_list_[slab_id]);
        //printf("get [ssd %d] skey %s, pkey %lu\n",dev_idx, skey.c_str(), pkey.get_seq());
        ssds_[dev_idx].kv_get(&pkey, &pval);

        value->val = (char*)malloc(slab_list_[slab_id]);
        unpack_value(get_val_buf, value);
        free(get_val_buf);
    }
    return true;
}

bool KVEC::kvr_write_batch(WriteBatch *batch) {
    printf("NOT IMPLEMENT\n");
}

void KVEC::KVECIterator::retrieveValue(int userkey_len, std::string &retrieveKey, std::string &value) {
    int k_ = kvr_->k_;
    int r_ = kvr_->r_;
    int *slab_list_ = kvr_->slab_list_;
    KV_DEVICE *ssds_ = kvr_->ssds_;
    phy_key pkey;
    pkey.decode(&retrieveKey);
    
    int slab_id = pkey.get_slab_id();
    int seq = pkey.get_seq();
    int dev_idx = ((seq/k_ % (k_+r_)) + seq%k_) % (k_+r_);

    char *actual_val = (char*)malloc(slab_list_[slab_id]);
    phy_val pval(actual_val, slab_list_[slab_id]);
    //printf("get [ssd %d] skey %s, pkey %lu\n",dev_idx, skey.c_str(), pkey.get_seq());
    ssds_[dev_idx].kv_get(&pkey, &pval);
    kvr_value get_val;
    get_val.val = (char *)malloc(pval.actual_len);
    unpack_value(actual_val, &get_val);

    value.clear();
    value.append(get_val.val, get_val.length);
    free(actual_val);
    free(get_val.val);
}

void KVEC::save_meta() {
    std::string meta_key = "KVEC_meta";
    std::string meta_val;
    meta_val.append((char *)&num_slab_, sizeof(num_slab_)); // num_slabs
    for (int i = 0; i < num_slab_; i++) {
        int num_avail_seq = slabs_[i].avail_seq_.size();
        meta_val.append((char *)&num_avail_seq, sizeof(num_avail_seq)); // num of avail seq
        while (!slabs_[i].avail_seq_.empty()) {
            uint64_t avail_seq = slabs_[i].avail_seq_.front();
            meta_val.append((char *)&avail_seq, sizeof(avail_seq));
            slabs_[i].avail_seq_.pop();
        }
        uint64_t seq = slabs_[i].get_curr_seq();
        meta_val.append((char *)&seq, sizeof(uint64_t)); // group_id per slab
    }
    for (int i = 0; i < r_; i++)
        ssds_[i].kv_store(&meta_key, &meta_val); // mirror to num_r devs
}
    
bool KVEC::load_meta(int size) {
    std::string meta_key = "KVEC_meta";
    std::string meta_val;
    ssds_[0].kv_get(&meta_key, &meta_val); // only access dev_0;
    if (meta_val.size() == 0) {
        return false; // no meta;
    }

    char *p = (char *)meta_val.c_str();
    int num_slabs = *(int *)p;
    p += sizeof(int);
    if (num_slabs != size) {
        printf("number of slabs not same as last open, exit\n");
        exit(-1);
    }
    for (int i = 0; i < size; i++) {
        int num_avail_seq = *(int *)p;
        p += sizeof(int);
        for (int j = 0 ; j < num_avail_seq; j++) {
            int avail_seq = *(uint64_t *)p;
            p += sizeof(uint64_t);
            slabs_[i].avail_seq_.push(avail_seq);
        }
        slabs_[i].seq_ = *(uint64_t *)p;
        p += sizeof(uint64_t);
    }
    return true;
}


} // end namespace kvec

KVR *NewKVEC(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t) {
    return new kvec::KVEC(num_d, num_r, num_slab, s_list, conts, meta_t);
}