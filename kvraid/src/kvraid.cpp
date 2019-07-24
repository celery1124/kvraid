/* kvraid.cpp
* 04/23/2019
* by Mian Qin
*/
#include <chrono>
#include "kvraid.h"

#define RECLAIMS_BULKS 2 * k_
#define MAX_TRIM_NUM 64 * k_

#define GC_DEV_USAGE_VOL_RATIO_THRES 2
#define GC_DEV_UTIL_THRES 0.5
#define GC_DELETE_Q_THRES 0

#define DEQ_TIMEOUT 1000 // us

namespace kvraid {

void SlabQ::get_delete_ids(std::vector<uint64_t>& groups, int trim_num) {
    {
        std::unique_lock<std::mutex> lock(seq_mutex_);
        while (!delete_seq_.empty() && trim_num > 0) {
            groups.push_back(delete_seq_.front());
            delete_seq_.pop();
            trim_num--;
        }
    }
}

void SlabQ::add_delete_ids(std::vector<uint64_t>& groups) {
    {
        std::unique_lock<std::mutex> lock(seq_mutex_);
        for (auto it=groups.begin(); it!=groups.end(); ++it) {
            delete_seq_.push(*it);
        }
    }
}

uint64_t SlabQ::get_new_group_id() {
    uint64_t ret;
    {
        std::unique_lock<std::mutex> lock(seq_mutex_);
        ret = seq_;
        seq_++;
    }
    return ret;
}

void SlabQ::add_delete_id(uint64_t group_id) {
    {
        std::unique_lock<std::mutex> lock(seq_mutex_);
        delete_seq_.push(group_id);
    }
}

bool SlabQ::track_finish(int id, int num_ios) {
    std::lock_guard<std::mutex> guard(finish_mtx_);
    if(finish_.count(id))
        finish_[id]++;
    else
        finish_[id] = 1;

    if (finish_[id] == num_ios) {
        finish_.erase(id);
        return true;
    } else {
        return false;
    }
}

void DeleteQ::insert(uint64_t index) {
    uint64_t group_id = index/group_size_;
    uint8_t group_offset = index%group_size_;
    {
        std::unique_lock<std::mutex> lock(gl_mutex_);
        // if whole group is updated, put to trim list
        if (group_list_[group_id].size() == (k_ - 1)) {
            group_list_.erase(group_id);
            parent_->add_delete_id(group_id);
            count_ -= (k_-1);
        }
        else {
            group_list_[group_id].push_back(group_offset);
            count_++;
        }
    }
}

void DeleteQ::scan (int min_num_invalids, std::vector<uint64_t>& actives, 
    std::vector<uint64_t>& groups) {
    int scan_len = 0;
    std::unique_lock<std::mutex> lock(gl_mutex_);
    auto it = group_list_.begin();
    while (it != group_list_.end()) {
        // control scan length
        if (actives.size() > MAX_ENTRIES_PER_GC || 
        scan_len++ > MAX_SCAN_LEN_PER_GC) break;

        if (it->second.size() >= min_num_invalids) {
            groups.push_back(it->first);
            // get active list 
            char *tmp_bitmap = (char*)calloc(k_, sizeof(char));
            int num_actives = 0;
            for (int i = 0; i< it->second.size(); i++) {
                tmp_bitmap[it->second[i]] = 1;
            }
            for (int i = 0; i< k_; i++) {
                if(tmp_bitmap[i]==0) {
                    actives.push_back(it->first*group_size_+i);
                    num_actives++;
                }
            }
            
            count_ -= num_actives;
            it = group_list_.erase(it);
            free(tmp_bitmap);
        }
        else
            ++it;
        
    }
}

void DeleteQ::erase(uint64_t index) {
    uint64_t group_id = index/group_size_;
    uint8_t group_offset = index%group_size_;
    bool found = false;
    std::unique_lock<std::mutex> lock(gl_mutex_);
    auto it = group_list_.find(group_id);
    if (it == group_list_.end()) {
        printf("KVR_REPLACE cannot find updated relcaim group_id in delete q\n");
        exit(-1);
    }
    for (auto i = it->second.begin(); i!=it->second.end(); ++i) {
        if (*i == group_offset) {
            it->second.erase(i);
            found = true; break;
        }
    }
    if (!found) {
        printf("KVR_REPLACE cannot find updated relcaim id in delete q\n");
        exit(-1);
    }
    count_ -= 1;
    if (it->second.size() == 0)
        group_list_.erase(group_id);
}

static void pack_value(char *dst, kvr_key *key, kvr_value *val) {
    uint8_t key_len = key->length;
    *((uint8_t*)dst) = key->length;
    memcpy(dst+KEY_SIZE_BYTES, key->key, key_len);
    memcpy(dst+KEY_SIZE_BYTES+key_len, val->val, val->length);
}

static void unpack_value(char *src, uint32_t val_len, kvr_key *key, kvr_value *val) {
    uint8_t key_len = *((uint8_t*)src);
    if (key != NULL) {
        key->length = key_len;
        key->key = src+KEY_SIZE_BYTES;
    }
    val->length = val_len-key_len-KEY_SIZE_BYTES;
    val->val = src+KEY_SIZE_BYTES+key_len;
}

static void on_bulk_write_complete(void *arg) {
    bulk_io_context *bulk_io_ctx = (bulk_io_context *)arg;
    if(!bulk_io_ctx->q->track_finish(bulk_io_ctx->id, bulk_io_ctx->num_ios)) {
        return;
    }
    
    // post process
    kvr_context *kvr_ctx;
    for (int i = 0; i < bulk_io_ctx->bulk_size; i++) {
        kvr_ctx = bulk_io_ctx->kvr_ctxs[i];
        {
            std::unique_lock<std::mutex> lck(kvr_ctx->mtx);
            kvr_ctx->ready = true;
            kvr_ctx->cv.notify_one();
        }
    }

    // free memory
    free(bulk_io_ctx->keys);
    free(bulk_io_ctx->vals);
    delete [] (bulk_io_ctx->kvr_ctxs);
    delete bulk_io_ctx;
};

static void on_delete_complete(void *arg) {
    delete_io_context *delete_io_ctx = (delete_io_context *)arg;
    
    // free memory
    delete delete_io_ctx->key;
    delete delete_io_ctx;
};

// bulk dequeue, either dequeue max or wait for time out
template <class T> 
static int dequeue_bulk_timed(moodycamel::BlockingConcurrentQueue<T*> &q, 
    T **kvr_ctxs,
    size_t max, int64_t timeout_usecs) {
    const uint64_t quanta = 100;
    const double timeout = ((double)timeout_usecs - quanta) / 1000000;
    size_t total_count = 0;
    size_t max_batch = max;
    auto start = std::chrono::system_clock::now();
    auto elapsed = [start]() -> double {
        return std::chrono::duration<double>(std::chrono::system_clock::now() - start).count();
    };
    do
    {
        auto count = q.wait_dequeue_bulk_timed(&kvr_ctxs[total_count], max_batch, quanta);
        total_count += count;
        max_batch -= count;
    } while (total_count < max && elapsed() < timeout);

    return total_count;
};

void SlabQ::processQ() {
    while (true) {
        // clean buffer
        //printf("======clean buffer======\n");
        clear_data_buf();
        uint64_t group_id = get_new_group_id();
        int dev_idx_start = group_id%(k_+r_);
        int dev_idx;
        int total_count = 0;
        do {
            int count;
            kvr_context **kvr_ctxs = new kvr_context*[k_];
            //count = q.wait_dequeue_bulk_timed(kvr_ctxs, k_-total_count, 7000);
            count = dequeue_bulk_timed<kvr_context>(q, kvr_ctxs, k_-total_count, DEQ_TIMEOUT);

            if(count == 0) continue;

            // allocate pkey, pval
            phy_key *pkeys = (phy_key *)malloc(sizeof(phy_key)*(count+r_));
            phy_val *pvals = (phy_val *)malloc(sizeof(phy_val)*(count+r_));

            // write to buffer and apply ec
            for (int i = 0; i < count; i++) {
                memcpy(data_[i + total_count], kvr_ctxs[i]->value->val, kvr_ctxs[i]->value->length);
            } 
            ec_->encode(data_, code_, slab_size_);

            // prepare bulk_io_context
            uint64_t unique_id = group_id*k_+total_count;
            bulk_io_context *bulk_io_ctx = new bulk_io_context 
            {unique_id, count + r_, count, kvr_ctxs, pkeys, pvals, this};

            // write to index map
            dev_idx = (dev_idx_start+total_count) % (k_+r_);
            for (int i = 0; i < count; i++){
                std::string skey = std::string(kvr_ctxs[i]->key->key, kvr_ctxs[i]->key->length);
                phy_key stale_key;
                (void) new (&pkeys[i]) phy_key(sid_, group_id*(k_+r_) + total_count + i);
                
                if (kvr_ctxs[i]->ops == KVR_INSERT) {
                    // insert 
                    parent_->key_map_->insert(&skey, &pkeys[i]);
                    //printf("insert %s -> %d\n",skey.c_str(), pkeys[i].get_seq());
                }
                else if (kvr_ctxs[i]->ops == KVR_UPDATE) {
                    // update
                    std::unique_lock<std::mutex> lock(parent_->idx_mutex_);
                    parent_->key_map_->lookup(&skey, &stale_key);
                    parent_->key_map_->update(&skey, &pkeys[i]);
                    //printf("update %s -> (%d) %d\n",skey.c_str(), stale_key.get_seq(), pkeys[i].get_seq());
                }
                else if (kvr_ctxs[i]->ops == KVR_REPLACE) {
                    // replace
                    phy_key tmp;
                    std::unique_lock<std::mutex> lock(parent_->idx_mutex_);
                    bool exist = parent_->key_map_->lookup(&skey, &tmp);
                    if (exist && tmp == *(kvr_ctxs[i]->kv_ctx->pkey)) {
                        parent_->key_map_->update(&skey, &pkeys[i]);
                    }
                    else { // reclaimed kv got updated/deleted (rare)
                        // we need to update the deleteQ
                        delete_q.erase(kvr_ctxs[i]->kv_ctx->pkey->get_seq());
                        dq_insert(pkeys[i].get_seq());
                        
                        std::unique_lock<std::mutex> lck(kvr_ctxs[i]->mtx);
                        kvr_ctxs[i]->ready = true;
                        kvr_ctxs[i]->cv.notify_one();
                        continue;  // no need for IO
                    }
                }
                else {
                    if (kvr_ctxs[i]->ops == KVR_INSERT)
                        printf("[SlabQ::processQ] insert logical key already exist\n");
                    else if (kvr_ctxs[i]->ops == KVR_UPDATE)
                        printf("[SlabQ::processQ] update logical key not exist\n");
                    else 
                        printf("[SlabQ::processQ] unsupport KVR_OPS\n");
                    exit(-1);
                }

                if (kvr_ctxs[i]->ops == KVR_UPDATE) {
                    dq_insert(stale_key.get_seq());
                }
                // write data
                (void) new (&pvals[i]) phy_val(kvr_ctxs[i]->value->val, kvr_ctxs[i]->value->length);
                parent_->ssds_[dev_idx].kv_astore(&pkeys[i], &pvals[i], on_bulk_write_complete, (void*)bulk_io_ctx);
                //printf("insert [ssd %d] skey %s pkey %lu\n",dev_idx, skey.c_str(), pkeys[i].get_seq());
                dev_idx = (dev_idx+1)%(k_+r_);
            }
            // write code
            dev_idx = (dev_idx_start+k_) % (k_+r_);
            for (int j = 0; j < r_; j++) {
                int i = j+count;
                (void) new (&pkeys[i]) phy_key(sid_, group_id*(k_+r_) + k_+j);
                (void) new (&pvals[i]) phy_val(code_[j], slab_size_);

                parent_->ssds_[dev_idx].kv_astore(&pkeys[i], &pvals[i], on_bulk_write_complete, (void*)bulk_io_ctx);
                //printf("insert [ssd %d] group_id %d pkey %lu\n",dev_idx, group_id, pkeys[i].get_seq());
                dev_idx = (dev_idx+1)%(k_+r_);
            }
            total_count += count;

            // report
            // if (count == k_) {
            //     // full group
            //     printf("[%lu] full count = %d\n", group_id, total_count);
            //     break;
            // }
            // else {
            //     // partial group
            //     if (count != 0)
            //         printf("[%lu] partial count = %d\n", group_id, total_count); 
            // }

        // clean up
        } while (total_count < k_);
        
    }
}

void SlabQ::dq_insert(uint64_t index) {
    delete_q.insert(index);
}

void KVRaid::DoTrim(int slab_id) {
    std::vector<uint64_t> groups;
    SlabQ *slab = &slabs_[slab_id];
    slab->get_delete_ids(groups, MAX_TRIM_NUM);

    int group_size = k_+r_;
    for (auto it = groups.begin(); it != groups.end(); ++it) {
        uint64_t group_id = *it;
        int dev_idx = group_id%group_size;
        //physical trim
        for (int i = 0; i < group_size; i++) {
            phy_key *phykey = new phy_key(slab_id, group_id*group_size + i);
            delete_io_context *delete_io_ctx = new delete_io_context {dev_idx, phykey};
            ssds_[dev_idx].kv_adelete(phykey, on_delete_complete, delete_io_ctx);
            //printf("delete [ssd %d] group_id %d, phykey %d\n",dev_idx, group_id, phykey->get_seq());
            dev_idx = (dev_idx+1)%group_size;
        }
    }


    data_volume_.fetch_sub(slab_list_[slab_id]*groups.size()*(k_+r_), std::memory_order_relaxed);
}


void on_reclaim_get_complete(void *args) {
    reclaim_get_context* ctx = (reclaim_get_context*) args;
    kv_context* kv_ctx = new kv_context {ctx->pkey, ctx->pval};
    ctx->kvQ->enqueue(kv_ctx);

    delete ctx;
}

void KVRaid::DoReclaim(int slab_id) {
    std::vector<uint64_t> actives;
    std::vector<uint64_t> groups;
    int slab_size = slab_list_[slab_id];
    SlabQ *slab = &slabs_[slab_id];
    DeleteQ *dq = &(slab->delete_q);

    // calculate cost
    int min_num_invalids = k_-1;
    for (int i = k_-1; i>0; i--) {
        if ((r_+1)*i > (k_+r_)) min_num_invalids = i;
        else break;
    }
    
    // scan delete q
    dq->scan(min_num_invalids, actives, groups);

    // DO reclaim
    int num_ios = actives.size();
    moodycamel::BlockingConcurrentQueue<kv_context *> kvQ;
    for (auto it = actives.begin(); it != actives.end(); ++it) {
        int dev_idx = slab->get_dev_idx(*it);
        phy_key *pkey = new phy_key(slab_id, *it);
        char *c_val = (char*)malloc(slab_size);
        phy_val *pval = new phy_val(c_val, slab_size);
        reclaim_get_context *aget_ctx = new reclaim_get_context {num_ios, pkey, pval, &kvQ};
        ssds_[dev_idx].kv_aget(pkey, pval, on_reclaim_get_complete, aget_ctx);
    }

    int count;
    std::vector<kvr_context*> kvr_ctx_vec;
    while (num_ios > 0) {
        kv_context **kvs = new kv_context*[RECLAIMS_BULKS];
        //count = kvQ.wait_dequeue_bulk_timed(kvs, RECLAIMS_BULKS, 7000);
        count = dequeue_bulk_timed<kv_context>(kvQ, kvs, RECLAIMS_BULKS, DEQ_TIMEOUT);

        for (int i = 0; i < count; i++) {
            kvr_key *mv_key = new kvr_key;
            kvr_value *mv_val = new kvr_value;
            unpack_value(kvs[i]->pval->c_val, kvs[i]->pval->val_len, mv_key, mv_val);
            kvr_context* kvr_ctx = new kvr_context(KVR_REPLACE, mv_key, mv_val, kvs[i]);
            slab->q.enqueue(kvr_ctx);
            kvr_ctx_vec.push_back(kvr_ctx);
        }
        delete [] kvs;
        num_ios -= count;
    }

    data_volume_.fetch_add(slab_list_[slab_id]*actives.size()*k_/r_, std::memory_order_relaxed);
    data_volume_.fetch_sub(slab_list_[slab_id]*groups.size()*(k_+r_), std::memory_order_relaxed);


    // wait for all IO complete
    for (int i = 0; i < kvr_ctx_vec.size(); i++) {
        {
            std::unique_lock<std::mutex> lck(kvr_ctx_vec[i]->mtx);
            kvr_ctx_vec[i]->cv.wait(lck);
        }

        free(kvr_ctx_vec[i]->kv_ctx->pval->c_val);
        delete kvr_ctx_vec[i]->kv_ctx->pkey;
        delete kvr_ctx_vec[i]->kv_ctx->pval;
        delete kvr_ctx_vec[i]->kv_ctx;
        delete kvr_ctx_vec[i];
    }

    // release phy key (for trim)
    slab->add_delete_ids(groups);    
}

bool KVRaid::CheckGCTrigger(int slab_id) {
    // Three conditions to trigger GC, 
    // 1, Device usage to acual data volume ratio pass a certain threshold;
    // 2, Device utilization pass a certain threshold;
    // 3, delete_q_ size is too large (we want to keep the invalid-alive low)
    // return get_usage()/get_volume() > GC_DEV_USAGE_VOL_RATIO_THRES ||
    //     get_util() >= GC_DEV_UTIL_THRES || 
    //     slabs_[slab_id].dq_size() >= GC_DELETE_Q_THRES;
    
    return false;
}

void KVRaid::DoGC() {
    for (int i = 0; i < num_slab_; i++) {
        SlabQ *slab = &slabs_[i];
        // 1, check trim list and do trim
        DoTrim(i);
        // 2, check GC trigger condition
        if (CheckGCTrigger(i)) {
            DoReclaim(i);
        }
    }
    
}

void KVRaid::bg_GC() {
    const auto timeWindow = std::chrono::milliseconds(200);

    while(true)
    {
        auto start = std::chrono::steady_clock::now();
        DoGC();
        auto end = std::chrono::steady_clock::now();
        auto elapsed = end - start;

        auto timeToWait = timeWindow - elapsed;
        if(timeToWait > std::chrono::milliseconds::zero())
        {
            std::this_thread::sleep_for(timeToWait);
        }
    }
}

int KVRaid::kvr_get_slab_id(int size) {
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

bool KVRaid::kvr_insert(kvr_key *key, kvr_value *value) {
    uint32_t actual_vlen = key->length + value->length + KEY_SIZE_BYTES;
    int slab_id = kvr_get_slab_id(actual_vlen);
    SlabQ *slab = &slabs_[slab_id];

    // generate new value 
    char *pack_val = (char*)malloc(actual_vlen);
    pack_value(pack_val, key, value);
    kvr_value new_value = {pack_val, actual_vlen};

    // write to the context queue
    kvr_context kvr_ctx(KVR_INSERT, key, &new_value);
    slab->q.enqueue(&kvr_ctx);

    data_volume_.fetch_add(slab_list_[slab_id]*k_/r_, std::memory_order_relaxed);

    //wait for IOs finish
    {
        std::unique_lock<std::mutex> lck(kvr_ctx.mtx);
        while (!kvr_ctx.ready) kvr_ctx.cv.wait(lck);
    }
    free(pack_val);
    return true;
}
bool KVRaid::kvr_update(kvr_key *key, kvr_value *value) {
    uint32_t actual_vlen = key->length + value->length + KEY_SIZE_BYTES;
    int slab_id = kvr_get_slab_id(actual_vlen);
    SlabQ *slab = &slabs_[slab_id];

    // generate new value 
    char *pack_val = (char*)malloc(actual_vlen);
    pack_value(pack_val, key, value);
    kvr_value new_value = {pack_val, actual_vlen};

    // write to the context queue
    kvr_context kvr_ctx(KVR_UPDATE, key, &new_value);
    slab->q.enqueue(&kvr_ctx);

    data_volume_.fetch_add(slab_list_[slab_id]*k_/r_, std::memory_order_relaxed);

    //wait for IOs finish
    {
        std::unique_lock<std::mutex> lck(kvr_ctx.mtx);
        while (!kvr_ctx.ready) kvr_ctx.cv.wait(lck);
    }
    free(pack_val);
    return true;
}
bool KVRaid::kvr_delete(kvr_key *key) {
    std::string skey = std::string(key->key, key->length);
    phy_key pkey;
    // update log->phy translation table
    bool exist = key_map_->lookup(&skey, &pkey);
    if (!exist) {
        printf("[KVRaid::kvr_delete] logical key not exist\n");
        exit(-1);
    }
    key_map_->erase(&skey);

    // insert to delete queue
    int slab_id = pkey.get_slab_id();
    SlabQ *slab = &slabs_[slab_id];
    slab->dq_insert(pkey.get_seq());

}
bool KVRaid::kvr_get(kvr_key *key, kvr_value *value) {
    std::string skey = std::string(key->key, key->length);
    phy_key pkey;
    // lookup log->phy translation table
    bool exist = key_map_->lookup(&skey, &pkey);
    if (!exist) {
        printf("[KVRaid::kvr_get] logical key not exist\n");
        exit(-1);
    }
    
    int slab_id = pkey.get_slab_id();
    int seq = pkey.get_seq();
    int dev_idx = ((seq/(k_+r_) % (k_+r_)) + seq%(k_+r_)) % (k_+r_);

    char *actual_val = (char*)malloc(slab_list_[slab_id]);
    phy_val pval(actual_val, slab_list_[slab_id]);
    //printf("get [ssd %d] skey %s, pkey %lu\n",dev_idx, skey.c_str(), pkey.get_seq());
    ssds_[dev_idx].kv_get(&pkey, &pval);

    kvr_value new_val;
    unpack_value(pval.c_val, pval.actual_len, NULL, &new_val);

    value->length = new_val.length;
    value->val = (char*)malloc(new_val.length);
    memcpy(value->val, actual_val+key->length+KEY_SIZE_BYTES, value->length);

    free(actual_val);

    return true;
}



bool KVRaid::kvr_write_batch(WriteBatch *batch) {
    std::vector<kvr_context*> kvr_ctx_vec;
    for (auto it = batch->list_.begin(); it != batch->list_.end(); ++it) {
        kvr_key *key = it->key;
        kvr_value *value = it->val;
        uint32_t actual_vlen = key->length + value->length + KEY_SIZE_BYTES;
        int slab_id = kvr_get_slab_id(actual_vlen);
        SlabQ *slab = &slabs_[slab_id];

        // generate new value 
        char *pack_val = (char*)malloc(actual_vlen);
        pack_value(pack_val, key, value);
        kvr_value new_value = {pack_val, actual_vlen};

        // write to the context queue
        kvr_context *kvr_ctx = new kvr_context(KVR_OPS(it->type), key, &new_value);
        kvr_ctx_vec.push_back(kvr_ctx);
        slab->q.enqueue(kvr_ctx);
    }

    // wait util whole batch is done
    for (auto it = kvr_ctx_vec.begin(); it != kvr_ctx_vec.end(); ++it) {
        std::unique_lock<std::mutex> lck((*it)->mtx);
        while (!(*it)->ready) (*it)->cv.wait(lck);
        delete (*it);
    }

}


} // end namespace kvraid

KVR *NewKVRaid(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t) {
    return new kvraid::KVRaid(num_d, num_r, num_slab, s_list, conts, meta_t);
}