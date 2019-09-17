/* kvraid_pack.cpp
* 04/23/2019
* by Mian Qin
*/
#include <chrono>
#include "kvraid_pack.h"

#define RECLAIMS_BULKS 4 * k_
#define MAX_TRIM_NUM 512
#define TRIM_GUARD_NUM 2048

#define GC_DEV_USAGE_VOL_RATIO_THRES 2
#define GC_DEV_UTIL_THRES 0.5
#define GC_DELETE_Q_THRES 0

#define DEQ_TIMEOUT 500 // us

namespace kvraid_pack {

void SlabQ::get_delete_ids(std::vector<uint64_t>& groups, int trim_num, int guard_num) {
    {
        std::unique_lock<std::mutex> lock(dseq_mutex_);
        while (!delete_seq_.empty() && trim_num > 0 && delete_seq_.size() >= guard_num) {
            groups.push_back(delete_seq_.front());
            delete_seq_.pop();
            trim_num--;
        }
    }
}

void SlabQ::get_all_delete_ids(std::vector<uint64_t>& groups) {
    {
        std::unique_lock<std::mutex> lock(dseq_mutex_);
        while (!delete_seq_.empty()) {
            groups.push_back(delete_seq_.front());
            delete_seq_.pop();
        }
    }
}

void SlabQ::add_delete_ids(std::vector<uint64_t>& groups) {
    {
        std::unique_lock<std::mutex> lock(dseq_mutex_);
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

uint64_t SlabQ::get_curr_group_id(){
    uint64_t ret;
    {
        std::unique_lock<std::mutex> lock(seq_mutex_);
        ret = seq_;
    }
    return ret;
}

void SlabQ::add_delete_id(uint64_t group_id) {
    {
        std::unique_lock<std::mutex> lock(dseq_mutex_);
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
        group_list_[group_id].push_back(group_offset);
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
            // get active list 
            char *tmp_bitmap = (char*)calloc(group_size_, sizeof(char));
            int num_actives = 0;
            for (int i = 0; i< it->second.size(); i++) {
                tmp_bitmap[it->second[i]] = 1;
            }
            for (int i = 0; i< group_size_; i++) {
                if(tmp_bitmap[i]==0) {
                    actives.push_back(it->first*group_size_+i);
                    num_actives++;
                }
            }
            if (num_actives == 0) {// can be trim directly
                parent_->add_delete_id(it->first);
            }
            else
                groups.push_back(it->first);
            it = group_list_.erase(it);
            free(tmp_bitmap);
        }
        else
            ++it;
    }
}

bool DeleteQ::erase(uint64_t index) {
    uint64_t group_id = index/group_size_;
    uint8_t group_offset = index%group_size_;
    bool found = false;
    std::unique_lock<std::mutex> lock(gl_mutex_);
    auto it = group_list_.find(group_id);
    if (it == group_list_.end()) {
        printf("KVR_REPLACE cannot find updated relcaim group_id in delete q\n");
        return false;
    }
    for (auto i = it->second.begin(); i!=it->second.end(); ++i) {
        if (*i == group_offset) {
            it->second.erase(i);
            found = true; break;
        }
    }
    
    if (it->second.size() == 0)
        group_list_.erase(group_id);
    
    return found;
}

static void pack_value(char *dst, kvr_key *key, kvr_value *val) {
    char *p = dst;
    *((uint8_t*)p) = key->length;
    p += KEY_SIZE_BYTES;
    memcpy(p, key->key, key->length);
    p += key->length;
    *((uint32_t*)p) = val->length;
    p += VAL_SIZE_BYTES;
    memcpy(p, val->val, val->length);
}

static void unpack_value(char *src, kvr_key *key, kvr_value *val) {
    uint8_t key_len = *((uint8_t*)src);
    if (key != NULL) {
        key->length = key_len;
        key->key = src+KEY_SIZE_BYTES;
    }
    if (val != NULL) {
        val->length = *((uint32_t *)(src+KEY_SIZE_BYTES+key_len));
        val->val = src+KEY_SIZE_BYTES+key_len+VAL_SIZE_BYTES;
    }
}

static void new_unpack_value(int pack_size, int pack_id, char *src, kvr_key *key, kvr_value *val) {
    char *p = src;
    for (int i = 0; i < pack_size; i++) {
        uint8_t key_len = *((uint8_t*)p);
        p += KEY_SIZE_BYTES;
        if (key != NULL) {
            key->length = key_len;
            key->key = p;
        }
        p += key_len;
        uint32_t val_len = *((uint32_t *)(p));
        p += VAL_SIZE_BYTES;
        if (val != NULL) {
            val->length = val_len;
            val->val = p;
        }
        p += val_len;
        if (i == pack_id) break;
    }
}

static void on_bulk_write_complete(void *arg) {
    bulk_io_context *bulk_io_ctx = (bulk_io_context *)arg;
    if(!bulk_io_ctx->q->track_finish(bulk_io_ctx->id, bulk_io_ctx->num_ios)) {
        return;
    }
    
    // post process
    kvr_context *kvr_ctx;
    phy_key *pkeys = bulk_io_ctx->keys;
    for (int i = 0; i < bulk_io_ctx->bulk_size; i++) {
        kvr_ctx = bulk_io_ctx->kvr_ctxs[i];
        
        {
            std::unique_lock<std::mutex> lck(kvr_ctx->mtx);
            kvr_ctx->ready = true;
            kvr_ctx->cv.notify_one();
        }
    }

    // free memory
    for (int i = 0; i < bulk_io_ctx->k; i++) free(bulk_io_ctx->data_buf[i]);
    for (int i = 0; i < bulk_io_ctx->r; i++) free(bulk_io_ctx->code_buf[i]);
    delete [] (bulk_io_ctx->data_buf);
    delete [] (bulk_io_ctx->code_buf);
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

int SlabQ::get_id() {return sid_;}

void SlabQ::processQ(int id) {
    int *pack_id = new int[k_];
    int *pack_offset = new int[k_];
    while (true) {
        
        // create EC buffer for new group
        //printf("======create buffer======\n");
        char **data = new char*[k_];
        char **code = new char*[r_];
        int buffer_size = slab_size_ * pack_size_;
        for (int i = 0; i<k_; i++) {
            data[i] = (char *) calloc(buffer_size, sizeof(char));
            pack_id[i] = 0;
            pack_offset[i] = 0;
        }
        for (int i = 0; i<r_; i++) {
            code[i] = (char *) malloc(buffer_size);
        }
        uint64_t group_id = get_new_group_id();
        int dev_idx_start = group_id%(k_+r_);
        int dev_idx;
        int total_count = 0;
        int bulk_count = k_*pack_size_ ;
        do {
            // check thread shutdown
            {
                std::unique_lock<std::mutex> lck (thread_m_[id]);
                if (shutdown_[id] == true) {
                    delete [] pack_id; 
                    delete [] pack_offset; 
                    return;
                }
            }
            int count;
            kvr_context **kvr_ctxs = new kvr_context*[bulk_count]; // TODO might leak when shutdown
            //count = q.wait_dequeue_bulk_timed(kvr_ctxs, k_-total_count, 7000);
            count = dequeue_bulk_timed<kvr_context>(q, kvr_ctxs, bulk_count-total_count, DEQ_TIMEOUT);

            if(count == 0) { // dequeue timeout
                delete kvr_ctxs;
                continue;
            }

            // write to buffer and apply ec
            // tight pack on each data chunk
            int pack_count = 0;
            int chunk_start = -1, chunk_end = -1;
            for (int i = 0; i < k_; i++) {
                if (pack_id[i] == pack_size_) continue;
                for (int j = pack_id[i]; j < pack_size_; j++) {
                    if (chunk_start == -1) chunk_start = i;
                    memcpy(data[i]+pack_offset[i], kvr_ctxs[pack_count]->value->val, kvr_ctxs[pack_count]->value->length);
                    pack_id[i]++;
                    pack_offset[i] += kvr_ctxs[pack_count]->value->length;
                    pack_count++;

                    if (pack_count == count) {
                        chunk_end = i;
                        break;
                    }
                }
                if (chunk_end != -1) break;
            }
            ec_->encode(data, code, slab_size_*pack_size_);


            // allocate pkey, pval
            int io_count = chunk_end - chunk_start + 1;
            phy_key *pkeys = (phy_key *)malloc(sizeof(phy_key)*(io_count+r_));
            phy_val *pvals = (phy_val *)malloc(sizeof(phy_val)*(io_count+r_));

            // prepare bulk_io_context
            uint64_t unique_id = group_id*k_*pack_size_+total_count;
            bulk_io_context *bulk_io_ctx = new bulk_io_context 
            {unique_id, io_count + r_, count, kvr_ctxs, pkeys, pvals, k_, r_, data, code, this};

            // write to index map
            dev_idx = (dev_idx_start+chunk_start) % (k_+r_);
            for (int i = 0; i < io_count; i++) {
                (void) new (&pkeys[i]) phy_key(sid_, group_id*k_*pack_size_ + (chunk_start + i)*pack_size_);
                
                // write data
                (void) new (&pvals[i]) phy_val(data[chunk_start+i], pack_offset[chunk_start+i]);
                parent_->ssds_[dev_idx].kv_astore(&pkeys[i], &pvals[i], on_bulk_write_complete, (void*)bulk_io_ctx);
                //printf("insert [ssd %d] skey %s pkey %lu\n",dev_idx, skey.c_str(), pkeys[i].get_seq());
                dev_idx = (dev_idx+1)%(k_+r_);
            }
            // update mapping table
            for (int i = 0; i < count; i++){
                std::string skey = std::string(kvr_ctxs[i]->key->key, kvr_ctxs[i]->key->length);
                phy_key stale_key;
                int chunk_id = (total_count + i)/pack_size_ - chunk_start;

                phy_key assigned_pkey(sid_, group_id*k_*pack_size_ + i + total_count);
                kvr_ctxs[i]->replace_key = assigned_pkey;
                if (kvr_ctxs[i]->ops == KVR_INSERT) {
                    // insert 
                    parent_->key_map_->insert(&skey, &assigned_pkey);
                    //printf("insert %s -> %d\n",skey.c_str(), assigned_pkey.get_seq());
                }
                else if (kvr_ctxs[i]->ops == KVR_UPDATE) {
                    // update
                    parent_->key_map_->readmodifywrite(&skey, &stale_key, &assigned_pkey);
                    int del_slab_id = stale_key.get_slab_id();
                    parent_->slabs_[del_slab_id].dq_insert(stale_key.get_seq());
                    //printf("update %s -> (%d) %d\n",skey.c_str(), stale_key.get_seq(), assigned_pkey.get_seq());
                }
                else if (kvr_ctxs[i]->ops == KVR_REPLACE) {
                    // // replace
                    // phy_key rd_pkey;
                    // bool match;
                    // match = parent_->key_map_->readtestupdate(&skey, &rd_pkey, kvr_ctxs[i]->kv_ctx->pkey, &pkeys[i]);
                    // if (!match) { // reclaimed kv got updated/deleted (rare)
                    //     // we need to update the deleteQ
                    //     delete_q_.erase(kvr_ctxs[i]->kv_ctx->pkey->get_seq());
                    //     dq_insert(pkeys[i].get_seq());                        
                    // }
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
            }
            // write code
            dev_idx = (dev_idx_start+k_) % (k_+r_);
            for (int j = 0; j < r_; j++) {
                int i = j+io_count;
                (void) new (&pkeys[i]) phy_key(sid_, group_id*k_*pack_size_);
                (void) new (&pvals[i]) phy_val(code[j], slab_size_*pack_size_);

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
        } while (total_count < bulk_count);
        
    }
}

void SlabQ::dq_insert(uint64_t index) {
    delete_q_.insert(index);
}

void SlabQ::DoTrim() {
    std::vector<uint64_t> groups;
    get_delete_ids(groups, MAX_TRIM_NUM, TRIM_GUARD_NUM);

    for (auto it = groups.begin(); it != groups.end(); ++it) {
        uint64_t group_id = *it;
        int dev_idx = group_id%(k_+r_);
        //physical trim
        for (int i = 0; i < k_; i++) {
            phy_key *phykey = new phy_key(sid_, group_id*k_ + i);
            delete_io_context *delete_io_ctx = new delete_io_context {dev_idx, phykey};
            parent_->ssds_[dev_idx].kv_adelete(phykey, on_delete_complete, delete_io_ctx);
            //printf("delete [ssd %d] group_id %d, phykey %d\n",dev_idx, group_id, phykey->get_seq());
            dev_idx = (dev_idx+1)%(k_+r_);
        }
        for (int i = 0; i < r_; i++) {
            phy_key *phykey = new phy_key(sid_, group_id*k_ );
            delete_io_context *delete_io_ctx = new delete_io_context {dev_idx, phykey};
            parent_->ssds_[dev_idx].kv_adelete(phykey, on_delete_complete, delete_io_ctx);
            //printf("delete [ssd %d] group_id %d, phykey %d\n",dev_idx, group_id, phykey->get_seq());
            dev_idx = (dev_idx+1)%(k_+r_);
        }
    }
}

void SlabQ::DoTrimAll() {
    std::vector<uint64_t> groups;
    get_all_delete_ids(groups);

    for (auto it = groups.begin(); it != groups.end(); ++it) {
        uint64_t group_id = *it;
        int dev_idx = group_id%(k_+r_);
        //physical trim
        for (int i = 0; i < k_; i++) {
            phy_key *phykey = new phy_key(sid_, group_id*k_ + i);
            delete_io_context *delete_io_ctx = new delete_io_context {dev_idx, phykey};
            parent_->ssds_[dev_idx].kv_adelete(phykey, on_delete_complete, delete_io_ctx);
            //printf("delete [ssd %d] group_id %d, phykey %d\n",dev_idx, group_id, phykey->get_seq());
            dev_idx = (dev_idx+1)%(k_+r_);
        }
        for (int i = 0; i < r_; i++) {
            phy_key *phykey = new phy_key(sid_, group_id*k_ );
            delete_io_context *delete_io_ctx = new delete_io_context {dev_idx, phykey};
            parent_->ssds_[dev_idx].kv_adelete(phykey, on_delete_complete, delete_io_ctx);
            //printf("delete [ssd %d] group_id %d, phykey %d\n",dev_idx, group_id, phykey->get_seq());
            dev_idx = (dev_idx+1)%(k_+r_);
        }
    }
}

void on_reclaim_get_complete(void *args) {
    reclaim_get_context* ctx = (reclaim_get_context*) args;
    kv_context* kv_ctx = new kv_context {ctx->pkey, ctx->pval};
    ctx->kvQ->enqueue(kv_ctx);

    delete ctx;
}

void SlabQ::DoReclaim() {
    std::vector<uint64_t> actives;
    std::vector<uint64_t> groups;
    int slab_size = slab_size_;

    // scan delete q
    delete_q_.scan(parent_->min_num_invalids_, actives, groups);
    if (groups.size() == 0) return;

    // DO reclaim
    int num_ios = actives.size();
    moodycamel::BlockingConcurrentQueue<kv_context *> kvQ;
    for (auto it = actives.begin(); it != actives.end(); ++it) {
        int dev_idx = get_dev_idx(*it);
        phy_key *pkey = new phy_key(sid_, *it);
        char *c_val = (char*)malloc(slab_size);
        phy_val *pval = new phy_val(c_val, slab_size);
        reclaim_get_context *aget_ctx = new reclaim_get_context {num_ios, pkey, pval, &kvQ};
        parent_->ssds_[dev_idx].kv_aget(pkey, pval, on_reclaim_get_complete, aget_ctx);
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
            mv_val->length = kvs[i]->pval->actual_len;
            mv_val->val = kvs[i]->pval->c_val;
            int pack_id = kvs[i]->pkey->get_seq() % pack_size_;
            new_unpack_value(pack_size_, pack_id, kvs[i]->pval->c_val, mv_key, NULL);
            kvr_context* kvr_ctx = new kvr_context(KVR_REPLACE, mv_key, mv_val, kvs[i]);
            q.enqueue(kvr_ctx);
            kvr_ctx_vec.push_back(kvr_ctx);
        }
        delete [] kvs;
        num_ios -= count;
    }

    // wait for all IO complete
    for (int i = 0; i < kvr_ctx_vec.size(); i++) {
        kvr_context *ack_kvr_ctx = kvr_ctx_vec[i];
        {
            std::unique_lock<std::mutex> lck(ack_kvr_ctx->mtx);
            while (!ack_kvr_ctx->ready) ack_kvr_ctx->cv.wait(lck);
        }
        // update mapping for KVR_REPLACE
        if (ack_kvr_ctx->ops == KVR_REPLACE) {
            std::string skey = std::string(ack_kvr_ctx->key->key, ack_kvr_ctx->key->length);
            // replace
            phy_key rd_pkey;
            bool match;
            match = parent_->key_map_->readtestupdate(&skey, &rd_pkey, ack_kvr_ctx->kv_ctx->pkey, &(ack_kvr_ctx->replace_key));
            if (!match) { // reclaimed kv got updated/deleted (rare)
                // we need to update the deleteQ
                int retry_cnt = 0;
                while (!delete_q_.erase(ack_kvr_ctx->kv_ctx->pkey->get_seq())) {
                    usleep(100);
                    if (++retry_cnt >= 3) break; // double update, direct insert replace to delete q
                }
                dq_insert(ack_kvr_ctx->replace_key.get_seq());                        
            }
        }
        

        free(kvr_ctx_vec[i]->kv_ctx->pval->c_val);
        delete kvr_ctx_vec[i]->kv_ctx->pkey;
        delete kvr_ctx_vec[i]->kv_ctx->pval;
        delete kvr_ctx_vec[i]->kv_ctx;
        delete kvr_ctx_vec[i];
    }

    // release phy key (for trim)
    add_delete_ids(groups);    
}

bool SlabQ::CheckGCTrigger() {
    // Three conditions to trigger GC, 
    // 1, Device usage to acual data volume ratio pass a certain threshold;
    // 2, Device utilization pass a certain threshold;
    // 3, delete_q_ size is too large (we want to keep the invalid-alive low)
    // return get_usage()/get_volume() > GC_DEV_USAGE_VOL_RATIO_THRES ||
    //     get_util() >= GC_DEV_UTIL_THRES || 
    //     slabs_[slab_id].dq_size() >= GC_DELETE_Q_THRES;
    
    return parent_->do_gc_.load();
}

void SlabQ::DoGC() {
    
    // 1, check trim list and do trim
    DoTrim();
    // 2, check GC trigger condition
    if (CheckGCTrigger()) {
        DoReclaim();
    }
    
}

void SlabQ::bg_GC() {
    const auto timeWindow = std::chrono::milliseconds(200);

    while(true)
    {
        // check thread shutdown
        {
            std::unique_lock<std::mutex> lck (gc_thread_m_);
            if (gc_shutdown_ == true) break;
        }
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
    // clean up delete_seq_
    // trim all delete_seq_
    DoTrimAll();
}

bool KVRaidPack::CheckGCTrigger() {
    // Three conditions to trigger GC, 
    // 1, Device usage to acual data volume ratio pass a certain threshold;
    // 2, Device utilization pass a certain threshold;
    // 3, delete_q_ size is too large (we want to keep the invalid-alive low)
    // return get_usage()/get_volume() > GC_DEV_USAGE_VOL_RATIO_THRES ||
    //     get_util() >= GC_DEV_UTIL_THRES || 
    //     slabs_[slab_id].dq_size() >= GC_DELETE_Q_THRES;
    
    return get_util() >= GC_DEV_UTIL_THRES;
}

void KVRaidPack::bg_check() {
    const auto timeWindow = std::chrono::milliseconds(500);

    while(true)
    {
        // check thread shutdown
        {
            std::unique_lock<std::mutex> lck (bg_thread_m_);
            if (bg_shutdown_ == true) break;
        }
        auto start = std::chrono::steady_clock::now();
        do_gc_.store(CheckGCTrigger());
        auto end = std::chrono::steady_clock::now();
        auto elapsed = end - start;

        auto timeToWait = timeWindow - elapsed;
        if(timeToWait > std::chrono::milliseconds::zero())
        {
            std::this_thread::sleep_for(timeToWait);
        }
    }
}

int KVRaidPack::kvr_get_slab_id(int size) {
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

bool KVRaidPack::kvr_insert(kvr_key *key, kvr_value *value) {
    uint32_t actual_vlen = key->length + value->length + KEY_SIZE_BYTES + VAL_SIZE_BYTES;
    int slab_id = kvr_get_slab_id(actual_vlen);
    SlabQ *slab = &slabs_[slab_id];

    // generate new value 
    char *pack_val = (char*)malloc(actual_vlen);
    pack_value(pack_val, key, value);
    kvr_value new_value = {pack_val, actual_vlen};

    // write to the context queue
    kvr_context kvr_ctx(KVR_INSERT, key, &new_value);
    slab->q.enqueue(&kvr_ctx);

    //wait for IOs finish
    {
        std::unique_lock<std::mutex> lck(kvr_ctx.mtx);
        while (!kvr_ctx.ready) kvr_ctx.cv.wait(lck);
    }
    free(pack_val);
    return true;
}
bool KVRaidPack::kvr_update(kvr_key *key, kvr_value *value) {
    uint32_t actual_vlen = key->length + value->length + KEY_SIZE_BYTES + VAL_SIZE_BYTES;
    int slab_id = kvr_get_slab_id(actual_vlen);
    SlabQ *slab = &slabs_[slab_id];

    // generate new value 
    char *pack_val = (char*)malloc(actual_vlen);
    pack_value(pack_val, key, value);
    kvr_value new_value = {pack_val, actual_vlen};

    std::string skey = std::string(key->key, key->length);
    //LockEntry *l = req_key_fl_.Lock(skey);

    // write to the context queue
    kvr_context kvr_ctx(KVR_UPDATE, key, &new_value);
    slab->q.enqueue(&kvr_ctx);

    //wait for IOs finish
    {
        std::unique_lock<std::mutex> lck(kvr_ctx.mtx);
        while (!kvr_ctx.ready) kvr_ctx.cv.wait(lck);
    }

    //req_key_fl_.UnLock(skey, l);

    free(pack_val);
    return true;
}
bool KVRaidPack::kvr_delete(kvr_key *key) {
    std::string skey = std::string(key->key, key->length);
    //LockEntry *l = req_key_fl_.Lock(skey);
    
    phy_key pkey;
    // update log->phy translation table
    bool exist = key_map_->lookup(&skey, &pkey);
    if (!exist) {
        printf("[KVRaidPack::kvr_delete] logical key not exist\n");
        exit(-1);
    }
    key_map_->erase(&skey);

    // insert to delete queue
    int slab_id = pkey.get_slab_id();
    SlabQ *slab = &slabs_[slab_id];
    slab->dq_insert(pkey.get_seq());

    //req_key_fl_.UnLock(skey, l);

    return true;

}
bool KVRaidPack::kvr_get(kvr_key *key, kvr_value *value) {
    std::string skey = std::string(key->key, key->length);
    //LockEntry *l = req_key_fl_.Lock(skey);

    phy_key pkey;
    // lookup log->phy translation table
    bool exist = key_map_->lookup(&skey, &pkey);
    
    if (!exist) {
        printf("[KVRaidPack::kvr_get] logical key not exist\n");
        exit(-1);
    }
    
    int slab_id = pkey.get_slab_id();
    int pack_size = slabs_[slab_id].pack_size_;
    int seq = pkey.get_seq();
    int pack_id = seq%pack_size;
    int dev_idx = ((seq/k_/pack_size % (k_+r_)) + (seq%(k_*pack_size)/pack_size)) % (k_+r_);

    char *actual_val = (char*)malloc(slab_list_[slab_id]);
    phy_val pval(actual_val, slab_list_[slab_id]);
    //printf("get [ssd %d] skey %s, pkey %lu\n",dev_idx, skey.c_str(), pkey.get_seq());
    pkey.phykey = pkey.phykey - pack_id;
    exist = ssds_[dev_idx].kv_get(&pkey, &pval);
    if (!exist) {
        value->length = 0;
        return false;
    }

    //req_key_fl_.UnLock(skey, l);

    kvr_value new_val;
    //unpack_value(pval.c_val, NULL, &new_val);
    new_unpack_value(pack_size, pack_id, pval.c_val, NULL, &new_val);
    if (new_val.length == 0) {
        int test = 1;
    }

    value->length = new_val.length;
    value->val = (char*)malloc(new_val.length);
    memcpy(value->val, new_val.val, new_val.length);

    free(actual_val);

    return true;
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

bool KVRaidPack::kvr_erased_get(int erased, kvr_key *key, kvr_value *value) {
    std::string skey = std::string(key->key, key->length);
    phy_key pkey;
    // lookup log->phy translation table
    bool exist = key_map_->lookup(&skey, &pkey);
    
    if (!exist) {
        printf("[KVRaidPack::kvr_get] logical key not exist\n");
        exit(-1);
    }
    
    int slab_id = pkey.get_slab_id();
    int pack_size = slabs_[slab_id].pack_size_;
    int seq = pkey.get_seq();
    int pack_id = seq%pack_size;
    int dev_idx = ((seq/k_/pack_size % (k_+r_)) + (seq%(k_*pack_size)/pack_size)) % (k_+r_);

    if (dev_idx == erased) {
        int group_id = seq/k_/pack_size;
        int group_offset = seq%(k_*pack_size);
        int code_size_ = slab_list_[slab_id] * pack_size;
        int dev_index = group_id % (k_+r_);

        // decode buffers
        char **data = (char **)malloc(k_*sizeof(char *));
        char **codes = (char **)malloc(r_*sizeof(char *));
        for (int i = 0; i < k_; i++) data[i] = (char *)calloc(code_size_, 1);
        for (int i = 0; i < r_; i++) codes[i] = (char *)calloc(code_size_, 1);

        // read survival data and codes
        phy_key *pkeys_c = (phy_key *)malloc(sizeof(phy_key)*(k_+r_-1));
        phy_val *pvals_c = (phy_val *)malloc(sizeof(phy_val)*(k_+r_-1));
        Monitor *mons_c = new Monitor[k_+r_-1];
        int j = 0;
        int logic_erased;
        for (int i = 0; i < (k_+r_); i++) {
            if (dev_index != dev_idx) {
                if (i < k_) {
                    (void) new (&pkeys_c[j]) phy_key(slab_id, (group_id*k_+i)*pack_size);
                    (void) new (&pvals_c[j]) phy_val(data[i], code_size_);
                }
                else {
                    (void) new (&pkeys_c[j]) phy_key(slab_id, group_id*k_*pack_size);
                    (void) new (&pvals_c[j]) phy_val(codes[i-k_], code_size_);
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
        ec_.single_failure_decode(logic_erased, data, codes, code_size_);
        kvr_value new_val;
        new_unpack_value(pack_size, pack_id, data[logic_erased], NULL, &new_val);
        value->length = new_val.length;
        value->val = (char*)malloc(new_val.length);
        memcpy(value->val, new_val.val, new_val.length);

        for (int i = 0; i < k_; i++) free(data[i]);
        for (int i = 0; i < r_; i++) free(codes[i]);
        free(data);
        free(codes);
        free(pkeys_c);
        free(pvals_c);
    }
    else {
        char *actual_val = (char*)malloc(slab_list_[slab_id]*slabs_[slab_id].pack_size_);
        pkey.phykey = pkey.phykey - pack_id;
        phy_val pval(actual_val, slab_list_[slab_id]*slabs_[slab_id].pack_size_);
        //printf("get [ssd %d] skey %s, pkey %lu\n",dev_idx, skey.c_str(), pkey.get_seq());
        ssds_[dev_idx].kv_get(&pkey, &pval);

        kvr_value new_val;
        new_unpack_value(pack_size, pack_id, pval.c_val, NULL, &new_val);

        value->length = new_val.length;
        value->val = (char*)malloc(new_val.length);
        memcpy(value->val, new_val.val, new_val.length);

        free(actual_val);
    }
    return true;
}

bool KVRaidPack::kvr_write_batch(WriteBatch *batch) {
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
        kvr_value *new_value = new kvr_value{pack_val, actual_vlen};

        // write to the context queue
        kvr_context *kvr_ctx = new kvr_context(KVR_OPS(it->type), key, new_value);
        kvr_ctx_vec.push_back(kvr_ctx);
        slab->q.enqueue(kvr_ctx);
    }

    // wait util whole batch is done
    for (auto it = kvr_ctx_vec.begin(); it != kvr_ctx_vec.end(); ++it) {
        std::unique_lock<std::mutex> lck((*it)->mtx);
        while (!(*it)->ready) (*it)->cv.wait(lck);
        free((*it)->value->val); // pack_val
        delete (*it)->value;
        delete (*it);
    }

}


void KVRaidPack::KVRaidPackIterator::retrieveValue(int userkey_len, std::string &retrieveKey, std::string &value) {
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

    kvr_value new_val;
    unpack_value(pval.c_val, NULL, &new_val);

    value.clear();
    value.append(new_val.val, new_val.length);
    free(actual_val);
}

void KVRaidPack::save_meta() {
    std::string meta_key = "KVRaidPack_meta";
    std::string meta_val;
    meta_val.append((char *)&num_slab_, sizeof(num_slab_)); // num_slabs
    for (int i = 0; i < num_slab_; i++) {
        uint64_t seq = slabs_[i].get_curr_group_id();
        meta_val.append((char *)&seq, sizeof(uint64_t)); // group_id per slab
    }
    for (int i = 0; i < r_; i++)
        ssds_[i].kv_store(&meta_key, &meta_val); // mirror to num_r devs
}
    
bool KVRaidPack::load_meta(uint64_t *arr, int size) {
    std::string meta_key = "KVRaidPack_meta";
    std::string meta_val;
    ssds_[0].kv_get(&meta_key, &meta_val); // only access dev_0;
    if (meta_val.size() == 0) {
        for (int i = 0; i < size; i++) arr[i] = 0;
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
        arr[i] = *(uint64_t *)p;
        p += sizeof(uint64_t);
    }
    return true;
}


} // end namespace kvraid_pack

KVR *NewKVRaidPack(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t, bool GC_ENA) {
    return new kvraid_pack::KVRaidPack(num_d, num_r, num_slab, s_list, conts, meta_t, GC_ENA);
}