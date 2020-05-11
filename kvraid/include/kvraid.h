/* kvraid.h
* 04/23/2019
* by Mian Qin
*/

#ifndef   _kvraid_h_   
#define   _kvraid_h_   

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <new>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>

#include "kvr_api.h"
#include "kvr.h"
#include "ec.h"
#include "blockingconcurrentqueue.h"
#include "kv_device.h"
#include "mapping_table.h"

#define RD_IO_RETRY_TIMEOUT 150

#define MAX_ENTRIES_PER_GC 8192
#define MAX_SCAN_LEN_PER_GC 524288

#define GC_MIN_INVALID_BIAS 0

namespace kvraid {

typedef struct {
    phy_key* pkey;
    phy_val* pval;
} kv_context;
typedef struct {
    int num_ios;
    phy_key* pkey;
    phy_val* pval;
    moodycamel::BlockingConcurrentQueue<kv_context*> *kvQ;
} reclaim_get_context;

enum KVR_OPS { 
    KVR_INSERT, 
    KVR_UPDATE,
    KVR_REPLACE
};

class kvr_context {
public:
    KVR_OPS ops; // 0-insert, 1-update, 2-replace
    kvr_key *key;
    kvr_value *value;
    kv_context *kv_ctx;
    phy_key replace_key;
    std::mutex mtx;
    std::condition_variable cv;
    bool ready ;
    kvr_context(KVR_OPS ops, kvr_key *k, kvr_value *v):
    ops(ops), key(k), value(v), kv_ctx(NULL), ready(false) {}
    kvr_context(KVR_OPS ops, kvr_key *k, kvr_value *v, kv_context *pkv):
    ops(ops), key(k), value(v), kv_ctx(pkv), ready(false) {}
    ~kvr_context() {}
} ;

class LockEntry {
private:
    std::mutex m_;
    int ref_cnt_;
public:
    LockEntry() : ref_cnt_(0) {};
    ~LockEntry() {};
    void Ref() {ref_cnt_++;}
    bool UnRef() {
        ref_cnt_--;
        return ref_cnt_ == 0;
    }
    void Lock() {m_.lock();}
    void UnLock() {m_.unlock();}
};

template <class T>
class FineLock {
private:
    std::mutex m_;
    std::unordered_map<T, LockEntry *> lock_map_;
public:
    FineLock (){};
    ~FineLock (){};
    LockEntry* Lock (T id) {
        LockEntry *l;
        {
            std::unique_lock<std::mutex> lock(m_);
            auto it = lock_map_.find(id);
            if(it == lock_map_.end()) {
                l = new LockEntry;
                lock_map_[id] = l;
                l->Ref();
            }
            else {
                l = it->second;
                l->Ref();
            }
        }

        l->Lock();
        return l;
    }
    void UnLock (T id, LockEntry* l) {
        l->UnLock();
        {
            std::unique_lock<std::mutex> lock(m_);
            if (l->UnRef() == true) {
                delete l;
                lock_map_.erase(id);
            }
        }
    }
};

class SlabQ; // forward declare
class DeleteQ
{
public:
    SlabQ* parent_;
    int k_;
    int group_size_;
    std::unordered_map<uint64_t,std::vector<uint8_t>> group_list_;
    std::mutex gl_mutex_;
    
    DeleteQ() {group_list_.reserve(20e6);}
    DeleteQ(SlabQ* p, int k, int m) : parent_(p), k_(k), group_size_(k_) {}
    ~DeleteQ(){
        int total_invalid = 0;
        int cnt4=0,cnt3=0,cnt2=0,cnt1=0;
        for (auto it = group_list_.begin(); it != group_list_.end(); ++it) {
                switch (it->second.size()) {
                case 1: cnt1++;total_invalid+=1;break;
                case 2: cnt2++;total_invalid+=2;break;
                case 3: cnt3++;total_invalid+=3;break;
                case 4: cnt4++;total_invalid+=4;break;
                default: break;
                }
            //for (int i = 0; i < it->second.size(); i++) total_invalid++;
        }
        FILE *fd = fopen("kv_device.log","a");
        fprintf(fd, "invalid-alive %d, IA4 %d, IA3 %d, IA2 %d, IA1 %d\n", total_invalid, cnt4, cnt3, cnt2, cnt1);
        fclose(fd);
    }
    void insert(uint64_t index);
    void scan (int min_num_invalids, std::vector<uint64_t>& reclaims, 
    std::vector<uint64_t>& groups);
    bool erase(uint64_t index);
};

class SlabQ;
typedef struct {
    uint64_t id;
    int num_ios;
    int bulk_size;
    kvr_context **kvr_ctxs;
    phy_key *keys;
    phy_val *vals;
    int k;
    int r;
    char **data_buf;
    char **code_buf;
    bool free_buf;
    SlabQ *q;
} bulk_io_context;

typedef struct {
    int dev_idx;
    phy_key *key;
} delete_io_context;

class KVRaid; // forward declaration
class SlabQ {
private:
    int sid_;
    // slab info
    int slab_size_;
    int k_;
    int r_;
    // ec engine
    EC *ec_;
    // seq generator
    uint64_t seq_; // monotonous for recovery
    std::mutex seq_mutex_;
    std::queue<uint64_t> delete_seq_;  // seq before trim
    std::mutex dseq_mutex_;

    // process request Q
    int num_pq_;
    std::mutex *thread_m_;
    bool *shutdown_;
    std::thread **thrd_;

    // GC thread
    std::thread gc_thrd_;
    std::mutex gc_thread_m_;
    bool gc_ena_;
    bool gc_shutdown_;

    // Fine grind locking mechanism for active KV being GCed
    std::atomic<bool> gc_on_; // coarse grind indication for kvr_get whether slab is working on GC
    std::unordered_set<uint64_t> replace_keys_; // active keys being GC (replace)
    // CV to for locking kvr_get
    std::mutex gc_lock_mtx_;
    std::condition_variable gc_lock_cv_;
    bool gc_lock_ready_ ;

    void bg_GC();
    void DoGC();
    void DoTrim();
    void DoTrimAll();
    void DoReclaim();
    bool CheckGCTrigger();

    // track bulk io finish
    std::unordered_map<int,int> finish_;
    std::mutex finish_mtx_;

public:
    KVRaid *parent_;
    // Delete queue
    DeleteQ delete_q_;

    moodycamel::BlockingConcurrentQueue<kvr_context*> q;
    int get_id() ;
    SlabQ(KVRaid *p, int id, int size, int num_d, int num_r, EC *ec, uint64_t seq, int num_pq, bool GC_ENA) : 
    parent_(p), sid_(id), slab_size_(size), k_(num_d), r_(num_r), 
    ec_(ec), seq_(seq), num_pq_(num_pq), delete_q_(this, num_d, num_d+num_r),
    gc_ena_(GC_ENA) {

        // thread processQ thread
        thrd_ = new std::thread*[num_pq];
        thread_m_ = new std::mutex[num_pq];
        shutdown_ = new bool[num_pq];
        for (int i = 0; i < num_pq; i++) {
            shutdown_[i] = false;
            thrd_[i] = new std::thread(&SlabQ::processQ, this, i);
            //thrd_[i]->detach();
        }

        // GC thread
        gc_shutdown_ = false;
        if(gc_ena_) gc_thrd_ = std::thread(&SlabQ::bg_GC, this);
        //t_GC = thrd.native_handle();
        //thrd.detach();

        // GC lock
        gc_on_.store(false);
        gc_lock_ready_ = false;

    }
    ~SlabQ() {
        delete [] thrd_;
        delete [] shutdown_;
        delete [] thread_m_;
    }
    void processQ(int id);
    void get_all_delete_ids(std::vector<uint64_t>& groups);
    void get_delete_ids(std::vector<uint64_t>& groups, int trim_num, int guard_num);
    void add_delete_ids(std::vector<uint64_t>& groups);
    void add_delete_id(uint64_t group_id);
    uint64_t get_new_group_id();
    uint64_t get_curr_group_id();
    bool track_finish(int id, int num_ios);
    void dq_insert(uint64_t index);

    int get_dev_idx (uint64_t seq) {
        uint64_t group_id = seq/k_;
        int id = seq%k_;
        return ((group_id%(k_+r_)) + id) % (k_+r_);
    }
    void shutdown_workers() {
        for (int i = 0; i < num_pq_; i++) { 
            {
                std::unique_lock<std::mutex> lck (thread_m_[i]);
                shutdown_[i] = true;
            }
            thrd_[i]->join();
            delete thrd_[i];
        }
    }
    void shutdown_gc() {
        {
            std::unique_lock<std::mutex> lck (gc_thread_m_);
            gc_shutdown_ = true;
        }
        if (gc_ena_) gc_thrd_.join();
    }

    bool wait_for_GC(uint64_t seq) {
        if (!gc_on_.load()) return false;
        {
            std::unique_lock<std::mutex> lk(gc_lock_mtx_);
            while (!gc_lock_ready_)
                gc_lock_cv_.wait(lk);
        }
        return true; // wait on GC, re-lookup mapping table
    }
};


class KVRaid : public KVR {
    friend class SlabQ;
public:
	// key index
	Map *key_map_; 
    // slabs
    SlabQ *slabs_;
private:
	int k_; // number of data nodes
    int r_;  // number of parity 
	int num_slab_;
	int *slab_list_;

    // device
    KV_DEVICE *ssds_;

    // erasure code
    EC ec_;
    
    // finelock on request key
    FineLock<std::string> req_key_fl_;

    int min_num_invalids_;

	int kvr_get_slab_id(int size);
    int process_slabq();

    // bg thread to check dev utilization
    std::thread bg_thrd_;
    std::mutex bg_thread_m_;
    bool bg_shutdown_;
    std::atomic<bool> do_gc_;

    void bg_check();
    bool CheckGCTrigger();
    void shutdown_bg() {
        {
            std::unique_lock<std::mutex> lck (bg_thread_m_);
            bg_shutdown_ = true;
        }
        bg_thrd_.join();
    }

    // data volume info
    std::atomic<int64_t> data_volume_; //estimate data vol, not accurate
    int64_t get_volume() {return data_volume_.load();}

    // dev_info
    int64_t get_capacity() {return ssds_[0].get_capacity();}
    int64_t get_log_capacity() {return ssds_[0].get_log_capacity();}
    int64_t get_usage(int i) {return int64_t(ssds_[i].get_util()*get_capacity());}
    double get_util() {
        return (double)get_usage(0)/get_log_capacity();
    }
    float get_waf() {
        double waf = 0;
        for(int i = 0; i<k_+r_; i++) waf += ssds_[i].get_waf();
        return waf/(k_+r_);
    }

    // meta data (slabQ seq) serialiazation
    void save_meta();
    bool load_meta(uint64_t *arr, int size);
    
public:
	KVRaid(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t, bool GC_ENA, Cache *c) :
    KVR(c), k_(num_d), r_(num_r), num_slab_(num_slab), ec_(num_d,num_r), do_gc_(false), data_volume_(0){
		slab_list_ = new int[num_slab];
        slabs_ = (SlabQ *)malloc(sizeof(SlabQ)*num_slab);
        ec_.setup();

        ssds_ = (KV_DEVICE *)malloc(sizeof(KV_DEVICE) * (k_+r_));
        for (int i = 0; i < (k_+r_); i++) {
            (void) new(&ssds_[i]) KV_DEVICE(i, &conts[i], 4, 64);
        }
        // get KVRaid meta
        uint64_t *slab_seq = new uint64_t[num_slab];
        bool newdb = !load_meta(slab_seq, num_slab);
        if (newdb) {
            printf("Clean KVRaid initialized\n");
        }
        else {
            printf("Restore existing KVRaid\n");
        }

        for (int i = 0; i < num_slab; i++) {
            slab_list_[i] = s_list[i];
            (void) new (&slabs_[i]) SlabQ(this, i, s_list[i], k_, r_, &ec_, slab_seq[i], 1, GC_ENA);
            usleep(10000);
        }
        delete [] slab_seq;

        switch (meta_t) {
            case Mem:
                key_map_= NewMemMap();
                break;
            case Storage:
                key_map_ = NewStorageMap(conts, k_, r_);
                break;
            case Cuckoo:
                key_map_ = NewCuckooMap();
                break;
            default:
                printf("wrong MetaType \n");
                exit(-1);
        }

        // calculate GC cost function 
        // (k+r)/k + (k-x)/x*(k+r)/k <= (r+1)
        min_num_invalids_ = k_-1;
        for (int i = k_-1; i>0; i--) {
            if ((r_+1)*i > (k_+r_)) min_num_invalids_ = i;
            else break;
        }
        min_num_invalids_ -= GC_MIN_INVALID_BIAS;  

        // BG check thread
        bg_shutdown_ = false;
        bg_thrd_ = std::thread(&KVRaid::bg_check, this);

	}

	~KVRaid() {
        
        // shutdown slab workers
        for (int i = 0; i < num_slab_; i++) {
            slabs_[i].shutdown_workers();
            slabs_[i].shutdown_gc();
        }
        shutdown_bg();
        // save KVRaid state
        save_meta();

        // clean up slab queues
		delete[] slab_list_;
        for (int i = 0; i < num_slab_; i++) {
            slabs_[i].~SlabQ();
        }
        free(slabs_);
        for (int i = 0; i < (k_+r_); i++) {
            ssds_[i].~KV_DEVICE();
        }
        free(ssds_);
        delete key_map_;
	}

public:
	bool kvr_insert(kvr_key *key, kvr_value *value);
	bool kvr_update(kvr_key *key, kvr_value *value);
    bool kvr_delete(kvr_key *key);
	bool kvr_get(kvr_key *key, kvr_value *value);
    bool kvr_erased_get(int erased, kvr_key *key, kvr_value *value);

    bool kvr_write_batch(WriteBatch *batch);
    // void kvr_stats(double &slab_overhead, double &occup_capacity, double &invalid_capacity);
    // void kvr_gc_stats(double &gc_efficiency);

public:
    class KVRaidIterator : public Iterator {
    private:
        KVRaid *kvr_;
        MapIterator *it_;
        std::string curr_key_;
        std::string curr_val_;
        bool val_retrieved_;
    public:
        KVRaidIterator(KVRaid *kvr) : kvr_(kvr), val_retrieved_(false) {
            it_ = kvr_->key_map_->NewMapIterator();
        }
        ~KVRaidIterator() {delete it_;}
        void Seek(kvr_key &key) {
            std::string seekkey(key.key, key.length);
            it_->Seek(seekkey);
            val_retrieved_ = false;
            if (it_->Valid()) 
                curr_key_ = it_->Key();
        }
        void SeekToFirst() {
            it_->SeekToFirst();
            val_retrieved_ = false;
            if (it_->Valid())
                curr_key_ = it_->Key();
        }
        void Next() {
            it_->Next();
            val_retrieved_ = false;
            if (it_->Valid())
                curr_key_ = it_->Key();
        }
        bool Valid() {return it_->Valid();}
        kvr_key Key() {
            return {(char *)curr_key_.data(), (uint8_t)curr_key_.size()};
        }
        kvr_value Value() {
            if (!val_retrieved_) {
                retrieveValue(it_->Key().size(), it_->Value(), curr_val_);
                val_retrieved_ = true;
            }
            return {(char *)curr_val_.data(), (uint32_t)curr_val_.size()};
        }
        void retrieveValue(int userkey_len, std::string &retrieveKey, std::string &value);
    };
    Iterator* NewIterator() {
        return new KVRaidIterator(this);
    }
};

} // end namespace kvraid


#endif 
