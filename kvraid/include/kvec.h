/* kvec.h
* 06/23/2019
* by Mian Qin
*/

#ifndef   _kvec_h_   
#define   _kvec_h_   

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <map>
#include <unordered_map>
#include <new>
#include <queue>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>

#include "kvr_api.h"
#include "kvr.h"
#include "ec.h"
#include "kv_device.h"
#include "mapping_table.h"

namespace kvec {

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

class BitMap
{
public:
  uint8_t *bm;
  uint64_t size_;
  BitMap(uint64_t size) : size_(size) {
    bm = (uint8_t *)calloc(size, sizeof(uint8_t));
  }
  BitMap() {BitMap(1024);}
  ~BitMap() {free(bm);}

  void resize(uint64_t size) {
    bm = (uint8_t *)realloc(bm, size);
    memset(bm + size/2, 0, size / 2);
    size_ = size;
  }

  void set(uint64_t index) {
    while (index/8 >= size_ - 1) resize(size_*2);
    bm[index / 8] = bm[index / 8] | (1 << (index % 8));
  }
  void reset(uint64_t index) {
    while (index/8 >= size_ - 1) resize(size_*2);
    bm[index / 8] = bm[index / 8] & (~(1 << (index % 8)));
  }

  bool get(uint64_t index) {
    return ((bm[index / 8] & (1 << (index % 8))) != 0);
  }
};

class KVEC; // forward declaration
class SlabQ {
    friend class KVEC;
private:
    KVEC *parent_;
    int sid_;
    // slab info
    int slab_size_;
    int k_;
    int r_;
    // ec 
    EC *ec_;
    // fine lock (on group id granularity)
    FineLock<uint64_t> fl_;
    // seq generator
    uint64_t seq_;
    std::queue<uint64_t> avail_seq_; 
    std::mutex seq_mutex_;

    pthread_t t_PQ;

    void get_index_id(uint64_t *index);
    uint64_t get_curr_seq() {return seq_;}
    void reclaim_index(uint64_t index);
    bool slab_insert(kvr_key *key, kvr_value *value);
    bool slab_update(kvr_value *value, phy_key *pkey);
    bool slab_delete(kvr_key *key, phy_key *pkey);

public:
    // bit map for group_id
    BitMap group_occu_;
    std::mutex bm_mutex_;

    int get_id() {return sid_;}
    SlabQ(KVEC *p, int id, int size, int num_d, int num_r, EC *ec) : 
    parent_(p), sid_(id), slab_size_(size), k_(num_d), r_(num_r), ec_(ec), seq_(0), group_occu_(1024) {

    }
    ~SlabQ() {
    }

    int get_dev_idx (uint64_t group_id, int offset) {
        return ((group_id%(k_+r_)) + offset) % (k_+r_);
    }
};

class KVEC : public KVR {
    friend class SlabQ;
private:
	int k_; // number of data nodes
    int r_;  // number of parity 
	int num_slab_;
	int *slab_list_;

    // device
    KV_DEVICE *ssds_;

    // erasure code
    EC ec_;
    
	// key index
	Map *key_map_; 

    // slabs
    SlabQ *slabs_;
	int kvr_get_slab_id(int size);

    // finelock on user key
    FineLock<std::string> req_key_fl_;

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

    void save_meta();
    bool load_meta(int size);
    
public:
	KVEC(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t) :
    k_(num_d), r_(num_r), num_slab_(num_slab), ec_(num_d,num_r), data_volume_(0){
		slab_list_ = new int[num_slab];
        slabs_ = (SlabQ *)malloc(sizeof(SlabQ)*num_slab);
        ec_.setup();

        ssds_ = (KV_DEVICE *)malloc(sizeof(KV_DEVICE) * (k_+r_));
        for (int i = 0; i < (k_+r_); i++) {
            (void) new(&ssds_[i]) KV_DEVICE(i, &conts[i], 4, 64);
        }

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

        for (int i = 0; i < num_slab; i++) {
            slab_list_[i] = s_list[i];
            (void) new (&slabs_[i]) SlabQ(this, i, s_list[i], k_, r_, &ec_);
        }
        
        // restore slab state
        bool newdb = !load_meta(num_slab);
        if (newdb) {
            printf("Clean KVEC initialized\n");
        }
        else {
            printf("Restore existing KVEC\n");
        }

        // restore bitmap in slabQ
        MapIterator *it = key_map_->NewMapIterator();
        for (it->SeekToFirst();it->Valid();it->Next()) {
            std::string retrieveKey = it->Value();
            phy_key pkey;
            pkey.decode(&retrieveKey);
            slabs_[pkey.get_slab_id()].group_occu_.set(pkey.get_seq()/k_);
        }
	}

	~KVEC() {
        // save KVEC state
        save_meta();
        for (int i = 0; i < (k_+r_); i++) {
            ssds_[i].~KV_DEVICE();
        }
        free(ssds_);
		delete[] slab_list_;
        for (int i = 0; i < num_slab_; i++) {
            slabs_[i].~SlabQ();
        }
        free(slabs_);
        delete key_map_;
	}

public:
	bool kvr_insert(kvr_key *key, kvr_value *value);
	bool kvr_update(kvr_key *key, kvr_value *value);
    bool kvr_delete(kvr_key *key);
	bool kvr_get(kvr_key *key, kvr_value *value);
    bool kvr_erased_get(int erased, kvr_key *key, kvr_value *value);

    bool kvr_write_batch(WriteBatch *batch);

    class KVECIterator : public Iterator {
    private:
        KVEC *kvr_;
        MapIterator *it_;
        std::string curr_key_;
        std::string curr_val_;
        bool val_retrieved_;
    public:
        KVECIterator(KVEC *kvr) : kvr_(kvr), val_retrieved_(false) {
            it_ = kvr_->key_map_->NewMapIterator();
        }
        ~KVECIterator() {delete it_;}
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
        return new KVECIterator(this);
    }
};

} // end namespace kvec

#endif 