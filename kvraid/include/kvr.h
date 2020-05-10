/* kvr.h
* 07/07/2019
* by Mian Qin
*/

#ifndef   _kvr_h_   
#define   _kvr_h_   

#include <stdint.h>
#include "kvr_api.h"
#include "kv_writebatch.h"
#include "kv_device.h"
#include "mapping_table.h"
#include "cache/cache.h"

typedef struct {
    std::atomic<uint64_t> hit{0};
    std::atomic<uint64_t> miss{0};
    std::atomic<uint64_t> fill{0};
    std::atomic<uint64_t> erase{0};
} cache_stats;

class Iterator {
public:
    Iterator() {};
    virtual ~Iterator() {};
    virtual void Seek(kvr_key &key) = 0;
    virtual void SeekToFirst() = 0;
    virtual void Next() = 0;
    virtual bool Valid() = 0;
    virtual kvr_key Key() = 0;
    virtual kvr_value Value() = 0;
};

enum CacheType {
  LRU,
  LFU,
};

class CacheEntry { 
public:
    char *val;
    int size;
    CacheEntry() : val(NULL), size(0){};
    CacheEntry(char *v, int s) : size(s) {
        val = (char *)malloc(s);
        memcpy(val, v, size);
    }
    ~CacheEntry() {if(val) free(val);}
};

template <class T>
static void DeleteEntry(const Slice& /*key*/, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
}

class KVR {
public:
    cache_stats stats_;
    Cache *cache_;
public:
    // choose either constructor
    // KVR() {cache_ = NewLRUCache(2048 << 20, 0);} // default constructor
    KVR() : cache_(NULL) {};
    KVR(Cache *c) : cache_(c) {}; 
    // KVR(CacheType t, size_t capacityMB, int shard_bits) {
    //     switch (t) {
    //         case LRU:
    //             cache_ = NewLRUCache(capacityMB << 20, shard_bits);
    //             break;
    //         case LFU:
    //             cache_ = NewLFUCache(capacityMB << 20, shard_bits);
    //             break;
    //         default:
    //             cache_ = NewLRUCache(capacityMB << 20, shard_bits);
    //     }
    // };
    virtual ~KVR() {
        printf("cache hit: %ld\n",stats_.hit.load());
        printf("cache miss: %ld\n",stats_.miss.load());
        printf("cache fill: %ld\n",stats_.fill.load());
        printf("cache erase: %ld\n",stats_.erase.load());
    };

    // define KVR interface
    virtual bool kvr_insert(kvr_key *key, kvr_value *value) = 0;
	virtual bool kvr_update(kvr_key *key, kvr_value *value) = 0;
    virtual bool kvr_delete(kvr_key *key) = 0;
	virtual bool kvr_get(kvr_key *key, kvr_value *value) = 0;
    
    // KV cache interface
	virtual Cache::Handle* kvr_read_cache(std::string& key, kvr_value *value) {
        if (cache_==NULL) return NULL;
        Cache::Handle *h = cache_->Lookup(key);
        if (h != NULL) {
            CacheEntry *rd_val = reinterpret_cast<CacheEntry*>(cache_->Value(h));
            value->length = rd_val->size;
            value->val = (char*)malloc(value->length);
            memcpy(value->val, rd_val->val, value->length);
            stats_.hit.fetch_add(1, std::memory_order_relaxed);
        }
        else 
            stats_.miss.fetch_add(1, std::memory_order_relaxed);
        return h;
    };
    virtual Cache::Handle* kvr_insert_cache(std::string& key, kvr_value *value) {
        if (cache_==NULL) return NULL;
        CacheEntry *ins_val = new CacheEntry(value->val, value->length);
        size_t charge = sizeof(CacheEntry) + value->length;
        Cache::Handle *h = cache_->Insert(key, reinterpret_cast<void*>(ins_val), charge, DeleteEntry<CacheEntry*>);

        stats_.fill.fetch_add(1, std::memory_order_relaxed);
        return h;
    };
    virtual void kvr_erase_cache(std::string& key) {
        if (cache_==NULL) return ;
        bool evicted = cache_->Erase(key);

        if (evicted) stats_.erase.fetch_add(1, std::memory_order_relaxed);
    };
    virtual void kvr_release_cache(Cache::Handle* h) {
        if (cache_==NULL) return ;
        cache_->Release(h);
    };

    virtual bool kvr_write_batch(WriteBatch *batch) = 0;
    
    virtual bool kvr_erased_get(int erased, kvr_key *key, kvr_value *value) = 0;

    virtual Iterator* NewIterator() = 0;
};

KVR* NewKVRaid(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t, bool GC_ENA, Cache *c);
KVR* NewKVEC(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t, Cache *c);
KVR* NewKVMirror(int num_d, int num_r, KVS_CONT *conts, Cache *c);
KVR* NewKVRaidPack(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t, bool GC_ENA, Cache *c);

KVR* NewKVDummy(int num_d, KVS_CONT *conts);
#endif