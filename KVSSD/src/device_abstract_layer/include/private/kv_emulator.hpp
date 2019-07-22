/**
 *   BSD LICENSE
 *
 *   Copyright (c) 2018 Samsung Electronics Co., Ltd.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Samsung Electronics Co., Ltd. nor the names of
 *       its contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _KV_STORE_INCLUDE_H_
#define _KV_STORE_INCLUDE_H_

#include <string>
#include <unordered_map>
#include <map>
#include <set>
#include <list>
#include <bitset>
#include <new>
#include <thread>
#include <chrono>
#include "kvs_adi_internal.h"
#include "history.hpp"

/**
 * this is for key value store and iteration in memory
 * to emulate KVSSD behavior through ADI
 * each namespace will have one store
 */

namespace kvadi {

struct CmpEmulPrefix {
    bool operator()(const kv_key* a, const kv_key* b) const {

        const char *strA = (const char *)a->key;
        const char *strB = (const char *)b->key;

        // using leading 4 bytes in ascending order for group and iteration
        uint32_t intA = 0;
        memcpy(&intA, strA, 4);
        uint32_t intB = 0;
        memcpy(&intB, strB, 4);

        // first compare first 32 bits
        if (intA == intB) {
            // if equal, compare length
            if (a->length < b->length) {
                return true;
            } else if (a->length > b->length) {
                return false;
            } else {
                // if same length, compare content
                // (a->length == b->length) {
                return (memcmp(strA + 4, strB + 4, b->length - 4) < 0);
            }
        } else {
            return intA < intB;
        }
    }
};

class kv_noop_emulator : public kv_device_api{
public:
    kv_noop_emulator(uint64_t capacity) {}
    virtual ~kv_noop_emulator() {}

    // basic operations
    kv_result kv_store(const kv_key *key, const kv_value *value, uint8_t option, uint32_t *consumed_bytes, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_retrieve(const kv_key *key, uint8_t option, kv_value *value, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_exist(const kv_key *key, uint32_t keycount, uint8_t *value, uint32_t &valuesize, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_purge(kv_purge_option option, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_delete(const kv_key *key, uint8_t option, uint32_t *recovered_bytes, void *ioctx) { return KV_SUCCESS; }

    // iterator
    kv_result kv_open_iterator(const kv_iterator_option opt, const kv_group_condition *cond, bool_t keylen_fixed, kv_iterator_handle *iter_hdl, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_close_iterator(kv_iterator_handle iter_hdl, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_iterator_next(kv_iterator_handle iter_hdl, kv_key *key, kv_value *value, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_iterator_next_set(kv_iterator_handle iter_hdl, kv_iterator_list *iter_list, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_list_iterators(kv_iterator *iter_list, uint32_t *count, void *ioctx) { return KV_SUCCESS; }
    kv_result kv_delete_group(kv_group_condition *grp_cond, uint64_t *recovered_bytes, void *ioctx) { return KV_SUCCESS; }

    kv_result set_interrupt_handler(const kv_interrupt_handler int_hdl) { return KV_SUCCESS; }
    kv_interrupt_handler get_interrupt_handler() { return NULL; }
    kv_result poll_completion(uint32_t timeout_usec, uint32_t *num_events) { return KV_SUCCESS; }

    uint64_t get_total_capacity() { return -1; }
    uint64_t get_available() { return -1; }
};

#define GC_SYNC_WATERMARK 4
#define GC_LOW_WATERMARK 0.4
#define GC_HIGH_WATERMARK 0.6

#define GC_MIN_BLK_THRES 0.3
#define GC_LOW_BLK_THRES 0.5
// record_meta (or chunks meta)
typedef struct {
    std::string key;
    uint16_t start;
    uint32_t length; // in bytes
} obj_meta;

typedef struct {
    uint32_t plid;
    uint16_t bid;
    uint16_t pid;
    uint16_t offset; // within page 
    uint8_t obj_idx;
    uint32_t length;
} kv_index;


class kvssd_block;
class kvssd_plane;
class kvssd_ftl;
class kvssd_stats {
public:
    kvssd_ftl *ftl;
    // ftl stats
    uint64_t FTLReadBytes;
    uint64_t FTLWriteBytes;
    // flash stats
    uint64_t numBlockWrite;
    uint64_t numBlockRead;
    uint64_t numBlockErase;
    // GC stats
    uint64_t numGCBlocks;
    uint64_t numGC_SYNCBlocks;

public:
    kvssd_stats() {
        reset_stats();
    }
    kvssd_stats(kvssd_ftl *p) :ftl(p) {
        reset_stats();
    }
    void print_stats();
    void reset_stats();
};

// page class only store metadata
class kvssd_page {
private:
    kvssd_block *parent;
    //char state; // 0-free 1-dirty 2-in-use
    obj_meta *obj_infos;
    char *obj_states; // 0-free, 1-use, 2-delete
    uint8_t active_obj;
    uint8_t max_num_objs;

    uint16_t tail;
public:
    kvssd_page (kvssd_block *p, uint8_t max_objs) : 
    parent(p), max_num_objs(max_objs) {
        obj_infos = new obj_meta[max_num_objs];
        obj_states = new char[max_num_objs];
        for (uint8_t i = 0; i < max_num_objs; i++) {
            obj_states[i] = 0;
        }
        active_obj = 0;
        tail = 0;
    };
    ~kvssd_page () {
        delete [] obj_infos;
        delete [] obj_states;
    };
    kvssd_block *get_parent() {return parent;}
    uint16_t get_tail() {return tail;}
    uint16_t get_avail_bytes() ;
    uint8_t get_avail_objs() {return max_num_objs - active_obj;}
    uint8_t get_active_obj_info() {return active_obj;}
    char get_obj_state(uint8_t obj_id) {return obj_states[obj_id];}
    obj_meta &get_obj_key(uint8_t obj_id) {return obj_infos[obj_id];}
    void insert_obj_info(const kv_key *key, uint32_t vlen) {
        // run out of OOB space, skip the page
        if (active_obj >= max_num_objs) {
            printf("run out of OOB space, skip the page\n");
            exit(-1);
        }
        obj_states[active_obj] = 1;
        obj_infos[active_obj] = {std::string((char*)key->key,key->length), tail, vlen};

        active_obj++;
    };
    void delete_obj_info(int obj_id) {
        obj_states[obj_id] = 2;
    };
    void consume_bytes (uint32_t len) {
        if (len > get_avail_bytes()) {
            printf("kvssd_page not enough bytes for page comsume\n");
            exit(1);
        }
        tail += len;
        if (tail > 4096) {
            printf("assert tail exceed limit\n");
        }
    };
    void erase() {
        for (uint8_t i = 0; i < max_num_objs; i++) {
            obj_states[i] = 0;
        }
        active_obj = 0;
        tail = 0;
    }
};

class kvssd_block {
private:
    friend class kvssd_page;
    kvssd_plane *parent;
    uint16_t bid;
    uint16_t page_size;
    uint16_t page_cnt;
    kvssd_page *pages;
    
    // block metadata
    char state; // 0-free 1-dirty
    kvssd_page * active_page;
    uint16_t used_pages;
    uint32_t valid_bytes;
public:
    kvssd_block(kvssd_plane *p, uint16_t blk_id, uint16_t pg_size, uint16_t pg_cnt) : 
        parent(p), bid(blk_id), page_size(pg_size), page_cnt(pg_cnt) {
        // initialize pages in block
        uint8_t max_num_objs = page_size / 256; // 256bytes chunk, 8bytes metadata per obj
        pages = (kvssd_page *)malloc(sizeof(kvssd_page) * page_cnt);
        for (uint16_t i = 0; i < page_cnt; i++) {
            (void) new (&pages[i]) kvssd_page(this, max_num_objs);
        }
        valid_bytes = 0;
        used_pages = 0;
        state = 0;
        active_page = &pages[0];
    };
    ~kvssd_block() {free(pages);}
    double get_util() {
        return (double)valid_bytes/page_cnt/page_size;
    }
    uint16_t get_avail_pages() { return page_cnt-used_pages-1;}
    kvssd_page* get_active_page() {return active_page;}
    uint16_t get_active_pid() {return (uint16_t)(active_page-pages);}
    kvssd_page* get_page(uint32_t pid) {return &pages[pid];}
    
    uint32_t get_avail_bytes() {
        return get_avail_pages()*page_size + active_page->get_avail_bytes();
    };
    kvssd_page* next_active_page() {
        used_pages++;
        active_page++;
        return active_page;
    }
    void consume_pages (uint32_t vlen) {
        if (vlen > get_avail_bytes()) {
            printf("kvssd_block not enough pages to consume\n");
            exit(1);
        }
        valid_bytes += vlen;
        do {
            uint16_t page_space = get_active_page()->get_avail_bytes();
            uint16_t consume_space = vlen <= page_space ? vlen : page_space;
            get_active_page()->consume_bytes(consume_space);
            if ((get_active_page()->get_avail_bytes() == 0 || 
                get_active_page()->get_avail_objs() <= 0) && used_pages < page_cnt-1) {
                active_page++;
                used_pages++;
                if (used_pages >256) {
                    printf("assert used_pages > 256\n");
                }
            }
            vlen -= consume_space;
        } while(vlen > 0);
        
    };
    kvssd_plane *get_parent() {return parent;}
    uint16_t get_id() {return bid;}
    void mark_dirty() {state = 1;}
    char get_state() {return state;}
    void invalid_bytes(uint32_t vlen) {valid_bytes -= vlen;};

    void erase() {
        for (uint16_t i = 0; i < page_cnt; i++) {
            pages[i].erase();
        }
        valid_bytes = 0;
        used_pages = 0;
        state = 0;
        active_page = &pages[0];
    }

};

class kvssd_plane {
private:
    kvssd_ftl *parent;
    uint32_t plid;
    uint16_t block_cnt;
    uint32_t block_size;
    uint16_t page_cnt;
    uint16_t page_size;
    kvssd_block* blocks;
    kvssd_block* active_block;
    // block management
    std::list<uint16_t> free_blocks;
    std::list<uint16_t> dirty_blocks;
    uint16_t used_blocks;
    
public:
    std::mutex plane_mutex;

    kvssd_plane (kvssd_ftl *p, uint32_t pool_id, uint32_t blk_size, uint16_t blk_cnt, uint16_t pg_size) :
    parent(p), plid(pool_id), block_cnt(blk_cnt), block_size(blk_size), page_size(pg_size) {
        blocks = (kvssd_block *)malloc(sizeof(kvssd_block)*block_cnt);
        page_cnt = block_size/page_size;
        for (uint16_t i = 0; i < block_cnt; i++) {
            (void) new (&blocks[i]) kvssd_block(this, i, page_size, page_cnt);
            free_blocks.push_back(i);
        }
        used_blocks = 1; // count the active block
        active_block = &blocks[free_blocks.front()];
        free_blocks.pop_front();
    };
    ~kvssd_plane () {
        free(blocks);
    };
    kvssd_block* BLAlloc() {return active_block;}
    kvssd_block* get_active_block() {return active_block;}
    kvssd_block* get_block(uint16_t bid) { return &blocks[bid];}
    kvssd_block* next_active_block (bool affirm);
    int reclaim_block();
    void reclaim_block_sync();
    void consume_blocks(uint16_t num_blks, bool affrim) { 
        // consume aligned blocks (for large objects)
        if (get_active_block()->get_avail_bytes() != block_size) {
            printf("kvssd_plane not clean block when consuming block\n");
            exit(0);
        }
        for (uint16_t i = 0; i < num_blks; i++) {
            get_active_block()->consume_pages(block_size);
            next_active_block(affrim);
        }
    };
    uint32_t get_id() {return plid;};
    uint16_t get_block_id(kvssd_block *blk) {return (uint16_t)(blk - blocks);}
    uint32_t get_plane_size() {return block_size*block_cnt;}
    uint16_t get_used_blocks() {return used_blocks ;} 
    void write(const kv_key *key, const kv_value *value, bool affirm) {
        uint32_t vlen = value->length;
        kvssd_block *block = get_active_block();
        kvssd_page *page = block->get_active_page();
        page->insert_obj_info(key, vlen); // store metadata in page OOB area

        // comsume pages
        if (vlen > block_size) {
            // should not happen in our evaluation
            uint16_t blocks_need = vlen/block_size + (uint16_t)(vlen%block_size != 0);
            consume_blocks(blocks_need, affirm);
        }
        else { // should have enough space in the active block
            block->consume_pages(vlen);
            if (block->get_avail_bytes() == 0) {
                next_active_block(affirm);
            }
        }
    }
    void tombmark(kv_index &kv_idx) {
        kvssd_page *page = get_block(kv_idx.bid)->get_page(kv_idx.pid);
        page->delete_obj_info(kv_idx.obj_idx);
        get_block(kv_idx.bid)->invalid_bytes(kv_idx.length);
    }

    kv_index get_index(const kv_key *key, uint32_t vlen, bool affirm) {
        // index allocation
        kvssd_block* block;
        kvssd_page *page;
        block = BLAlloc();
        page = block->get_active_page();

        if (page->get_avail_objs() <= 0) {
            page = block->next_active_page(); // TODO: is this right?
        }
        
        if (vlen > get_plane_size()) {
            printf("Not support object size larger than plane size");
            exit(-1);
        }
        else if (vlen > block_size) {
            /* large objects will align with block, benefit for GC */
            block = next_active_block(affirm);
            page = block->get_active_page();
        }
        else if (block->get_avail_bytes() < vlen) {
            // we cannot fit in the active block
            /* advance the active block */
            block = next_active_block(affirm);
            page = block->get_active_page();
        }

        kv_index kv_idx {get_id(), block->get_id(), block->get_active_pid(), \
        page->get_tail(), page->get_active_obj_info(), vlen};
        
        return kv_idx;
    }
};

class kvssd_ftl {
private:
    // block arch
    uint32_t plane_cnt;
    uint32_t plane_size;
    uint32_t block_size;
    uint16_t page_size;
    kvssd_plane* planes;
    uint32_t active_pool_id;
    // key component for kvssd indexing
    std::unordered_map<std::string, kv_index> kv_hash_index;
    std::mutex kv_index_mutex;

    // GC thread
    pthread_t t_GC;

    kvssd_plane* PAlloc() {
        kvssd_plane* ret = &planes[active_pool_id];
         // round robin way (simplified SSD plane allocation)
        active_pool_id = (active_pool_id + 1) % plane_cnt;
        return ret;
    };
    kvssd_plane* get_plane(uint32_t plid) {return &planes[plid];};
    double get_util () {
        uint32_t total_used_blocks = 0;
        for (uint32_t i = 0; i < plane_cnt; i++) {
            total_used_blocks += planes[i].get_used_blocks();
        }
        return (double)total_used_blocks *block_size / plane_size / plane_cnt;
    }
    void start_GC() {
        std::thread thrd = std::thread(&kvssd_ftl::GC_bg, this);
        t_GC = thrd.native_handle();
        thrd.detach();
    }
    void GC_bg();

public:
    kvssd_stats stats;
    kvssd_ftl(uint32_t pl_size, uint32_t pl_cnt, uint32_t blk_size, uint16_t pg_size) :
    plane_cnt(pl_cnt), plane_size(pl_size), block_size(blk_size), page_size(pg_size), stats(this) {
        planes = (kvssd_plane*)malloc(sizeof(kvssd_plane) * plane_cnt);
        uint32_t blks_in_pool = (plane_size / block_size);
        for (uint32_t i = 0; i < plane_cnt; i++) {
            (void) new (&planes[i]) kvssd_plane(this, i, block_size, blks_in_pool, page_size);
        }
        active_pool_id = 0;
        start_GC();
        printf("kvssd_ftl constructed\n");
    };
    ~kvssd_ftl () {
        stats.print_stats();
        free(planes);
        pthread_cancel(t_GC);
    }
    uint32_t get_block_size() {return block_size;}
    uint32_t get_plane_cnt() {return plane_cnt;}
    // public interface 
    void kvssd_ftl_insert(const kv_key *key, const kv_value *value, kvssd_plane* plane) {
        // if (((char *)key->key)[10] == '9' && ((char *)key->key)[11] == '9' && 
        // ((char *)key->key)[12] == '7' &&((char *)key->key)[13] == '0' && 
        // ((char *)key->key)[14] == '4')
        // {
        //     printf("99704\n");
        // }
        //printf("insert key = %s\n",(char*)key->key);
        uint32_t vlen = value->length;
        {
            std::unique_lock<std::mutex> lock(plane->plane_mutex);
            kv_index kv_idx = plane->get_index(key, vlen, false);

            // hash index entry
            {
                std::unique_lock<std::mutex> lock(kv_index_mutex);
                std::string s_key = std::string((char *)key->key, key->length);
                auto it = kv_hash_index.find(s_key);
                if (it != kv_hash_index.end()) {
                    printf("[kvssd_ftl_insert] record exist\n");
                    exit(-1);
                }
                else {
                    kv_hash_index.emplace(std::make_pair(s_key, kv_idx));
                }
            }

            // perform actual write
            plane->write(key, value, false);
            stats.FTLWriteBytes += (vlen + key->length);
        }
    }

    void kvssd_ftl_insert(const kv_key *key, const kv_value *value) {
        kvssd_ftl_insert(key, value, PAlloc());
    }
    void kvssd_ftl_update(const kv_key *key, const kv_value *value) {
        // if (((char *)key->key)[10] == '5' && ((char *)key->key)[11] == '7' && 
        // ((char *)key->key)[12] == '3' &&((char *)key->key)[13] == '0' && 
        // ((char *)key->key)[14] == '7')
        // {
        //     printf("57307\n");
        // }
        //printf("update key = %s\n",(char*)key->key);
        // check mapping table, delete entry
        kv_index old_kv_idx;
        {
            std::unique_lock<std::mutex> lock(kv_index_mutex);
            std::string s_key = std::string((char *)key->key, key->length);
            auto it = kv_hash_index.find(s_key);
            if (it == kv_hash_index.end()) {
                printf("[kvssd_ftl_update] record not exist\n");
                exit(0);
            }
            else {
                old_kv_idx = it->second;
            }
        }

        kvssd_plane *plane = get_plane(old_kv_idx.plid);
        uint32_t vlen = value->length;
        {
            std::unique_lock<std::mutex> lock(plane->plane_mutex);
            kv_index kv_idx = plane->get_index(key, vlen, false);

            // hash index entry
            {
                std::unique_lock<std::mutex> lock(kv_index_mutex);
                std::string s_key = std::string((char *)key->key, key->length);
                auto it = kv_hash_index.find(s_key);
                if (it == kv_hash_index.end()) {
                    printf("[kvssd_ftl_update] record not exist\n");
                    exit(-1);
                }
                else {
                    it->second = kv_idx;
                }
            }

            // perform actual write
            plane->write(key, value, false);
            stats.FTLWriteBytes += (vlen + key->length);

            // delete metadata
            plane->tombmark(old_kv_idx);
        }
    };
    void kvssd_ftl_delete(const kv_key *key) {
        // check mapping table, delete entry
        kv_index kv_idx;
        {
            std::unique_lock<std::mutex> lock(kv_index_mutex);
            std::string s_key = std::string((char *)key->key, key->length);
            auto it = kv_hash_index.find(s_key);
            if (it == kv_hash_index.end()) {
                printf("[kvssd_ftl_delete] record not exist\n");
                exit(0);
            }
            else {
                kv_idx = it->second;
            }
        }

        kvssd_plane *plane = get_plane(kv_idx.plid);
        {
            {
                std::unique_lock<std::mutex> lock(kv_index_mutex);
                std::string s_key = std::string((char *)key->key, key->length);
                auto it = kv_hash_index.find(s_key); // double check index, in case GC happens in between
                if (it != kv_hash_index.end()) {
                    kv_idx = it->second;
                    kv_hash_index.erase(it);
                }
                else {
                    printf("[kvssd_ftl_delete] record not exist\n");
                    exit(0);
                }
            }
            std::unique_lock<std::mutex> lock(plane->plane_mutex);
            // delete metadata
            plane->tombmark(kv_idx);
            
        }
        
    };
    void kvssd_ftl_replace(const kv_key *key, const kv_value *value, kvssd_plane* plane) {
        
        uint32_t vlen = value->length;
        {
            kv_index kv_idx = plane->get_index(key, vlen, true);
           
            // hash index entry
            {
                std::unique_lock<std::mutex> lock(kv_index_mutex);
                std::string s_key = std::string((char *)key->key, key->length);
                auto it = kv_hash_index.find(s_key);
                if (it == kv_hash_index.end()) {
                    printf("[kvssd_ftl_replace] record not exist\n");
                    exit(-1);
                }
                else {
                    // overwrite index
                    it->second = kv_idx;
                }
            }

            // perform actual write
            plane->write(key, value, true);
        }
    }
    void kvssd_ftl_get(const kv_key *key) {}; // don't model kv_get

};

class kv_emulator : public kv_device_api{
public:
    kv_emulator(uint64_t capacity, std::vector<double> iops_model_coefficients, bool_t use_iops_model, uint32_t nsid);
    virtual ~kv_emulator();

    // basic operations
    kv_result kv_store(const kv_key *key, const kv_value *value, uint8_t option, uint32_t *consumed_bytes, void *ioctx);
    kv_result kv_retrieve(const kv_key *key, uint8_t option, kv_value *value, void *ioctx);
    kv_result kv_exist(const kv_key *key, uint32_t keycount, uint8_t *value, uint32_t &valuesize, void *ioctx);
    kv_result kv_purge(kv_purge_option option, void *ioctx);
    kv_result kv_delete(const kv_key *key, uint8_t option, uint32_t *recovered_bytes, void *ioctx);

    // iterator
    kv_result kv_open_iterator(const kv_iterator_option opt, const kv_group_condition *cond, bool_t keylen_fixed, kv_iterator_handle *iter_hdl, void *ioctx);
    kv_result kv_close_iterator(kv_iterator_handle iter_hdl, void *ioctx);
    kv_result kv_iterator_next_set(kv_iterator_handle iter_hdl, kv_iterator_list *iter_list, void *ioctx);
    kv_result kv_iterator_next(kv_iterator_handle iter_hdl, kv_key *key, kv_value *value, void *ioctx);
    kv_result kv_list_iterators(kv_iterator *iter_list, uint32_t *count, void *ioctx);
    kv_result kv_delete_group(kv_group_condition *grp_cond, uint64_t *recovered_bytes, void *ioctx);

    uint64_t get_total_capacity();
    uint64_t get_available();

    // these do nothing, but to conform API, emulator have queue level operations for
    // device behavior simulation.
    kv_result set_interrupt_handler(const kv_interrupt_handler int_hdl);
    kv_interrupt_handler get_interrupt_handler();
    kv_result poll_completion(uint32_t timeout_usec, uint32_t *num_events);

private:

    kv_history stat;
    inline int insert_to_unordered_map(std::unordered_map<kv_key*, std::string> &unordered, kv_key* key,  const kv_value *value, const std::string &valstr, uint8_t option);
    // max capacity
    uint64_t m_capacity;

    // space available
    uint64_t m_available;

    typedef std::map<kv_key*, std::string, CmpEmulPrefix> emulator_map_t;
    //std::map<uint32_t, std::unordered_map<kv_key*, std::string> > m_map;
    std::map<kv_key*, std::string, CmpEmulPrefix> m_map;
    std::mutex m_map_mutex;

    std::map<int32_t, _kv_iterator_handle *> m_it_map;
    kv_iterator m_iterator_list[SAMSUNG_MAX_ITERATORS];
    std::mutex m_it_map_mutex;

    // use IOPS model or not
    bool_t m_use_iops_model;

    uint32_t m_nsid;
    // ftl
    kvssd_ftl *ftl;

    kv_interrupt_handler m_interrupt_handler;
};



} // end of namespace
#endif
