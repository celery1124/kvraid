/* kv_device.h
* 07/13/2017
* by Mian Qin
*/

#ifndef   _kv_device_h_   
#define   _kv_device_h_   

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <vector>
#include <atomic>
#include "kvssd/kvs_api.h"
#include "kvssd/kvs_cont.h"
#include "phy_kv.h"
//#include "threadpool.h"
#include <semaphore.h>

// class KV_DEVICE; // forward declaration
// typedef struct {
//     KVD_OPS ops; // 0-insert, 1-update, 2-delete, 3-get
// 	phy_key* key;
//     phy_val* value;
//     void (*callback)(void *);
//     void *argument;
//     KV_DEVICE *dev;
// } dev_io_context;

// static void io_task(void *arg);

typedef struct {
    std::atomic<uint64_t> write_bytes{0};
    std::atomic<uint32_t> num_store{0};
    std::atomic<uint32_t> num_retrieve{0};
    std::atomic<uint32_t> num_delete{0};
} kvd_stats;

class KV_DEVICE
{
public:
    int dev_id;
    // kvssd container
    KVS_CONT *cont_;

    // stats
    int64_t log_capacity_; //B
    int64_t real_capacity_; //
    kvd_stats stats;

    // req queue (using thread pool)
    // threadpool_t *pool;
    sem_t q_sem;

    KV_DEVICE(int id, KVS_CONT *kvs_cont, int thread_count, int queue_depth): 
        dev_id(id), cont_(kvs_cont), log_capacity_() {
        log_capacity_ = kvs_cont->get_log_capacity();
        real_capacity_ = get_real_capacity();

        // pool = threadpool_create(thread_count, queue_depth, 0, &q_sem);
        sem_init(&q_sem, 0, queue_depth);
    }
    ~KV_DEVICE() {
        // threadpool_destroy(pool, 1);
        sem_destroy(&q_sem);
        FILE *fd = fopen("kv_device.log","a");
        fprintf(fd, "store %d, get %d, delete %d, write_bytes %lu\n",stats.num_store.load(), stats.num_retrieve.load(), stats.num_delete.load(), stats.write_bytes.load());
        fprintf(fd, "usage %.3f\n", (double)get_capacity()*get_util()/1024/1024/1024); // GB
        fclose(fd);
    };

    
	bool kv_store(phy_key *key, phy_val *value);
    bool kv_store(std::string *key, std::string *value);
	bool kv_delete(phy_key *key);
    bool kv_delete(std::string *key);
	bool kv_get(phy_key *key, phy_val *value);
	bool kv_get(std::string *key, phy_val *value);
    bool kv_get(std::string *key, std::string *value);
    
    bool kv_astore(phy_key *key, phy_val *value, void (*callback)(void *), void *argument);
    bool kv_astore(std::string *key, phy_val *value, void (*callback)(void *), void *argument);
	bool kv_adelete(phy_key *key, void (*callback)(void *), void *argument);
	bool kv_adelete(std::string *key, void (*callback)(void *), void *argument);
    bool kv_aget(phy_key *key, phy_val *value, void (*callback)(void *), void *argument);
    bool kv_aget(std::string *key, phy_val *value, void (*callback)(void *), void *argument);

    void kv_scan_keys(std::vector<std::string> &keys); // for testing

    int64_t get_log_capacity() {return log_capacity_;};
    int64_t get_capacity() {return real_capacity_;}
    int64_t get_real_capacity();
    double get_util();
    float get_waf();
    
};


#endif
