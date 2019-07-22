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
#include <atomic>
#include "kvssd/kvs_api.h"
#include "kvssd/kvs_cont.h"
#include "phy_kv.h"
#include "threadpool.h"

class KV_DEVICE; // forward declaration
typedef struct {
    KVD_OPS ops; // 0-insert, 1-update, 2-delete, 3-get
	phy_key* key;
    phy_val* value;
    void (*callback)(void *);
    void *argument;
    KV_DEVICE *dev;
} dev_io_context;

static void io_task(void *arg);

typedef struct {
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
    // uint32_t capacity; //MB
    kvd_stats stats;

    // req queue (using thread pool)
    threadpool_t *pool;
    sem_t q_sem;

    KV_DEVICE(int id, KVS_CONT *kvs_cont, int thread_count, int queue_depth): 
        dev_id(id), cont_(kvs_cont) {
        //stats
        // stats = { 0, 0, 0 };
        // capacity = kvs_get_device_capacity(dev)/1024/1024;

        pool = threadpool_create(thread_count, queue_depth, 0, &q_sem);
        sem_init(&q_sem, 0, queue_depth);
    }
    ~KV_DEVICE() {
        threadpool_destroy(pool, 1);
        sem_destroy(&q_sem);
        printf("store %d, get %d, delete %d\n",stats.num_store.load(), stats.num_retrieve.load(), stats.num_delete.load());
    };

    
	bool kv_store(phy_key *key, phy_val *value);
	bool kv_delete(phy_key *key);
	bool kv_get(phy_key *key, phy_val *value);
    
    bool kv_astore(phy_key *key, phy_val *value, void (*callback)(void *), void *argument);
	bool kv_adelete(phy_key *key, void (*callback)(void *), void *argument);
    bool kv_aget(phy_key *key, phy_val *value, void (*callback)(void *), void *argument);

    int64_t get_capacity();
    double get_util();
    float get_waf();
    
};


#endif