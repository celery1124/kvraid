
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <atomic>
#include "leveldb/slice.h"
#include "kvssd/kvs_api.h"
#include "kvssd/kvs_cont.h"
#include "leveldb/ec.h"
#include <semaphore.h>

namespace kvssd {
  typedef struct {
      std::atomic<uint32_t> num_store{0};
      std::atomic<uint32_t> num_retrieve{0};
      std::atomic<uint32_t> num_delete{0};
  } kvd_stats;

  class KV_DEV {
    private:
      KVS_CONT *cont_;
      kvd_stats stats;
      sem_t q_sem;
    public:
      KV_DEV(KVS_CONT *kvs_cont, int queue_depth = 32) : cont_(kvs_cont) {
        sem_init(&q_sem, 0, queue_depth);
      };
      ~KV_DEV() {
        FILE *fd = fopen("kv_device.log","a");
        fprintf(fd, "store %d, get %d, delete %d\n",stats.num_store.load(), stats.num_retrieve.load(), stats.num_delete.load());
        fclose(fd);
        sem_destroy(&q_sem);
      };
      bool kv_exist (leveldb::Slice *key);
      uint32_t kv_get_size(leveldb::Slice *key);
      kvs_result kv_store(leveldb::Slice *key, leveldb::Slice *val);
      kvs_result kv_store_async(leveldb::Slice *key, leveldb::Slice *val, 
      void (*callback)(void *), void *args);
      kvs_result kv_append(leveldb::Slice *key, leveldb::Slice *val);
      // caller must free vbuf memory
      kvs_result kv_get(const leveldb::Slice *key, char*& vbuf, int& vlen);
      kvs_result kv_get_async(const leveldb::Slice *key, void (*callback)(void *), void *args);
      kvs_result kv_pget(const leveldb::Slice *key, char*& vbuf, int count, int offset);
      kvs_result kv_delete(leveldb::Slice *key);
      kvs_result kv_delete_async(leveldb::Slice *key, void (*callback)(void *), void *args);
      

      kvs_result kv_scan_keys(std::vector<std::string> &keys);
  };

  class KV_DEV_ARRAY {
    private:
      int k_, r_;
      EC ec_;
      KV_DEV *kvssds_;
    public:
      KV_DEV_ARRAY(int num_d, int num_p, KV_DEV *devs) :
      k_(num_d), r_(num_p), ec_(k_, r_), kvssds_(devs) {ec_.setup();};
      ~KV_DEV_ARRAY () {};

      bool kv_exist (leveldb::Slice *key);
      uint32_t kv_get_size(leveldb::Slice *key);
      kvs_result kv_store(leveldb::Slice *key, leveldb::Slice *val);
      // caller must free vbuf memory
      kvs_result kv_get(const leveldb::Slice *key, char*& vbuf, int& vlen);
      kvs_result kv_delete(leveldb::Slice *key);

      kvs_result kv_scan_keys(std::vector<std::string> &keys);
  };
} // namespace