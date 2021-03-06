
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "kvssd/kvs_api.h"
#include "kvssd/kvs_cont.h"
#include "leveldb/ec.h"

namespace kvssd {
  class KV_DEV {
    private:
      KVS_CONT *cont_;
    public:
      KV_DEV(KVS_CONT *kvs_cont) : cont_(kvs_cont) {};
      ~KV_DEV() {};
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