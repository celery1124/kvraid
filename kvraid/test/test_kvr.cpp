#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <new>
#include <assert.h>
#include <unistd.h>
#include <thread>
#include <cmath>

#include "kvr.h"
#include "kvraid.h"
#include "kvec.h"

#define OBJ_MAX_LEN 4000
#define OBJ_MIN_LEN 100
#define DEV_CAP 1 << 30

class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    Random rdn(0);
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      data_.append(1, (char)(' '+rdn.Uniform(95)));
    }
    pos_ = 0;
  }

  char* Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return (char *)(data_.data() + pos_ - len);
  }
};

void load(KVR *kvr, int num, bool seq, int tid) {
  RandomGenerator gen;
  Random key_rand(tid);
  Random vlen_rand(tid);
  kvr_key *keys = new kvr_key[num];
  kvr_value *vals = new kvr_value[num];
  for (int i = 0; i < num; i++) {
      const int k = seq ? i + tid*num : (key_rand.Next() % num) + tid*num;
      char key[100];
      snprintf(key, sizeof(key), "%016d", k);
      int vlen = vlen_rand.Uniform(OBJ_MAX_LEN-OBJ_MIN_LEN) + OBJ_MIN_LEN;
      char *value = gen.Generate(vlen);

      keys[i].key =key;
      keys[i].length = 16;
      vals[i].val = value;
      vals[i].length = vlen;

      kvr->kvr_insert(&keys[i], &vals[i]);
      //printf("[%d insert] key %s, val %s\n",tid, key, std::string(value, 8).c_str());
  }
  delete [] keys;
  delete [] vals;
}

void mixed (KVR *kvr, int dist, double wr_ratio, int ops_nums, int record_nums, int tid) {
  RandomGenerator gen;
  Random key_rand(tid);
  Random vlen_rand(tid);
  kvr_key *keys = new kvr_key[ops_nums];
  kvr_value *vals = new kvr_value[ops_nums];
  int ops ; // 0 - update, 1 - get
  for (int i = 0; i < ops_nums; i++) {
      ops = (double(rand() % 100) / 100) < wr_ratio ? 0 : 1;
      int k;
      if (dist == 0) { // uniform
        k = key_rand.Uniform(record_nums) ;
      }
      else if (dist == 1) { // zipfian
        k = key_rand.Skewed((int)std::log2(record_nums));
      }
      else if (dist == 2) { // seq
        k = (i + ops_nums*tid) % record_nums;
      }
      else { // default
        k = key_rand.Uniform(record_nums) ;
      }
      char key[100];
      snprintf(key, sizeof(key), "%016d", k);
      int vlen = vlen_rand.Uniform(OBJ_MAX_LEN-OBJ_MIN_LEN) + OBJ_MIN_LEN;

      if (ops == 0) {
        char *value = gen.Generate(vlen);
        keys[i].key =key;
        keys[i].length = 16;
        vals[i].val = value;
        vals[i].length = vlen;

        kvr->kvr_update(&keys[i], &vals[i]);
        //printf("[%d update] key %s, val %s\n",tid, key, std::string(value, 8).c_str());
      }
      else if (ops == 1) {
        keys[i].key =key;
        keys[i].length = 16;
        vals[i].val = NULL;
        kvr->kvr_get(&keys[i], &vals[i]);
        //printf("[get %d] key %s, val %s, val_len %d\n", tid, key, std::string(vals[i].val, 8).c_str(), vals[i].length);
        free(vals[i].val);
      }
      else {
        printf ("Mixed workload wrong ops!\n");
        exit(-1);
      }
      
  }
  delete [] keys;
  delete [] vals;
}

void update(KVR *kvr, int dist, int ops_nums, int record_nums, int tid) {
  RandomGenerator gen;
  Random key_rand(0);
  Random vlen_rand(tid);
  kvr_key *keys = new kvr_key[ops_nums];
  kvr_value *vals = new kvr_value[ops_nums];
  for (int i = 0; i < ops_nums; i++) {
    int k;
    if (dist == 0) { // uniform
      k = key_rand.Uniform(record_nums) ;
    }
    else if (dist == 1) { // zipfian
      k = key_rand.Skewed((int)std::log2(record_nums));
    }
    else if (dist == 2) { // seq
      k = (i + ops_nums*tid) % record_nums;
    }
    else { // default
      k = key_rand.Uniform(record_nums) ;
    }
    char key[100];
    snprintf(key, sizeof(key), "%016d", k);
    int vlen = vlen_rand.Uniform(OBJ_MAX_LEN-OBJ_MIN_LEN) + OBJ_MIN_LEN;

    char *value = gen.Generate(vlen);

    keys[i].key =key;
    keys[i].length = 16;
    vals[i].val = value;
    vals[i].length = vlen;

    kvr->kvr_update(&keys[i], &vals[i]);
    //printf("[%d update] key %s, val %s\n",tid, key, std::string(value, 8).c_str());
  }
  delete [] keys;
  delete [] vals;
}


void get(KVR *kvr, int dist, int ops_nums, int record_nums, int tid) {
  Random key_rand(tid);
  kvr_key *keys = new kvr_key[ops_nums];
  kvr_value *vals = new kvr_value[ops_nums];
  for (int i = 0; i < ops_nums; i++) {
    int k;
    if (dist == 0) { // uniform
      k = key_rand.Uniform(record_nums) ;
    }
    else if (dist == 1) { // zipfian
      k = key_rand.Skewed((int)std::log2(record_nums));
    }
    else if (dist == 2) { // seq
      k = (i + ops_nums*tid) % record_nums;
    }
    else { // default
      k = key_rand.Uniform(record_nums) ;
    }
    char key[100];
    snprintf(key, sizeof(key), "%016d", k);

    keys[i].key =key;
    keys[i].length = 16;
    vals[i].val = NULL;
    kvr->kvr_get(&keys[i], &vals[i]);
    //printf("[get %d] key %s, val %s, val_len %d\n", tid, key, std::string(vals[i].val, 8).c_str(), vals[i].length);
    free(vals[i].val);
  }

  delete [] keys;
  delete [] vals;
}

void erased_get(KVR *kvr, int num, int tid) {
  kvr_key *keys = new kvr_key[num];
  kvr_value *vals = new kvr_value[num];
  for (int i = 0; i < num; i++) {
      char key[100];
      snprintf(key, sizeof(key), "%016d", i);

      keys[i].key =key;
      keys[i].length = 16;
      vals[i].val = NULL;
      kvr->kvr_erased_get(5, &keys[i], &vals[i]);
      printf("[erased_get %d] key %s, val %s, val_len %d\n", tid, key, std::string(vals[i].val, 8).c_str(), vals[i].length);
      free(vals[i].val);
  }

  delete [] keys;
  delete [] vals;
}

void seek (KVR *kvr, int skey) {
  Iterator *it = kvr->NewIterator();
  kvr_key seekkey;
  char key[100];
  snprintf(key, sizeof(key), "%016d", skey);

  seekkey.key =key;
  seekkey.length = 16;
  it->Seek(seekkey);
  for (int i = 0; i < 10; i++) {
    if (it->Valid()) {
      std::string k(it->Key().key, it->Key().length);
      std::string v(it->Value().val, 8);
      printf("key %s, value: %s\n", k.c_str(), v.c_str());
      it->Next();
    }
    else break;
  }
  delete it;
}

int main(int argc, char *argv[]) {
  std::string workload;
  int kvr_type;
  int meta_type;
  int64_t record_nums, ops_nums;
  int dist;
  int thread_nums;
  double wr_ratio;
  if (argc < 9) {
    printf("Usage: %s <workload (mixed, rdonly, udonly, recovery, seek)> <record_nums> <ops_nums> <distribution (0-uniform, 1-zipfian)> <kvr type (0-kvraid, 1-kvmirror, 2-kvec, 3-kvraid_pack)> <meta type (0-Mem, 1-Storage, 2-Cuckoo)> <thread_nums> <wr_ratio (0.0-1.0)>\n", argv[0]);
    exit(0);
  }
  else {
    workload = std::string(argv[1]);
    record_nums = atoll(argv[2]);
    ops_nums = atoll(argv[3]);
    dist = atoi(argv[4]);
    kvr_type = atoi(argv[5]);
    meta_type = atoi(argv[6]);
    thread_nums = atoi(argv[7]);
    wr_ratio = atof(argv[8]);
  }
  int num_ssds = 6;
  int k = 4, r = 2;
  int slab_list[4] = {1024, 2048, 3072, 4096};

  KVS_CONT* kvs_conts;
  kvs_conts = (KVS_CONT*)malloc(num_ssds * sizeof(KVS_CONT));
  for (int i = 0; i < num_ssds; i++) {
    std::string dev_name = "/dev/kvemul" + std::to_string(i);
    (void) new (&kvs_conts[i]) KVS_CONT((char *)dev_name.c_str(), 64, DEV_CAP);
  }
  KVR *kvr;
  Cache *cache = NewLRUCache(512, 0); 

  switch(kvr_type) {
    case 0:
      kvr = NewKVRaid(k, r, 4, slab_list, kvs_conts, (MetaType)meta_type, false, cache);
      break;
    case 1:
      kvr = NewKVMirror(k, r, kvs_conts, cache);
      break;
    case 2:
      kvr = NewKVEC(k, r, 4, slab_list, kvs_conts, (MetaType)meta_type, cache);
      break;
    case 3:
      kvr = NewKVRaidPack(k, r, 4, slab_list, kvs_conts, Mem, false, cache);
      break;
    default:
      kvr = NewKVRaid(k, r, 4, slab_list, kvs_conts, (MetaType)meta_type, false, cache);
      break;
  }

  std::thread **th_load = new std::thread*[thread_nums];
  std::thread **th_run = new std::thread*[thread_nums];

  // load phase
  for (int i = 0; i< thread_nums; i++) {
      th_load[i] = new std::thread(load, kvr, record_nums/thread_nums , true, i);
  }

  for (int i = 0; i< thread_nums; i++) {
      th_load[i]->join();
  }

  printf("finish load\n\n");

  // close kvr and open again (for testing)
  delete kvr; 
  // repoen kvr
  switch(kvr_type) {
    case 0:
      kvr = NewKVRaid(k, r, 4, slab_list, kvs_conts, (MetaType)meta_type, false, cache);
      break;
    case 1:
      kvr = NewKVMirror(k, r, kvs_conts, cache);
      break;
    case 2:
      kvr = NewKVEC(k, r, 4, slab_list, kvs_conts, (MetaType)meta_type, cache);
      break;
    case 3:
      kvr = NewKVRaidPack(k, r, 4, slab_list, kvs_conts, Mem, false, cache);
      break;
    default:
      kvr = NewKVRaid(k, r, 4, slab_list, kvs_conts, (MetaType)meta_type, false, cache);
      break;
  }

  if (workload == "seek") {
    seek(kvr, 19);
    printf("finish iteraotr test\n\n");
  }
  else if (workload == "recovery") {
    for (int i = 0; i< thread_nums; i++) {
        th_run[i] = new std::thread(erased_get, kvr, 100 , i);
    }

    for (int i = 0; i< thread_nums; i++) {
        th_run[i]->join();
    }
    printf("finish erased_get test\n\n");
  }
  else if (workload == "rdonly") {
    for (int i = 0; i< thread_nums; i++) {
        th_run[i] = new std::thread(update, kvr, dist, ops_nums/thread_nums, record_nums, i);
    }

    for (int i = 0; i< thread_nums; i++) {
        th_run[i]->join();
    }
  }
  else if (workload == "uponly") {
    for (int i = 0; i< thread_nums; i++) {
        th_run[i] = new std::thread(get, kvr, dist, ops_nums/thread_nums, record_nums, i);
    }

    for (int i = 0; i< thread_nums; i++) {
        th_run[i]->join();
    }
  }
  else if (workload == "mixed") {
    for (int i = 0; i< thread_nums; i++) {
        th_run[i] = new std::thread(mixed, kvr, dist, wr_ratio, ops_nums/thread_nums, record_nums, i);
    }

    for (int i = 0; i< thread_nums; i++) {
        th_run[i]->join();
    }
  }


  //clean up
  delete kvr;

  for (int i = 0; i < num_ssds; i++) {
      kvs_conts[i].~KVS_CONT();
  }
  free(kvs_conts);

  for (int i = 0; i< thread_nums; i++) {
      delete th_load[i];
      delete th_run[i];
  }
  delete [] th_load;
  delete [] th_run;

  return 0;
}