#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <new>
#include <assert.h>
#include <unistd.h>
#include <thread>

#include "kv_writebatch.h"
#include "kvraid.h"

#define thread_cnt 1
#define OBJ_LEN 512+256
#define DEV_CAP 107374182400

using namespace kvraid;

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

void load(KVRaid *kvr, int num, bool seq, int tid) {
    RandomGenerator gen;
    Random rand(0);
    kvr_key *keys = new kvr_key[num];
    kvr_value *vals = new kvr_value[num];
    for (int i = 0; i < num; i++) {
        const int k = seq ? i + tid*num : (rand.Next() % num) + tid*num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        char *value = gen.Generate(OBJ_LEN);

        keys[i].key =key;
        keys[i].length = 16;
        vals[i].val = value;
        vals[i].length = OBJ_LEN;

        kvr->kvr_insert(&keys[i], &vals[i]);
        //printf("[%d insert] key %s, val %s\n",tid, key, std::string(value, 8).c_str());
    }
}

void batch_load(KVRaid *kvr, int num, int batch_size, bool seq, int tid) {
  RandomGenerator gen;
  Random rand(0);
  WriteBatch batch;
  for (int i = 0; i < num/batch_size; i++) {
    for (int j = 0; j < batch_size; j++) {
      const int k = seq ? i + tid*num : (rand.Next() % num) + tid*num;
      char *key = new char[100];
      snprintf(key, sizeof(key), "%016d", k);
      //char *value = gen.Generate(OBJ_LEN);
      char *value = new char[OBJ_LEN];


      kvr_key *keys = new kvr_key;
      kvr_value *vals = new kvr_value;
      keys->key =key;
      keys->length = 16;
      vals->val = value;
      vals->length = OBJ_LEN;

      batch.Put(keys, vals, 0);
    }
    kvr->kvr_write_batch(&batch);
    batch.Clear();
    printf("load batch %d\n", i);
  }
}

void update(KVRaid *kvr, int num, bool seq, int tid) {
  RandomGenerator gen;
  Random rand(0);
  kvr_key *keys = new kvr_key[num];
  kvr_value *vals = new kvr_value[num];
  for (int j = 0; j < 10; j++) {
    for (int i = 0; i < num; i++) {
        const int k = seq ? i + tid*num : (rand.Next() % num) + tid*num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        char *value = gen.Generate(OBJ_LEN);

        keys[i].key =key;
        keys[i].length = 16;
        vals[i].val = value;
        vals[i].length = OBJ_LEN;

        kvr->kvr_update(&keys[i], &vals[i]);
        //printf("[%d update] key %s, val %s\n",tid, key, std::string(value, 8).c_str());
    }
  }
}


void read(KVRaid *kvr, int num) {
    kvr_key *keys = new kvr_key[num];
    kvr_value *vals = new kvr_value[num];
    for (int i = 0; i < num; i++) {
        char key[100];
        snprintf(key, sizeof(key), "%016d", i);

        keys[i].key =key;
        keys[i].length = 16;
        vals[i].val = NULL;
        kvr->kvr_get(&keys[i], &vals[i]);
        printf("[get] key %s, val %s, val_len %d\n",key, std::string(vals[i].val, 8).c_str(), vals[i].length);
        free(vals[i].val);
    }
}

int main() {
    int num_ssds = 5;
    int k = 3, r = 2;
    int slab_list[2] = {1024, 2048};

    KVS_CONT* kvs_conts;
    kvs_conts = (KVS_CONT*)malloc(num_ssds * sizeof(KVS_CONT));
    for (int i = 0; i < num_ssds; i++) {
      (void) new (&kvs_conts[i]) KVS_CONT("/dev/kvemul", 64, DEV_CAP);
    }
    //KVRaid kvr(k, r, 1, slab_list, kvs_conts, Mem);
    KVRaid kvr(k, r, 1, slab_list, kvs_conts, Storage);

    std::thread th[16];
    for (int i = 0; i< thread_cnt; i++) {
        th[i] = std::thread(load, &kvr, 1000 , true, i);
    }

    for (int i = 0; i< thread_cnt; i++) {
        th[i].join();
    }

    for (int i = 0; i< thread_cnt; i++) {
        th[i] = std::thread(update, &kvr, 100 , false, i);
    }

    for (int i = 0; i< thread_cnt; i++) {
        th[i].join();
    }

    read(&kvr, 1000);

    batch_load(&kvr, 1000, 10, false, 0);

    sleep(100);

    return 0;
}