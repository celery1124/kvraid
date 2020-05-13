#include <stdio.h>
#include <stdlib.h>
#include <new>
#include <assert.h>
#include <cmath>

#include "../include/kv_device.h"
#include "../include/mapping_table.h"

#define DEV_CAP 107374182400

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

void load (Map *map, int nums) {
    for (int i = 0; i < nums; i++) {
        char key[100];
        snprintf(key, sizeof(key), "%016d", i);
        std::string skey(key, 16);
        phy_key ldkey(1, 1234);

        map->update(&skey, &ldkey);
    }
}

void heavy_update (Map* map, int dist, int ops_nums, int record_nums) {
    Random key_rand(0);
    for (int i = 0; i < ops_nums; i++) {
        int k;
        if (dist == 0) { // uniform
            k = key_rand.Uniform(record_nums) ;
        }
        else if (dist == 1) { // zipfian
            k = key_rand.Skewed((int)std::log2(record_nums));
        }
        else if (dist == 2) { // seq
            k = i%record_nums;
        }
        else { // default
        k = key_rand.Uniform(record_nums) ;
        }
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        std::string skey(key, 16);
        phy_key upkey(1, 1234);

        map->update(&skey, &upkey);
    }
}

void heavy_lookup (Map* map, int dist, int ops_nums, int record_nums) {
    Random key_rand(2);
    for (int i = 0; i < ops_nums; i++) {
        int k;
        if (dist == 0) { // uniform
            k = key_rand.Uniform(record_nums) ;
        }
        else if (dist == 1) { // zipfian
            k = key_rand.Skewed((int)std::log2(record_nums));
        }
        else if (dist == 2) { // seq
            k = i%record_nums;
        }
        else { // default
        k = key_rand.Uniform(record_nums) ;
        }
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        std::string skey(key, 16);
        phy_key rpkey;

        bool found = map->lookup(&skey, &rpkey);
    }
}

int main() {
    bool found = false;
    std::string skey = "1234";
    phy_key pkey(1, 1234);
    phy_key upkey(0, 0);
    phy_key rpkey;
    Map* memmap = NewMemMap();

    memmap->insert(&skey, &pkey);
    found = memmap->lookup(&skey, &rpkey);
    printf("memmap found pkey %d, %d\n",rpkey.get_slab_id(), rpkey.get_seq());
    memmap->update(&skey, &upkey);
    found = memmap->lookup(&skey, &rpkey);
    printf("memmap found pkey %d, %d\n",rpkey.get_slab_id(), rpkey.get_seq());

    KVS_CONT *conts = (KVS_CONT *)malloc(sizeof(KVS_CONT)*6);
    for (int i = 0; i < 6; i++) {
        (void) new (&conts[i]) KVS_CONT("/dev/kvemul", 64, DEV_CAP);
    }
    Map* storagemap = NewStorageMap(conts, 4, 2);

    storagemap->insert(&skey, &pkey);
    found = storagemap->lookup(&skey, &rpkey);
    printf("storagemap found pkey %d, %d\n",rpkey.get_slab_id(), rpkey.get_seq());
    storagemap->update(&skey, &upkey);
    found = storagemap->lookup(&skey, &rpkey);
    printf("storagemap found pkey %d, %d\n",rpkey.get_slab_id(), rpkey.get_seq());

    // heavy test
    load (storagemap, 100000);
    heavy_update (storagemap, 1, 100000, 100000);
    heavy_lookup (storagemap, 1, 100000, 100000);

    return 0;
}