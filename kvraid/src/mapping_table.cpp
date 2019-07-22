/* mapping_table.cpp
* 07/03/2019
* by Mian Qin
*/

#include <map>
#include <mutex>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/cache.h"
#include "leveldb/write_batch.h"
#include "mapping_table.h"

class MemMap : public Map {
private:
    std::map<std::string, phy_key> key_map_;
    std::mutex mutex_;
public:
    MemMap() {};
    ~MemMap() {};

    bool lookup(std::string *key, phy_key *val) {
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = key_map_.find(*key);
        bool exist = (it != key_map_.end());
        if (exist) *val = it->second;

        return exist;
    }

    void insert(std::string *key, phy_key *val) {
        std::unique_lock<std::mutex> lock(mutex_);
        key_map_.insert(std::make_pair(*key, *val));
    }

    void update(std::string *key, phy_key *val) {
        std::unique_lock<std::mutex> lock(mutex_);
        // already know key exist
        key_map_[*key] = *val;
    }

    void erase(std::string *key) {
        std::unique_lock<std::mutex> lock(mutex_);
        key_map_.erase(*key);
    }
};

class StorageMap : public Map {
private:
    leveldb::DB* db_;
    leveldb::Cache* cache_;
    leveldb::WriteOptions write_options_;
public:
    StorageMap(KVS_CONT *conts, int k, int r) {
        cache_ = leveldb::NewLRUCache(4194304);
        leveldb::Options options;
        options.create_if_missing = true;
        options.block_cache = cache_;
        options.filter_policy = NULL;
        options.reuse_logs = true;

        options.env = leveldb::NewKVEnv(leveldb::Env::Default(), conts, k, r);
        leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db_);
    }
    ~StorageMap() {
        delete cache_;
        delete db_;
    }
    bool lookup(std::string *key, phy_key *val) {
        bool exist;
        leveldb::ReadOptions options;
        std::string value;
        if (db_->Get(options, leveldb::Slice(*key), &value).ok()) {
            exist = true;
            val->decode(&value);
        }
        return exist;
    }

    void insert(std::string *key, phy_key *val) {
        leveldb::WriteBatch batch;
        std::string val_s = val->ToString();
        batch.Put(leveldb::Slice(*key), leveldb::Slice(val_s));
        db_->Write(write_options_, &batch);
    }

    void update(std::string *key, phy_key *val) {
        // already know key exist
        leveldb::WriteBatch batch;
        std::string val_s = val->ToString();
        batch.Put(leveldb::Slice(*key), leveldb::Slice(val_s));
        db_->Write(write_options_, &batch);
    }

    void erase(std::string *key) {
        leveldb::WriteBatch batch;
        batch.Delete(leveldb::Slice(*key));
        db_->Write(write_options_, &batch);
    }

};

Map* NewMemMap() {
    return new MemMap;
}

Map* NewStorageMap(KVS_CONT *conts, int k, int r) {
    return new StorageMap(conts, k, r);
}