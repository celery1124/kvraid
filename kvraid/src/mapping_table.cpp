/* mapping_table.cpp
* 07/03/2019
* by Mian Qin
*/
#include <fstream>

#include <map>
#include <mutex>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/cache.h"
#include "leveldb/write_batch.h"
#include "mapping_table.h"

class MemMap : public Map {
friend class MemMapIterator;
private:
    std::map<std::string, phy_key> key_map_;
    std::mutex mutex_;

    int serializedSize();
    void serialize(char *filename);
    void deserialize(char *filename);
public:
    class MemMapIterator : public MapIterator {
    private:
        MemMap *map_;
        std::map<std::string, phy_key>::iterator it_;
        std::string curr_key_;
        std::string curr_val_;
    public:
        explicit MemMapIterator(MemMap *map) : map_(map), it_(NULL) {}
        ~MemMapIterator() {}
        void Seek(std::string &key) {
            std::unique_lock<std::mutex> lock(map_->mutex_);
            it_ = map_->key_map_.find(key);
            if (it_ != map_->key_map_.end()) {
                curr_key_ = it_->first;
                curr_val_ = it_->second.ToString();
            }
        }
        void SeekToFirst() {
            std::unique_lock<std::mutex> lock(map_->mutex_);
            it_ = map_->key_map_.begin();
            if (it_ != map_->key_map_.end()) {
                curr_key_ = it_->first;
                curr_val_ = it_->second.ToString();
            }
        }
        void Next() {
            std::unique_lock<std::mutex> lock(map_->mutex_);
            ++it_;
            if (it_ != map_->key_map_.end()) {
                curr_key_ = it_->first;
                curr_val_ = it_->second.ToString();
            }
        }
        bool Valid() {
            std::unique_lock<std::mutex> lock(map_->mutex_);
            return it_ != map_->key_map_.end();
        }
        std::string& Key() {
            return curr_key_;
        }
        std::string& Value() {
            return curr_val_;
        }
    };
    MemMap() {
        std::ifstream f("mapping_table.log");
        if (f.good()) {
            deserialize("mapping_table.log");
        }
    };
    ~MemMap() {
        serialize("mapping_table.log");
    };

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

    MapIterator* NewMapIterator() {
        return new MemMapIterator(this);
    }
};

int MemMap::serializedSize() {
    int size = 0;
    for (auto it = key_map_.begin(); it != key_map_.end(); ++it) {
        // log_key str, len(u8), phy_key(u64)
        size += it->first.size() + sizeof(uint8_t) + sizeof(uint64_t);
    }
    return size + sizeof(uint64_t); // first u64, blob size;
}

void MemMap::serialize(char *filename) {
    // save data to archive
    uint64_t size = serializedSize();
    char *data = (char *)malloc(size);
    *(uint64_t *)data = size - sizeof(uint64_t);
    data += sizeof(uint64_t);
    for (auto it = key_map_.begin(); it != key_map_.end(); ++it) {
        uint8_t key_size = (uint8_t)it->first.size();
        // log_key len (u8)
        *(uint8_t *)data = key_size;
        data += sizeof(uint8_t);
        // log_key str 
        memcpy(data, it->first.c_str(), key_size);
        data += key_size;
        // phy_key
        *(uint64_t *)data = it->second.get_seq();
        data += sizeof(uint64_t);
    }
    // write to file
    std::ofstream ofs(filename, std::ios::binary);
    ofs.write(data, size);

    // clean up
    free(data);
}

void MemMap::deserialize(char *filename) {
    std::ifstream ifs(filename, std::ios::binary);
    // create and open an archive for input
    uint64_t blob_size;
    ifs.read((char*)&blob_size, sizeof(uint64_t));
    char *data = (char *)malloc(blob_size);
    ifs.read(data, blob_size);
    // read from archive to data structure
    while (blob_size > 0) {
        // key len (u8)
        uint8_t key_size = *(uint8_t *)data;
        data += sizeof(uint8_t);
        blob_size -= sizeof(uint8_t);
        // log_key
        std::string logkey(data, key_size);
        data += key_size;
        blob_size -= key_size;
        // phy_key
        phy_key phykey(*(uint64_t *)data);
        data += sizeof(uint64_t);
        blob_size -= sizeof(uint64_t);

        key_map_.insert(std::make_pair(logkey, phykey));
    }

    // clean up
    free(data);
}

class StorageMap : public Map {
friend class StorageMapIterator;
private:
    leveldb::DB* db_;
    leveldb::Cache* cache_;
    leveldb::WriteOptions write_options_;
public:
    class StorageMapIterator : public MapIterator {
    private:
        StorageMap *map_;
        leveldb::Iterator *it_;
        std::string curr_key_;
        std::string curr_val_;
    public:
        explicit StorageMapIterator(StorageMap *map):map_(map) {
            leveldb::ReadOptions rdopts;
            it_ = map_->db_->NewIterator(rdopts);
        }
        ~StorageMapIterator() {}
        void Seek(std::string &key) {
            it_->Seek(leveldb::Slice(key));
            if (Valid()) {
                curr_key_ = it_->key().ToString();
                curr_val_ = it_->value().ToString();
            }
        }
        void SeekToFirst() {
            it_->SeekToFirst();
            if (Valid()) {
                curr_key_ = it_->key().ToString();
                curr_val_ = it_->value().ToString();
            }
        }
        void Next() {
            it_->Next();
            if (Valid()) {
                curr_key_ = it_->key().ToString();
                curr_val_ = it_->value().ToString();
            }
        }
        bool Valid() {
            return it_->Valid();
        }
        std::string& Key() {
            return curr_key_;
        }
        std::string& Value() {
            return curr_val_;
        }
    };
    StorageMap(KVS_CONT *conts, int k, int r) {
        //cache_ = leveldb::NewLRUCache(4194304);
        cache_ = NULL;
        leveldb::Options options;
        options.create_if_missing = true;
        options.block_cache = cache_;
        options.max_open_files = 1000;
        options.filter_policy = NULL;
        options.reuse_logs = true;

        options.env = leveldb::NewKVEnv(leveldb::Env::Default(), conts, k, r);
        leveldb::Status status = leveldb::DB::Open(options, "map", &db_);
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

    MapIterator* NewMapIterator() {
        return new StorageMapIterator(this);
    }
};

Map* NewMemMap() {
    return new MemMap;
}

Map* NewStorageMap(KVS_CONT *conts, int k, int r) {
    return new StorageMap(conts, k, r);
}