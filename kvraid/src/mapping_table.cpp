/* mapping_table.cpp
* 07/03/2019
* by Mian Qin
*/
#include <fstream>

#include <map>
#include <unordered_map>
#include <mutex>
#include <pthread.h> 
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/cache.h"
#include "leveldb/write_batch.h"
#include "mapping_table.h"
#include "libcuckoo/cuckoohash_map.hh"

class MemMap : public Map {
friend class MemMapIterator;
private:
    std::unordered_map<std::string, phy_key> key_map_;
    std::mutex lock_;

    int serializedSize();
    void serialize(char *filename);
    void deserialize(char *filename);
public:
    class MemMapIterator : public MapIterator {
    private:
        MemMap *map_;
        std::unordered_map<std::string, phy_key>::iterator it_;
        std::string curr_key_;
        std::string curr_val_;
    public:
        explicit MemMapIterator(MemMap *map) : map_(map), it_(NULL) {}
        ~MemMapIterator() {}
        void Seek(std::string &key) {
            std::lock_guard<std::mutex> guard(map_->lock_);
            //it_ = map_->key_map_.lower_bound(key);
            it_ = map_->key_map_.end();
            if (it_ != map_->key_map_.end()) {
                curr_key_ = it_->first;
                curr_val_ = it_->second.ToString();
            }
        }
        void SeekToFirst() {
            std::lock_guard<std::mutex> guard(map_->lock_);
            it_ = map_->key_map_.begin();
            if (it_ != map_->key_map_.end()) {
                curr_key_ = it_->first;
                curr_val_ = it_->second.ToString();
            }
            
        }
        void Next() {
            std::lock_guard<std::mutex> guard(map_->lock_);
            ++it_;
            if (it_ != map_->key_map_.end()) {
                curr_key_ = it_->first;
                curr_val_ = it_->second.ToString();
            }
            
        }
        bool Valid() {
            std::lock_guard<std::mutex> guard(map_->lock_);
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
        key_map_.reserve(100e6); // reserve 100M buckets (avoid rehashing)
        std::ifstream f("mapping_table.log",std::ifstream::in|std::ios::binary);
        if (f) {
            deserialize("mapping_table.log");
        }
    };
    ~MemMap() {
        serialize("mapping_table.log");
    };

    bool lookup(std::string *key, phy_key *val) {
        std::lock_guard<std::mutex> guard(lock_);
        auto it = key_map_.find(*key);
        bool exist = (it != key_map_.end());
        if (exist) *val = it->second;
        
        return exist;
    }

    bool readmodifywrite(std::string* key, phy_key* rd_val, phy_key* wr_val) {
        bool exist;
        std::lock_guard<std::mutex> guard(lock_);
        auto it = key_map_.find(*key);
        assert(it != key_map_.end());
        exist = (it != key_map_.end());
        if (exist) {
            *rd_val = it->second;
            key_map_[*key] = *wr_val;
        }
        
        return exist;
    }

    bool readtestupdate(std::string* key, phy_key* rd_val, phy_key* old_val, phy_key* new_val) {
        bool match;
        std::lock_guard<std::mutex> guard(lock_);
        auto it = key_map_.find(*key);
        assert(it != key_map_.end());
        *rd_val = it->second;
        match = (*rd_val == *old_val);
        if (match) { // active KV not being updated
            key_map_[*key] = *new_val;
        }
        else { // active KV got updated before REPLACE
            //printf("rare case when doing GC\n"); // TODO
        }
        
        return match;
    }

    void insert(std::string *key, phy_key *val) {
        std::lock_guard<std::mutex> guard(lock_);
        key_map_.insert(std::make_pair(*key, *val));
        
    }

    void update(std::string *key, phy_key *val) {
        std::lock_guard<std::mutex> guard(lock_);
        // already know key exist
        key_map_[*key] = *val;
        
    }

    void erase(std::string *key) {
        std::lock_guard<std::mutex> guard(lock_);
        key_map_.erase(*key);
        
    }

    MapIterator* NewMapIterator() {
        return NULL;
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
    char *buf = (char *)malloc(size);
    char *data = buf;
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
        *(uint64_t *)data = it->second.get_phykey();
        data += sizeof(uint64_t);
    }
    // write to file
    std::ofstream ofs(filename, std::ofstream::out|std::ios::binary);
    ofs.write(buf, size);

    // clean up
    free(buf);
}

void MemMap::deserialize(char *filename) {
    std::ifstream ifs(filename, std::ifstream::in|std::ios::binary);
    // create and open an archive for input
    uint64_t blob_size;
    ifs.read((char*)&blob_size, sizeof(uint64_t));
    char *data = (char *)malloc(blob_size);
    ifs.read(data, blob_size);
    // read from archive to data structure
    char *p = data;
    while (blob_size > 0) {
        // key len (u8)
        uint8_t key_size = *(uint8_t *)p;
        p += sizeof(uint8_t);
        blob_size -= sizeof(uint8_t);
        // log_key
        std::string logkey(p, key_size);
        p += key_size;
        blob_size -= key_size;
        // phy_key
        phy_key phykey(*(uint64_t *)p);
        p += sizeof(uint64_t);
        blob_size -= sizeof(uint64_t);

        key_map_.insert(std::make_pair(logkey, phykey));
    }

    // clean up
    free(data);
}

class CuckooMap : public Map {
    friend class CuckooMapIterator;
private:
    cuckoohash_map<std::string, phy_key> key_map_;

    int serializedSize();
    void serialize(char *filename);
    void deserialize(char *filename);
public:
   class CuckooMapIterator : public MapIterator {
    private:
        cuckoohash_map<std::string, phy_key> *key_map_;
        cuckoohash_map<std::string, phy_key>::locked_table::iterator it_;
        std::string curr_key_;
        std::string curr_val_;
    public:
        explicit CuckooMapIterator(cuckoohash_map<std::string, phy_key> *map) : key_map_(map) {
        }
        ~CuckooMapIterator() {
        }
        void Seek(std::string &key) {
            auto lt = key_map_->lock_table();
            it_ = lt.find(key);
            curr_key_ = it_->first;
            curr_val_ = it_->second.ToString();
        }
        void SeekToFirst() {
            auto lt = key_map_->lock_table();
            it_ = lt.begin();
            if (it_ != lt.end()) {
                curr_key_ = it_->first;
                curr_val_ = it_->second.ToString();
            }
            
        }
        void Next() {
            auto lt = key_map_->lock_table();
            ++it_;
            if (it_ != lt.end()) {
                curr_key_ = it_->first;
                curr_val_ = it_->second.ToString();
            }
            
        }
        bool Valid() {
            auto lt = key_map_->lock_table();
            return it_ != lt.end();
            
        }
        std::string& Key() {
            return curr_key_;
        }
        std::string& Value() {
            return curr_val_;
        }
    };
    CuckooMap() {
        key_map_.reserve(100e6); // reserve 100M buckets (avoid rehashing)
        std::ifstream f("mapping_table.log",std::ifstream::in|std::ios::binary);
        if (f) {
            deserialize("mapping_table.log");
        }
    };
    ~CuckooMap() {
        serialize("mapping_table.log");
    };

    bool lookup(std::string *key, phy_key *val) {
        bool exist = key_map_.find(*key, *val);
        
        return exist;
    }

    bool readmodifywrite(std::string* key, phy_key* rd_val, phy_key* wr_val) {
        bool exist = key_map_.find(*key, *rd_val);
        assert(exist);
        if (exist) {
            key_map_.update(*key, *wr_val);
        }
        
        return exist;
    }

    bool readtestupdate(std::string* key, phy_key* rd_val, phy_key* old_val, phy_key* new_val) {
        bool match;
        bool exist = key_map_.find(*key, *rd_val);
        assert(exist);
        match = (*rd_val == *old_val);
        if (match) { // active KV not being updated
            key_map_.update(*key, *new_val);
        }
        else { // active KV got updated before REPLACE
            //printf("rare case when doing GC\n"); // TODO
        }
        
        return match;
    }

    void insert(std::string *key, phy_key *val) {
        key_map_.insert(*key, *val);
        
    }

    void update(std::string *key, phy_key *val) {
        key_map_.update(*key, *val);
        
    }

    void erase(std::string *key) {
        key_map_.erase(*key);
        
    }

    MapIterator* NewMapIterator() {
        return new CuckooMapIterator(&key_map_);
    }
};

int CuckooMap::serializedSize() {
    int size = 0;
    auto lt = key_map_.lock_table();
    for (const auto &it : lt) {
        // log_key str, len(u8), phy_key(u64)
        size += it.first.size() + sizeof(uint8_t) + sizeof(uint64_t);
    }
    return size + sizeof(uint64_t); // first u64, blob size;
}

void CuckooMap::serialize(char *filename) {
    // save data to archive
    uint64_t size = serializedSize();
    char *buf = (char *)malloc(size);
    char *data = buf;
    *(uint64_t *)data = size - sizeof(uint64_t);
    data += sizeof(uint64_t);
    auto lt = key_map_.lock_table();
    for (const auto &it : lt) {
        uint8_t key_size = (uint8_t)it.first.size();
        // log_key len (u8)
        *(uint8_t *)data = key_size;
        data += sizeof(uint8_t);
        // log_key str 
        memcpy(data, it.first.c_str(), key_size);
        data += key_size;
        // phy_key
        *(uint64_t *)data = it.second.get_phykey();
        data += sizeof(uint64_t);
    }
    // write to file
    std::ofstream ofs(filename, std::ofstream::out|std::ios::binary);
    ofs.write(buf, size);

    // clean up
    free(buf);
}

void CuckooMap::deserialize(char *filename) {
    std::ifstream ifs(filename, std::ifstream::in|std::ios::binary);
    // create and open an archive for input
    uint64_t blob_size;
    ifs.read((char*)&blob_size, sizeof(uint64_t));
    char *data = (char *)malloc(blob_size);
    ifs.read(data, blob_size);
    // read from archive to data structure
    char *p = data;
    while (blob_size > 0) {
        // key len (u8)
        uint8_t key_size = *(uint8_t *)p;
        p += sizeof(uint8_t);
        blob_size -= sizeof(uint8_t);
        // log_key
        std::string logkey(p, key_size);
        p += key_size;
        blob_size -= key_size;
        // phy_key
        phy_key phykey(*(uint64_t *)p);
        p += sizeof(uint64_t);
        blob_size -= sizeof(uint64_t);

        key_map_.insert(logkey, phykey);
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
    leveldb::Options options;
public:
    class StorageMapIterator : public MapIterator {
    private:
        StorageMap *map_;
        leveldb::Iterator *it_;
        std::string curr_key_;
        std::string curr_val_;
    public:
        explicit StorageMapIterator(StorageMap *map):map_(map), it_(NULL){
            leveldb::ReadOptions rdopts;
            it_ = map_->db_->NewIterator(rdopts);
        }
        ~StorageMapIterator() {if(it_ != NULL) delete it_;}
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
        cache_ = leveldb::NewLRUCache(1073741824);
        //cache_ = NULL;
        options.create_if_missing = true;
        options.block_cache = cache_;
        options.max_open_files = 100000;
	    options.max_file_size = 4000<< 10;
        options.write_buffer_size = 4000 << 10;

        options.filter_policy = NULL;
        options.reuse_logs = true;

        options.env = leveldb::NewKVEnv(leveldb::Env::Default(), conts, k, r);
        leveldb::Status status = leveldb::DB::Open(options, "map", &db_);
        if (!status.ok()) {
            printf("mapping table leveldb open error, exit\n");
            exit(-1);
        }
    }
    ~StorageMap() {
        delete db_;
        delete cache_;
        delete options.env;
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

    bool readmodifywrite(std::string* key, phy_key* rd_val, phy_key* wr_val) {
        bool exist;
        leveldb::ReadOptions options;
        std::string value;
        assert (db_->Get(options, leveldb::Slice(*key), &value).ok());
        exist = true;
        rd_val->decode(&value);
        // update
        leveldb::WriteBatch batch;
        std::string val_s = wr_val->ToString();
        batch.Put(leveldb::Slice(*key), leveldb::Slice(val_s));
        db_->Write(write_options_, &batch);
    
        return exist;
    }

    bool readtestupdate(std::string* key, phy_key* rd_val, phy_key* old_val, phy_key* new_val) {
        bool match;
        leveldb::ReadOptions options;
        std::string value;
        assert(db_->Get(options, leveldb::Slice(*key), &value).ok());
        rd_val->decode(&value);
        match = (*rd_val == *old_val);
        if (match) { // active KV not being updated
            // update
            leveldb::WriteBatch batch;
            std::string val_s = new_val->ToString();
            batch.Put(leveldb::Slice(*key), leveldb::Slice(val_s));
            db_->Write(write_options_, &batch);
        }
        else { // active KV got updated before REPLACE
            //printf("rare case when doing GC\n"); // TODO
        }
        return match;
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
