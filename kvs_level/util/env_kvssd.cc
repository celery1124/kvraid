// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "port/port.h"
#include <string>

namespace leveldb {

class KVSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  void *mapped_region_;
  size_t length_;
  size_t offset_;

 public:
  KVSequentialFile(const std::string& fname, void *base, size_t length)
      : filename_(fname),mapped_region_(base), length_(length), offset_(0) {
  }

  virtual ~KVSequentialFile() {
    free(mapped_region_);
  }

  // n is aligned with 64B
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    if (offset_ + n > length_) { // return rest of the region
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset_, length_ - offset_);
      offset_ = length_;
      //s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset_, n);
      offset_ += n;
    }
    return s;
  }
  virtual Status Skip(uint64_t n) {
    offset_ += n;
    return Status::OK();
  }
};

// class KVRandomAccessFile: public RandomAccessFile {
//  private:
//   std::string filename_;
//   Slice key_;
//   kvssd::KV_DEV *kvd_;
//   size_t length_;

//  public:
//   KVRandomAccessFile(const std::string& fname, kvssd::KV_DEV *kvd, size_t length)
//       : filename_(fname), kvd_(kvd), length_(length) {
//         key_ = filename_;
//   }

//   virtual ~KVRandomAccessFile() {
//   }

//   virtual Status Read(uint64_t offset, size_t n, Slice* result,
//                       char* scratch) const {
//     Status s;
//     char *vbuf;
//     int aligned_offset = offset & (~0x3f); // aligned offset to 64bytes
//     int mini_offset = offset & 0x3f;

//     if (offset + n > length_) { // return rest of the region
//       kvd_->kv_pget(&key_, vbuf, length_-aligned_offset, aligned_offset);
//       memcpy(scratch, vbuf+mini_offset, length_-offset);
//       *result = Slice(scratch, length_ - offset);
//     } else {
//       kvd_->kv_pget(&key_, vbuf, n+mini_offset, aligned_offset);
//       memcpy(scratch, vbuf+mini_offset, n);
//       *result = Slice(scratch, n);
//     }
//     free (vbuf);
//     return s;
//   }
// };

class KVRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mapped_region_;
  size_t length_;

 public:
  KVRandomAccessFile(const std::string& fname, void* base, size_t length)
      : filename_(fname), mapped_region_(base), length_(length) {
  }

  virtual ~KVRandomAccessFile() {
    free(mapped_region_);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) { // return rest of the region
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset, length_ - offset);
      //s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mapped_region_) + offset, n);
    }
    return s;
  }
};

class KVWritableFile : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  kvssd::KV_DEV_ARRAY* kvd_;
  bool synced;

 public:
  KVWritableFile(kvssd::KV_DEV_ARRAY* kvd, const std::string& fname)
      : filename_(fname), kvd_(kvd), synced(false) {  }

  ~KVWritableFile() { }

  virtual Status Append(const Slice& data) {
    value_.append(data.data(), data.size());
    return Status::OK();
  }

  virtual Status Reset() {
    value_.clear();
    return Status::OK();
  }

  virtual Status Close() {
    if (!synced) Sync();
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    //Status s = SyncDirIfManifest();
    Status s;
    Slice key (filename_);
    Slice val (value_);
    kvd_->kv_store(&key, &val);
    synced = true;
    //printf("KVWritable: %s, size %d bytes\n",filename_.c_str(), val.size());
    return s;
  }
};

class KVAppendableFile : public WritableFile {
 private:
  std::string filename_;
  std::string value_;
  kvssd::KV_DEV_ARRAY* kvd_;

 public:
  KVAppendableFile(kvssd::KV_DEV_ARRAY* kvd, const std::string& fname)
      : filename_(fname), kvd_(kvd){ 
        Slice key (filename_);
        char *vbuf;
        int size;
        kvd_->kv_get(&key, vbuf, size);
        value_.append(vbuf, size);
        free(vbuf);
       }

  ~KVAppendableFile() { }

  virtual Status Append(const Slice& data) {
    value_.append(data.data(), data.size());
    return Status::OK();
  }

  virtual Status Reset() {
    value_.clear();
    return Status::OK();
  }

  virtual Status Close() {
    return Status::OK();
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    //Status s = SyncDirIfManifest();
    Status s;
    Slice key (filename_);
    Slice val (value_);
    kvd_->kv_store(&key, &val);
    //printf("append: %s\n",filename_.c_str());
    return s;
  }
};

class KVSSDEnv : public EnvWrapper {
  private:
    int k_; // # data devs
    int r_; // # parity devs
    kvssd::KV_DEV* kvds_;
    kvssd::KV_DEV_ARRAY* kvd_;
  public:
  explicit KVSSDEnv(Env* base_env, KVS_CONT *kvs_conts, int k, int r) : 
  EnvWrapper(base_env), k_(k), r_(r) {
    kvds_ = (kvssd::KV_DEV *)malloc(sizeof(kvssd::KV_DEV) * (k+r));
    for (int i = 0; i < (k+r); i++) {
      (void) new (&kvds_[i]) kvssd::KV_DEV(&kvs_conts[i]);
    }
    kvd_ = new kvssd::KV_DEV_ARRAY(k_, r_, kvds_);
  }
  virtual ~KVSSDEnv() {
		free(kvds_);
    delete kvd_;
  }

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    *result = NULL;
    char * base;
    int size;
    Slice key (fname);
    kvd_->kv_get(&key, base, size);
    *result = new KVSequentialFile (fname, base, size);
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    char * base;
    int size;
    Slice key (fname);
    kvd_->kv_get(&key, base, size);
    *result = new KVRandomAccessFile (fname, base, size);
    return Status::OK();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    *result = new KVWritableFile(kvd_, fname);
    return Status::OK();
  }

  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result) {
    *result = new KVAppendableFile(kvd_, fname);
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) {
    Slice key(fname);
    return kvd_->kv_exist(&key);
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    std::vector<std::string> keys;
    kvd_->kv_scan_keys(keys);

    for (auto it = keys.begin(); it != keys.end(); ++it){
      const std::string& filename = *it;
      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' &&
          Slice(filename).starts_with(Slice(dir))) {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }

    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    Slice key(fname);
    if (!kvd_->kv_exist(&key)) {
      return Status::IOError(fname, "KV not found");
    }
    kvd_->kv_delete(&key);
    return Status::OK();
  }

  virtual Status CreateDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status DeleteDir(const std::string& dirname) {
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) {
    Slice key(fname);
    if (!kvd_->kv_exist(&key)) {
      return Status::IOError(fname, "KV not found");
    }

    *file_size = kvd_->kv_get_size(&key);
    return Status::OK();
  }

  // expensive, probably don't using it
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    Slice key_src(src);
    if (!kvd_->kv_exist(&key_src)) {
      return Status::IOError(src, "KV not found");
    }
    Slice key_target(target);
    char *vbuf;
    int vlen;
    kvd_->kv_get(&key_src, vbuf, vlen);
    Slice val_src (vbuf, vlen);
    kvd_->kv_store(&key_target, &val_src);
    kvd_->kv_delete(&key_src);

    return Status::OK();
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) {
    return Status::OK();
  }

  virtual Status GetTestDirectory(std::string* path) {
    *path = "/test";
    return Status::OK();
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    *result = NULL;
    return Status::OK();
  }
};

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;

Env* NewKVEnv(Env* base_env, KVS_CONT *kvs_conts, int k, int r) {
  return new KVSSDEnv(base_env, kvs_conts, k, r);
}

}  // namespace leveldb
