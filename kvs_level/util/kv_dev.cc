#include "leveldb/kv_dev.h"
#include <mutex>
#include <condition_variable>

#define ITER_BUFF 32768
#define INIT_GET_BUFF 1048576 // 1024 KB

namespace kvssd {

  typedef struct {
    char*& vbuf;
    uint32_t& actual_len;
    void* args;
  }Async_get_context;

  typedef struct {
    void (*cb) (void *);
    void* args;
  } cb_context;

  struct iterator_info{
    kvs_iterator_handle iter_handle;
    kvs_iterator_list iter_list;
    int has_iter_finish;
    kvs_iterator_option g_iter_mode;
  };

  void on_io_complete(kvs_callback_context* ioctx) {
    if (ioctx->result != 0 && ioctx->result != KVS_ERR_KEY_NOT_EXIST) {
      printf("io error: op = %d, key = %s, result = 0x%x, err = %s\n", ioctx->opcode, ioctx->key ? (char*)ioctx->key->key:0, ioctx->result, kvs_errstr(ioctx->result));
      exit(1);
    }
    
    sem_post((sem_t *)ioctx->private2);
    cb_context *cb_ctx = (cb_context *)ioctx->private1;
    switch (ioctx->opcode) {
    case IOCB_ASYNC_PUT_CMD : {
      void (*callback_put) (void *) = cb_ctx->cb;
      void *args_put = cb_ctx->args;
      if (callback_put != NULL) {
        callback_put((void *)args_put);
      }
      if(ioctx->key) free(ioctx->key);
      if(ioctx->value) free(ioctx->value);
      break;
    }
    case IOCB_ASYNC_GET_CMD : {
      void (*callback_get) (void *) = cb_ctx->cb;
      Async_get_context *args_get = (Async_get_context *)cb_ctx->args;
      args_get->vbuf = (char*) ioctx->value->value;
      args_get->actual_len = ioctx->value->actual_value_size;
      if (callback_get != NULL) {
        callback_get((void *)args_get->args);
      }
      if(ioctx->key) free(ioctx->key);
      if(ioctx->value) free(ioctx->value);
      break;
    }
    case IOCB_ASYNC_DEL_CMD : {
      void (*callback_del) (void *) = cb_ctx->cb;
      void *args_del = cb_ctx->args;
      if (callback_del != NULL) {
        callback_del((void *)args_del);
      }
      if(ioctx->key) free(ioctx->key);
      break;
    }
    default : {
      printf("aio cmd error \n");
      break;
    }
    }
    delete cb_ctx;
    return;
  }

  bool KV_DEV::kv_exist (leveldb::Slice *key) {
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    uint8_t result_buf[1];
    const kvs_exist_context exist_ctx = {NULL, NULL};
    kvs_exist_tuples(cont_->cont_handle, 1, &kvskey, 1, result_buf, &exist_ctx);
    //printf("[kv_exist] key: %s, existed: %d\n", std::string(key->data(),key->size()).c_str(), (int)result_buf[0]&0x1 == 1);
    return result_buf[0]&0x1 == 1;
  }

  uint32_t KV_DEV::kv_get_size(leveldb::Slice *key) {
    kvs_tuple_info info;

    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    kvs_result ret = kvs_get_tuple_info(cont_->cont_handle, &kvskey, &info);
    if (ret != KVS_SUCCESS) {
        printf("get info tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }
    //printf("[kv_get_size] key: %s, size: %d\n", std::string(key->data(),key->size()).c_str(), info.value_length);
    return info.value_length;
  }

  kvs_result KV_DEV::kv_store(leveldb::Slice *key, leveldb::Slice *val) {
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    const kvs_store_context put_ctx = {option, 0, 0};
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    const kvs_value kvsvalue = { (void *)val->data(), val->size(), 0, 0 /*offset */};
    kvs_result ret = kvs_store_tuple(cont_->cont_handle, &kvskey, &kvsvalue, &put_ctx);

    if (ret != KVS_SUCCESS) {
        printf("STORE tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }
    //printf("[kv_store] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), val->size());
    stats.num_store.fetch_add(1, std::memory_order_relaxed);
    stats.write_bytes.fetch_add(val->size(), std::memory_order_relaxed);
    return ret;
  }

  kvs_result KV_DEV::kv_store_async(leveldb::Slice *key, leveldb::Slice *val, void (*callback)(void *), void *args) {
    sem_wait(&q_sem);
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    cb_context *cb_ctx = new cb_context {callback, args};

    const kvs_store_context put_ctx = {option, (void *)cb_ctx, (void *)&q_sem};
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *)key->data();
    kvskey->length = (uint8_t)key->size();
    kvs_value *kvsvalue = (kvs_value*)malloc(sizeof(kvs_value));
    kvsvalue->value = (void *)val->data();
    kvsvalue->length = val->size();
    kvsvalue->actual_value_size = kvsvalue->offset = 0;
    kvs_result ret = kvs_store_tuple_async(cont_->cont_handle, kvskey, kvsvalue, &put_ctx, on_io_complete);

    if (ret != KVS_SUCCESS) {
        printf("kv_store_async error %s\n", kvs_errstr(ret));
        exit(1);
    }
    stats.num_store.fetch_add(1, std::memory_order_relaxed);
    stats.write_bytes.fetch_add(val->size(), std::memory_order_relaxed);
    return ret;
  }

  // (not support)
  kvs_result KV_DEV::kv_append(leveldb::Slice *key, leveldb::Slice *val) {
    kvs_store_option option;
    option.st_type = KVS_STORE_APPEND;
    option.kvs_store_compress = false;

    const kvs_store_context put_ctx = {option, 0, 0};
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size()};
    const kvs_value kvsvalue = { (void *)val->data(), val->size(), 0, 0 /*offset */};
    kvs_result ret = kvs_store_tuple(cont_->cont_handle, &kvskey, &kvsvalue, &put_ctx);

    if (ret != KVS_SUCCESS) {
        printf("APPEND tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }
    //printf("[kv_append] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), val->size());
    return ret;
  }

  kvs_result KV_DEV::kv_get(const leveldb::Slice *key, char*& vbuf, int& vlen) {
    vbuf = (char *) malloc(INIT_GET_BUFF);
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    kvs_value kvsvalue = { vbuf, INIT_GET_BUFF , 0, 0 /*offset */}; //prepare 32KB buffer
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    kvs_result ret = kvs_retrieve_tuple(cont_->cont_handle, &kvskey, &kvsvalue, &ret_ctx);
    stats.num_retrieve.fetch_add(1, std::memory_order_relaxed);
    vlen = kvsvalue.actual_value_size;
    if(ret == KVS_SUCCESS|| ret==KVS_ERR_KEY_NOT_EXIST) {
      return ret;
    }
    //if (ret == KVS_ERR_BUFFER_SMALL) { // do anther IO KVS_ERR_BUFFER_SMALL not working
    
    if (INIT_GET_BUFF < vlen) {
      // implement own aligned_realloc
      char *realloc_vbuf = (char *) malloc(vlen + 4 - (vlen%4)); //4byte align
      memcpy(realloc_vbuf, vbuf, INIT_GET_BUFF);
      free(vbuf); vbuf = realloc_vbuf;
      kvsvalue.value = vbuf;
      kvsvalue.length = vlen + 4 - (vlen%4);
      kvsvalue.offset = INIT_GET_BUFF; // skip the first IO buffer
      ret = kvs_retrieve_tuple(cont_->cont_handle, &kvskey, &kvsvalue, &ret_ctx);
      stats.num_retrieve.fetch_add(1, std::memory_order_relaxed);
    }
    //printf("[kv_get] key: %s, size: %d\n",std::string(key->data(),key->size()).c_str(), vlen);
    return ret;
  }

  kvs_result KV_DEV::kv_get_async(const leveldb::Slice *key, void (*callback)(void *), void *args) {
    sem_wait(&q_sem);
    char *vbuf = (char *) malloc(INIT_GET_BUFF);
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *) key->data();
    kvskey->length = key->size();
    kvs_value *kvsvalue = (kvs_value*)malloc(sizeof(kvs_value));
    kvsvalue->value = vbuf;
    kvsvalue->length = INIT_GET_BUFF;
    kvsvalue->actual_value_size = kvsvalue->offset = 0;
  
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;

    cb_context *cb_ctx = new cb_context {callback, args};
    const kvs_retrieve_context ret_ctx = {option, (void *)cb_ctx, (void *)&q_sem};
    kvs_result ret = kvs_retrieve_tuple_async(cont_->cont_handle, kvskey, kvsvalue, &ret_ctx, on_io_complete);
   
   int retry_cnt = 0;
    while(ret != KVS_SUCCESS) {
        if (++retry_cnt >= 3) {
            printf("kv_get_async error %d, retry %d\n", ret, retry_cnt);
            exit(1);
        }
      usleep(10);
      ret = kvs_retrieve_tuple_async(cont_->cont_handle, kvskey, kvsvalue, &ret_ctx, on_io_complete);
    }

    stats.num_retrieve.fetch_add(1, std::memory_order_relaxed);
    return KVS_SUCCESS;
  }

  // offset must be 64byte aligned (not support)
  kvs_result KV_DEV::kv_pget(const leveldb::Slice *key, char*& vbuf, int count, int offset) {
    vbuf = (char *) aligned_alloc(4096, count+64);
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    kvs_value kvsvalue = { vbuf, count , 0, offset /*offset */}; 
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    kvs_result ret = kvs_retrieve_tuple(cont_->cont_handle, &kvskey, &kvsvalue, &ret_ctx);
    if(ret != KVS_SUCCESS) {
      printf("position get tuple failed with error %s\n", kvs_errstr(ret));
      exit(1);
    }
    //printf("[kv_pget] key: %s, count: %d, offset: %d\n",std::string(key->data(),key->size()).c_str(), count, offset);
    return ret;
  }

  kvs_result KV_DEV::kv_delete(leveldb::Slice *key) {
    const kvs_key  kvskey = { (void *)key->data(), (uint8_t)key->size() };
    const kvs_delete_context del_ctx = { {false}, 0, 0};
    kvs_result ret = kvs_delete_tuple(cont_->cont_handle, &kvskey, &del_ctx);

    if(ret != KVS_SUCCESS) {
        printf("delete tuple failed with error %s\n", kvs_errstr(ret));
        exit(1);
    }
    //printf("[kv_delete] key: %s\n",std::string(key->data(),key->size()).c_str());
    stats.num_delete.fetch_add(1, std::memory_order_relaxed);
    return ret;
  }

  kvs_result KV_DEV::kv_delete_async(leveldb::Slice *key, void (*callback)(void *), void *args) {
    sem_wait(&q_sem);
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *)key->data();
    kvskey->length = (uint8_t)key->size();
    cb_context *cb_ctx = new cb_context {callback, args};
    const kvs_delete_context del_ctx = { {false}, (void *)cb_ctx, (void *)&q_sem};
    kvs_result ret = kvs_delete_tuple_async(cont_->cont_handle, kvskey, &del_ctx, on_io_complete);

    if(ret != KVS_SUCCESS) {
        printf("kv_delete_async error %s\n", kvs_errstr(ret));
        exit(1);
    }
    stats.num_delete.fetch_add(1, std::memory_order_relaxed);
    return ret;
  
  }

  kvs_result KV_DEV::kv_scan_keys(std::vector<std::string>& keys) {
    struct iterator_info *iter_info = (struct iterator_info *)malloc(sizeof(struct iterator_info));
    iter_info->g_iter_mode.iter_type = KVS_ITERATOR_KEY;
    
    int ret;
    printf("start scan keys\n");
    /* Open iterator */

    kvs_iterator_context iter_ctx_open;
    iter_ctx_open.option = iter_info->g_iter_mode;
    iter_ctx_open.bitmask = 0x00000000;
    unsigned int PREFIX_KV = 0;

    iter_ctx_open.bit_pattern = 0x00000000;
    iter_ctx_open.private1 = NULL;
    iter_ctx_open.private2 = NULL;
    
    ret = kvs_open_iterator(cont_->cont_handle, &iter_ctx_open, &iter_info->iter_handle);
    if(ret != KVS_SUCCESS) {
      printf("iterator open fails with error 0x%x - %s\n", ret, kvs_errstr(ret));
      free(iter_info);
      exit(1);
    }
      
    /* Do iteration */
    iter_info->iter_list.size = ITER_BUFF;
    uint8_t *buffer;
    buffer =(uint8_t*) kvs_malloc(ITER_BUFF, 4096);
    iter_info->iter_list.it_list = (uint8_t*) buffer;

    kvs_iterator_context iter_ctx_next;
    iter_ctx_next.option = iter_info->g_iter_mode;
    iter_ctx_next.private1 = iter_info;
    iter_ctx_next.private2 = NULL;

    while(1) {
      iter_info->iter_list.size = ITER_BUFF;
      memset(iter_info->iter_list.it_list, 0, ITER_BUFF);
      ret = kvs_iterator_next(cont_->cont_handle, iter_info->iter_handle, &iter_info->iter_list, &iter_ctx_next);
      if(ret != KVS_SUCCESS) {
        printf("iterator next fails with error 0x%x - %s\n", ret, kvs_errstr(ret));
        free(iter_info);
        exit(1);
      }
          
      uint8_t *it_buffer = (uint8_t *) iter_info->iter_list.it_list;
      uint32_t key_size = 0;
      
      for(int i = 0;i < iter_info->iter_list.num_entries; i++) {
        // get key size
        key_size = *((unsigned int*)it_buffer);
        it_buffer += sizeof(unsigned int);

        // add key
        keys.push_back(std::string((char *)it_buffer, key_size));
        it_buffer += key_size;
      }
          
      if(iter_info->iter_list.end) {
        break;
      } 
    }

    /* Close iterator */
    kvs_iterator_context iter_ctx_close;
    iter_ctx_close.private1 = NULL;
    iter_ctx_close.private2 = NULL;

    ret = kvs_close_iterator(cont_->cont_handle, iter_info->iter_handle, &iter_ctx_close);
    if(ret != KVS_SUCCESS) {
      printf("Failed to close iterator\n");
      exit(1);
    }
    
    if(buffer) kvs_free(buffer);
    if(iter_info) free(iter_info);
    return KVS_SUCCESS;
  }

  class Monitor {
  public:
      std::mutex mtx_;
      std::condition_variable cv_;
      bool ready_ ;
      Monitor() : ready_(false) {}
      ~Monitor(){}
      void reset() {ready_ = false;};
      void notify() {
          std::unique_lock<std::mutex> lck(mtx_);
          ready_ = true;
          cv_.notify_one();
      }
      void wait() {
          std::unique_lock<std::mutex> lck(mtx_);
          while (!ready_) cv_.wait(lck);
      }
  };

  static void monitor_complete(void *args) {
    Monitor * mon = (Monitor *)args;
    mon->notify();
  }

  bool KV_DEV_ARRAY::kv_exist (leveldb::Slice *key) {
    return kvssds_[0].kv_exist(key);
  }

  uint32_t KV_DEV_ARRAY::kv_get_size(leveldb::Slice *key) {
    uint32_t size = kvssds_[0].kv_get_size(key);
    if (strncmp("ldb", key->data()+key->size()-3, 3) != 0) 
      return size;
    else
      return size * k_;
  }

  kvs_result KV_DEV_ARRAY::kv_store(leveldb::Slice *key, leveldb::Slice *val) {
    if (strncmp("ldb", key->data()+key->size()-3, 3) != 0) {// mirror
      Monitor *mons = new Monitor[r_];
      for (int i = 0; i < r_ ; i++) {
        kvssds_[i].kv_store_async(key, val, monitor_complete, &mons[i]);
      }
      for (int i = 0; i < r_ ; i++) mons[i].wait();
      delete [] mons;
    }
    else { // EC
      int dev_id_start = atoi(key->data()+key->size()-7) % (k_+r_);
      // prepare ec buffer
      int chunk_size = ((val->size() + sizeof(int) )/k_ / EC_codeword + 1) * EC_codeword;
      char **data = (char **)malloc(k_*sizeof(char*));
      char **code = (char **)malloc(r_*sizeof(char*));

      for (int i = 0; i < k_ ; i++) {
        data[i] = (char *)aligned_alloc(4096, chunk_size);
        if ((i+1)*chunk_size < val->size())
          memcpy(data[i], val->data()+i*chunk_size, chunk_size);
        else {
          memcpy(data[i], val->data()+i*chunk_size, val->size() - i*chunk_size);
          // add footer
          int *p = (int *)(data[i] + chunk_size - sizeof(int));
          *p = val->size();
        }
      }
      for (int i = 0; i < r_ ; i++) {
        code[i] = (char *)aligned_alloc(4096, chunk_size);
      }

      ec_.encode(data, code, chunk_size);

      Monitor *mons = new Monitor[k_+r_];
      leveldb::Slice *vals = (leveldb::Slice *)malloc(sizeof(leveldb::Slice) * (k_+r_));
      for (int i = 0; i < k_+r_ ; i++) {
        if(i < k_) {
          (void) new (&vals[i]) leveldb::Slice(data[i], chunk_size);
        }
        else {
          (void) new (&vals[i]) leveldb::Slice(code[i-k_], chunk_size);
        }
        kvssds_[(i+dev_id_start) % (k_+r_)].kv_store_async(key, &vals[i], monitor_complete, &mons[i]);
      }
      for (int i = 0; i < k_+r_ ; i++) mons[i].wait();

      //clean up
      delete [] mons;
      for (int i = 0; i < k_ ; i++) free(data[i]);
      for (int i = 0; i < r_ ; i++) free(code[i]);
      free(data); free(code);
      free(vals);
    }
    return KVS_SUCCESS;
  }

  kvs_result KV_DEV_ARRAY::kv_get(const leveldb::Slice *key, char*& vbuf, int& vlen) {
    if (strncmp("ldb", key->data()+key->size()-3, 3) != 0) {// mirror
      kvssds_[0].kv_get(key, vbuf, vlen);
    }
    else { // EC
      int dev_id_start = atoi(key->data()+key->size()-7) % (k_+r_);

      char **vbufs = (char **)malloc(sizeof(char*) * k_);
      Monitor *mons = new Monitor[k_];
      uint32_t *vlens = new uint32_t[k_];
      Async_get_context *ctxs = (Async_get_context *)malloc(sizeof(Async_get_context) * k_);
      for (int i = 0; i < k_; i++) {
        (void) new (&ctxs[i]) Async_get_context{vbufs[i], vlens[i], (void *)&mons[i]};
        kvssds_[(i+dev_id_start)%(k_+r_)].kv_get_async(key, monitor_complete, &ctxs[i]);
      }

      for (int i = 0; i < k_ ; i++) mons[i].wait();

      // restore value
      int chunk_size = vlens[0];
      int *p = (int*)(vbufs[k_-1] + chunk_size - sizeof(int));
      vlen = *p;
      vbuf = (char *) malloc(vlen);
      for (int i = 0; i < k_; i++) {
        if ((i+1)*chunk_size <= vlen)
          memcpy(vbuf+i*chunk_size, vbufs[i], chunk_size);
        else
          memcpy(vbuf+i*chunk_size, vbufs[i], vlen - i*chunk_size);
      }

      //clean up
      delete [] mons;
      delete [] vlens;
      free(ctxs);
      for (int i = 0; i < k_; i++) free(vbufs[i]);
      free(vbufs);
    }
    return KVS_SUCCESS;
  }

  kvs_result KV_DEV_ARRAY::kv_delete(leveldb::Slice *key) {
    if (strncmp("ldb", key->data()+key->size()-3, 3) != 0) {// mirror
      Monitor *mons = new Monitor[r_];
      for (int i = 0; i < r_; i++)
        kvssds_[i].kv_delete_async(key, monitor_complete, &mons[i]);
      for (int i = 0; i < r_; i++) mons[i].wait();
      delete [] mons;
    }
    else { //EC
      Monitor *mons = new Monitor[k_+r_];
      for (int i = 0; i < k_+r_; i++)
        kvssds_[i].kv_delete_async(key, monitor_complete, &mons[i]);
      for (int i = 0; i < k_+r_; i++) mons[i].wait();
      delete [] mons;
    }
    return KVS_SUCCESS;
  }

  kvs_result KV_DEV_ARRAY::kv_scan_keys(std::vector<std::string> &keys) {
    return KVS_SUCCESS;
  }

}
