/* kv_interface.cpp
* 03/13/2017
* by Mian Qin
*/

#include <sys/time.h>
#include "kv_device.h"

//#define IO_DEBUG
#define INIT_GET_BUFF 4096 // 4kB (mainly for KVRaid meta data)

#ifdef IO_DEBUG
struct timeval tp;
extern long int ts;
#endif

void on_io_complete(kvs_callback_context* ioctx) {
    if (ioctx->result != 0 && ioctx->result != KVS_ERR_KEY_NOT_EXIST) {
      printf("io error: op = %d, key = %s, result = 0x%x, err = %s\n", ioctx->opcode, ioctx->key ? (char*)ioctx->key->key:0, ioctx->result, kvs_errstr(ioctx->result));
      exit(1);
    }
    
    switch (ioctx->opcode) {
    case IOCB_ASYNC_PUT_CMD : {
      void (*callback_put) (void *) = (void (*)(void *))ioctx->private1;
      void *args_put = (void *)ioctx->private2;
      if (callback_put != NULL) {
        callback_put((void *)args_put);
      }
      if(ioctx->key) free(ioctx->key);
      if(ioctx->value) free(ioctx->value);
      break;
    }
    case IOCB_ASYNC_GET_CMD : {
      void (*callback_get) (void *) = (void (*)(void *))ioctx->private1;
      void *args_get = (void *)ioctx->private2;
      if (callback_get != NULL) {
        callback_get(args_get);
      }
      if(ioctx->key) free(ioctx->key);
      if(ioctx->value) free(ioctx->value);
      break;
    }
    case IOCB_ASYNC_DEL_CMD : {
      void (*callback_del) (void *) = (void (*)(void *))ioctx->private1;
      void *args_del = (void *)ioctx->private2;
      if (callback_del != NULL) {
        callback_del((void *)args_del);
      }
      if(ioctx->key) free(ioctx->key);
      break;
    }
    default : {
      printf("aio cmd error \n");
      exit(-1);
      break;
    }
    }
    return;
  }

bool KV_DEVICE::kv_store(phy_key *key, phy_val *value)
{
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;
        
    const kvs_store_context put_ctx = {option, 0, 0};
    const kvs_key  kvskey = { (void *)key->c_str(), key->get_klen()};
    const kvs_value kvsvalue = { value->c_val, value->val_len, 0, 0 /*offset */};
    int ret = kvs_store_tuple(cont_->cont_handle, &kvskey, &kvsvalue, &put_ctx);

    if (ret != KVS_SUCCESS) {
        printf("STORE tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }

#ifdef IO_DEBUG
    gettimeofday(&tp, NULL);
    printf("[%.6f] kv_device:insert key: %s, value: %s\n", ((float)(tp.tv_sec*1000000 + tp.tv_usec - ts)) / 1000000 ,ckey, cval);
#endif
    stats.num_store.fetch_add(1, std::memory_order_relaxed);
    return true;
		
}

bool KV_DEVICE::kv_store(std::string *key, std::string *value)
{
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;
        
    const kvs_store_context put_ctx = {option, 0, 0};
    const kvs_key  kvskey = { (void *)key->c_str(), key->size()};
    const kvs_value kvsvalue = { (void *)value->c_str(), value->size(), 0, 0 /*offset */};
    int ret = kvs_store_tuple(cont_->cont_handle, &kvskey, &kvsvalue, &put_ctx);

    if (ret != KVS_SUCCESS) {
        printf("STORE tuple failed with err %s\n", kvs_errstr(ret));
        exit(1);
    }

    stats.num_store.fetch_add(1, std::memory_order_relaxed);
    return true;
		
}


bool KV_DEVICE::kv_delete(phy_key *key)
{
    const kvs_key  kvskey = { (void *)key->c_str(), key->get_klen()};
    const kvs_delete_context del_ctx = { {true}, 0, 0};
    int ret = kvs_delete_tuple(cont_->cont_handle, &kvskey, &del_ctx);

    if(ret != KVS_SUCCESS) {
        printf("delete tuple failed with error 0x%x - %s, key %s\n", ret, kvs_errstr(ret), key->c_str());
        exit(1);
    }
    
#ifdef IO_DEBUG
    gettimeofday(&tp, NULL);
    printf("[%.6f] kv_device:delete key: %s\n",((float)(tp.tv_sec*1000000 + tp.tv_usec - ts)) / 1000000 , ckey);
#endif    
    stats.num_delete.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool KV_DEVICE::kv_get(phy_key *key, phy_val *value)
{
    const kvs_key  kvskey = { (void *)key->c_str(), key->get_klen() };
    kvs_value *kvsvalue = new kvs_value{ value->c_val, value->val_len , 0, 0 /*offset */};
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    int ret = kvs_retrieve_tuple(cont_->cont_handle, &kvskey, kvsvalue, &ret_ctx);
    if(ret != KVS_SUCCESS) {
        printf("retrieve tuple %s failed with error 0x%x - %s,\n", key->c_str(), ret, kvs_errstr(ret));
        exit(1);
    }
    value->actual_len = kvsvalue->actual_value_size;

    delete kvsvalue;
#ifdef IO_DEBUG
    gettimeofday(&tp, NULL);
    printf("[%.6f] kv_device:get key: %s, value: %s\n",((float)(tp.tv_sec*1000000 + tp.tv_usec - ts)) / 1000000 , ckey, ret);
#endif       
    stats.num_retrieve.fetch_add(1, std::memory_order_relaxed);
    return true;

}

bool KV_DEVICE::kv_get(std::string *key, std::string *value)
{
    const kvs_key  kvskey = { (void *)key->c_str(), key->size() };
    char *vbuf = (char *)malloc(INIT_GET_BUFF);
    kvs_value kvsvalue = { (void *)vbuf, INIT_GET_BUFF , 0, 0 /*offset */};
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, 0, 0};
    int ret = kvs_retrieve_tuple(cont_->cont_handle, &kvskey, &kvsvalue, &ret_ctx);
    if(ret != KVS_SUCCESS && ret != KVS_ERR_KEY_NOT_EXIST) {
        printf("retrieve tuple %s failed with error 0x%x - %s,\n", key->c_str(), ret, kvs_errstr(ret));
        exit(1);
    }
    value->clear();
    value->append(vbuf, kvsvalue.actual_value_size);
    
    stats.num_retrieve.fetch_add(1, std::memory_order_relaxed);
    return true;

}

bool KV_DEVICE::kv_astore(phy_key *key, phy_val *value, void (*callback)(void *), void *argument){
    kvs_store_option option;
    option.st_type = KVS_STORE_POST;
    option.kvs_store_compress = false;

    const kvs_store_context put_ctx = {option, (void *)callback, (void *)argument};
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *)key->c_str();
    kvskey->length = (uint8_t)key->get_klen();
    kvs_value *kvsvalue = (kvs_value*)malloc(sizeof(kvs_value));
    kvsvalue->value = (void *)value->c_val;
    kvsvalue->length = value->val_len;
    kvsvalue->actual_value_size = kvsvalue->offset = 0;
    kvs_result ret = kvs_store_tuple_async(cont_->cont_handle, kvskey, kvsvalue, &put_ctx, on_io_complete);

    if (ret != KVS_SUCCESS) {
        printf("kv_store_async error %s\n", kvs_errstr(ret));
        exit(1);
    }

    stats.num_store.fetch_add(1, std::memory_order_relaxed);
    return true;
}


bool KV_DEVICE::kv_adelete(phy_key *key, void (*callback)(void *), void *argument){
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *)key->c_str();
    kvskey->length = (uint8_t)key->get_klen();
    const kvs_delete_context del_ctx = { {false}, (void *)callback, (void *)argument};
    kvs_result ret = kvs_delete_tuple_async(cont_->cont_handle, kvskey, &del_ctx, on_io_complete);

    if(ret != KVS_SUCCESS) {
        printf("kv_delete_async error %s\n", kvs_errstr(ret));
        exit(1);
    }

    stats.num_delete.fetch_add(1, std::memory_order_relaxed);
    return true;
}

// value->c_val already allocated buffer (slab size)
bool KV_DEVICE::kv_aget(phy_key *key, phy_val *value, void (*callback)(void *), void *argument){
    kvs_key *kvskey = (kvs_key*)malloc(sizeof(kvs_key));
    kvskey->key = (void *) key->c_str();
    kvskey->length = key->get_klen();
    kvs_value *kvsvalue = (kvs_value*)malloc(sizeof(kvs_value));
    kvsvalue->value = value->c_val;
    kvsvalue->length = value->val_len;
    kvsvalue->actual_value_size = kvsvalue->offset = 0;
  
    kvs_retrieve_option option;
    memset(&option, 0, sizeof(kvs_retrieve_option));
    option.kvs_retrieve_decompress = false;
    option.kvs_retrieve_delete = false;
    const kvs_retrieve_context ret_ctx = {option, (void *)callback, (void *)argument};
    kvs_result ret = kvs_retrieve_tuple_async(cont_->cont_handle, kvskey, kvsvalue, &ret_ctx, on_io_complete);
    if(ret != KVS_SUCCESS) {
      printf("kv_get_async error %d\n", ret);
      exit(1);
    }

    stats.num_retrieve.fetch_add(1, std::memory_order_relaxed);
    return true;
}

// bool KV_DEVICE::kv_astore(phy_key *key, phy_val *value, void (*callback)(void *), void *argument)
// {
//     dev_io_context *dev_ctx = new dev_io_context{KVD_STORE, key, value, callback, argument, this};
//     sem_wait(&q_sem);
//     if (threadpool_add(pool, &io_task, dev_ctx, 0) < 0) {
//         printf("kv insert pool_add error, key %s\n", key->c_str());
//         exit(1);
//     }
//     return true;
// }


// bool KV_DEVICE::kv_adelete(phy_key *key, void (*callback)(void *), void *argument)
// {
//     dev_io_context *dev_ctx = new dev_io_context{KVD_DELETE, key, NULL, callback, argument, this};
//     sem_wait(&q_sem);
//     if (threadpool_add(pool, &io_task, dev_ctx, 0) < 0) {
//         printf("kv delete pool_add error, key %s\n", key->c_str());
//         exit(1);
//     }
//     return true;
// }

// bool KV_DEVICE::kv_aget(phy_key *key, phy_val *value, void (*callback)(void *), void *argument)
// {
//     dev_io_context *dev_ctx = new dev_io_context{KVD_GET, key, value, callback, argument, this};
//     sem_wait(&q_sem);
//     if (threadpool_add(pool, &io_task, dev_ctx, 0) < 0) {
//         printf("kv get pool_add error, key %s\n", key->c_str());
//         exit(1);
//     }
//     return true;
// }

int64_t KV_DEVICE::get_capacity() {
    int64_t dev_cap;
    kvs_get_device_capacity(cont_->dev, &dev_cap);
    return dev_cap;
}

double KV_DEVICE::get_util() {
    int dev_util;
    kvs_get_device_utilization(cont_->dev, &dev_util);
    return (double)dev_util/10000;
}

float KV_DEVICE::get_waf() {
    //return kvs_get_waf(dev);
    return 0;
}

#define ITER_BUFF 32768
struct iterator_info{
    kvs_iterator_handle iter_handle;
    kvs_iterator_list iter_list;
    int has_iter_finish;
    kvs_iterator_option g_iter_mode;
};

void KV_DEVICE::kv_scan_keys(std::vector<std::string>& keys) {
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
    
    return;
  }


// static void io_task(void *arg) {
//     dev_io_context* ctx = (dev_io_context *)arg;
//     int ret;
//     switch (ctx->ops) {
//         case KVD_STORE: {
//             ctx->dev->kv_store(ctx->key, ctx->value);
//             break;
//         }
//         case KVD_DELETE: {
//             ctx->dev->kv_delete(ctx->key);
//             break;

//         }
//         case KVD_GET: {
//             ctx->dev->kv_get(ctx->key, ctx->value);
//             break;
//         }
//         default: printf("op_task ops wrong\n");
//     }
//     if (ctx->callback != NULL)
//         ctx->callback(ctx->argument);
//     delete ctx;
    
// }
