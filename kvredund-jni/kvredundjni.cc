#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
#include <cstddef>
#include <fstream>
#include <iostream>
#include "com_yahoo_ycsb_db_KVredund.h"
#include "com_yahoo_ycsb_db_WriteBatch.h"
#include "com_yahoo_ycsb_db_Iterator.h"

#include "json11.h"
#include "kvr.h"
#include "kv_writebatch.h"
#include "kvraid.h"
#include "kvec.h"
#include "kvmirror.h"

KVS_CONT* kv_conts;
int num_ssds;
KVR *kvr;
Cache *cache;
int batch_size;
// int slab_list[2] = {1024, 2048};

void handler(int sig) {
  void *array[20];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 20);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

jboolean Java_com_yahoo_ycsb_db_KVredund_init(JNIEnv* env, jobject /*jdb*/) { 
    std::ifstream ifs("kvredund_config.json");
    std::string file_content( (std::istreambuf_iterator<char>(ifs) ),
                       (std::istreambuf_iterator<char>()    ) );
    std::string err;
    if (file_content.size() == 0) { // default jason
        file_content = R"({"dev_mode":0,
        "num_data_nodes":4, "num_code_nodes":2, 
        "kvr_type":0, "meta_type":0, "slab_list":[256,512,768,1024,1280],
        "dev_cap": 16,
        "batch_Size":6})";
        printf("Using default kvredund config file\n");
    }

    // parse json
    const auto config = json11::Json::parse(file_content, err);
    int dev_mode = config["dev_mode"].int_value();
    int k = config["num_data_nodes"].int_value();
    int r = config["num_code_nodes"].int_value();
    int kvr_type = config["kvr_type"].int_value();
    int meta_type = config["meta_type"].int_value();
    int gc_ena = config["gc_ena"].int_value();
    json11::Json::array slab_array = config["slab_list"].array_items();
    int64_t dev_cap = (int64_t)config["dev_cap"].number_value();
    batch_size = config["batch_size"].int_value();
    int slab_size = slab_array.size();
    int *slab_list = new int[slab_size];
    for ( int i = 0; i < slab_size; i++ ) {
        slab_list[i] = slab_array[i].int_value();
    }
    std::string cache_type = config["cache_type"].string_value();
    int64_t cache_size = config["cache_size"].int_value();
    int cache_shard = config["cache_shard"].int_value();
    if (cache_type == "lru") {
        cache = NewLRUCache(cache_size << 20, cache_shard);
        printf("Cache LRU {%ld MB, %d} initiated\n", cache_size, cache_shard);
    }
    else if (cache_type == "wlfu") {
        cache = NewLFUCache(cache_size << 20, cache_shard);
        printf("Cache LFU {%ld MB, %d} initiated\n", cache_size, cache_shard);
    }
    else { // default
        cache = NewLRUCache(cache_size << 20, cache_shard);
        printf("Cache LRU {%ld MB, %d} initiated\n", cache_size, cache_shard);
    }

    num_ssds = k+r;
	kv_conts = (KVS_CONT*)malloc(num_ssds * sizeof(KVS_CONT));
    for (int i = 0; i < num_ssds; i++) {
        std::string dev_name;
        if (dev_mode == 0) {
            dev_name = "/dev/kvemul"+std::to_string(i);
        }
        else if (dev_mode == 1){
            dev_name = "/dev/nvme"+std::to_string(i)+"n1";
        }
        else {
            printf("dev_mode wrong, exit\n");
            exit(-1);
        }
        (void) new (&kv_conts[i]) KVS_CONT((char *)dev_name.c_str(), 128, dev_cap);
        printf("[dev %d %s] opened\n",i,dev_name.c_str());
    }
    switch (kvr_type) {
    case 0 :
        kvr = NewKVRaid (k, r, slab_size, slab_list, kv_conts, static_cast<MetaType>(meta_type), gc_ena==1, cache);
        printf("[KVRaid] {%d, %d} initiated]\n", k, r);
        break;
    case 1 :
        kvr = NewKVEC (k, r, slab_size, slab_list, kv_conts, static_cast<MetaType>(meta_type), cache);
        printf("[KVEC] {%d, %d} initiated]\n", k, r);
        break;
    case 2 :
        kvr = NewKVMirror (k, r, kv_conts, cache);
        printf("[KVMirror] {%d, %d} initiated]\n", k, r);
        break;
    case 3 :
        kvr = NewKVRaidPack  (k, r, slab_size, slab_list, kv_conts, static_cast<MetaType>(meta_type), gc_ena==1, cache);
        printf("[KVRaidPack] {%d, %d} initiated]\n", k, r);
        break;
    case 9 :
        kvr = NewKVDummy (k, kv_conts);
        printf("[KVDummy] {%d, %d} initiated]\n", k, r);
        break;
    default :
        return false;
    }
    //signal(SIGSEGV, handler);   // install our handler
    //printf("Install custom error handler to trace call stacks\n");    
    return true;
}

jboolean Java_com_yahoo_ycsb_db_KVredund_close(JNIEnv* env, jobject /*jdb*/) { 
    delete kvr;
    delete cache;
    for (int i = 0; i < num_ssds; i++) {
       kv_conts[i].~KVS_CONT();
    }
}

jint Java_com_yahoo_ycsb_db_KVredund_getBatchSize (JNIEnv *env, jobject) {
    return batch_size;
}

static jbyteArray copyBytes(JNIEnv* env, char *value, unsigned int val_len) {
    jbyteArray jbytes = env->NewByteArray(val_len);
    if(jbytes == nullptr) {
        // exception thrown: OutOfMemoryError	
        return nullptr;
    }
    
    env->SetByteArrayRegion(jbytes, 0, val_len,
    const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value)));
    return jbytes;
}

jbyteArray Java_com_yahoo_ycsb_db_KVredund_get(JNIEnv* env, jobject /*jdb*/,
                            jbyteArray jkey, jint jkey_len) {
	kvr_key kv_key;
	kvr_value kv_val;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    kv_key.key = (char*)key;
    kv_key.length = jkey_len;
    kv_val.val = NULL;

	bool success = kvr->kvr_get(&kv_key, &kv_val);
	//kvr->kvr_erased_get(5, &kv_key, &kv_val);

    if ((!success) || kv_val.val == nullptr || (kv_val.length == 0)) {
        // exception occurred
        delete[] key;
        return nullptr;
    }

    jbyteArray jret_value = copyBytes(env, kv_val.val, kv_val.length);
    // cleanup
    delete[] key;
    free(kv_val.val);

    return jret_value;
}

jboolean Java_com_yahoo_ycsb_db_KVredund_insert(JNIEnv* env, jobject /*jdb*/,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len) {
	kvr_key kv_key;
	kvr_value kv_val;
	int ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    jbyte* value = new jbyte[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, value);
    
    kv_key.key = (char*)key;
    kv_key.length = (unsigned int)jkey_len;
    kv_val.val = (char*)value;
    kv_val.length = (unsigned int)jval_len;

    ret = kvr->kvr_insert(&kv_key, &kv_val);

    // cleanup
    delete[] value;
    delete[] key;

    return ret;
}

jboolean Java_com_yahoo_ycsb_db_KVredund_update(JNIEnv* env, jobject /*jdb*/,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len) {
    kvr_key kv_key;
	kvr_value kv_val;
	int ret;

    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

    jbyte* value = new jbyte[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, value);
    
    kv_key.key = (char*)key;
    kv_key.length = (unsigned int)jkey_len;
    kv_val.val = (char*)value;
    kv_val.length = (unsigned int)jval_len;

    ret = kvr->kvr_update(&kv_key, &kv_val);

    // cleanup
    delete[] value;
    delete[] key;

    return ret;
}

jboolean Java_com_yahoo_ycsb_db_KVredund_delete(JNIEnv* env, jobject /*jdb*/,
                            jbyteArray jkey, jint jkey_len) {
		kvr_key kv_key;
    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);

		kv_key.key = (char*)key;
    kv_key.length = (unsigned int)jkey_len;

		kvr->kvr_delete(&kv_key);

    // cleanup
    delete[] key;

    return true;
}


jboolean Java_com_yahoo_ycsb_db_KVredund_writeBatch(JNIEnv* env, jobject thisObj /*jdb*/,
                            jobject writebatchObject) {
    jclass writebatchClass = env->GetObjectClass(writebatchObject);
    jfieldID fidcppPtr = env->GetFieldID(writebatchClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(writebatchObject, fidcppPtr);
    WriteBatch *batch =  *(WriteBatch**)&cpp_ptr;
    
    kvr->kvr_write_batch(batch);

    return true;
}


static WriteBatch *_WB_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(WriteBatch**)&cpp_ptr;
}
static void _WB_set_java_ptr(JNIEnv *env, jobject thisObj, WriteBatch *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

void Java_com_yahoo_ycsb_db_WriteBatch_init(JNIEnv *env, jobject thisobj) {
    WriteBatch *self = new WriteBatch();
    _WB_set_java_ptr(env, thisobj, self);
}

void Java_com_yahoo_ycsb_db_WriteBatch_destory(JNIEnv *env, jobject thisobj) {
    WriteBatch *self = _WB_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        delete self;
        _WB_set_java_ptr(env, thisobj, NULL);
    }
}

void Java_com_yahoo_ycsb_db_WriteBatch_put(JNIEnv* env, jobject thisobj /*jdb*/,
                            jbyteArray jkey, jint jkey_len,
                            jbyteArray jval, jint jval_len, jbyte jtype) {
    WriteBatch *batch = _WB_get_cpp_ptr(env, thisobj);
    assert(batch != NULL);

    kvr_key *kv_key = new kvr_key;
	kvr_value *kv_val = new kvr_value;

    char* key = new char[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, (jbyte *)key);

    char* value = new char[jval_len];
    env->GetByteArrayRegion(jval, 0, jval_len, (jbyte *)value);
    
    kv_key->key = key;
    kv_key->length = (uint8_t)jkey_len;
    kv_val->val = value;
    kv_val->length = (uint32_t)jval_len;

    batch->Put(kv_key, kv_val, (char)jtype);
}

void Java_com_yahoo_ycsb_db_WriteBatch_clear(JNIEnv *env, jobject thisobj) {
    WriteBatch *batch = _WB_get_cpp_ptr(env, thisobj);
    assert(batch != NULL);

    batch->Clear();
}


static Iterator *_IT_get_cpp_ptr(JNIEnv *env, jobject thisObj)
{
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    jlong cpp_ptr = env->GetLongField(thisObj, fidcppPtr);
    return *(Iterator**)&cpp_ptr;
}
static void _IT_set_java_ptr(JNIEnv *env, jobject thisObj, Iterator *self)
{
    jlong ptr = *(jlong*)&self;
    jclass thisClass = env->GetObjectClass(thisObj);
    jfieldID fidcppPtr = env->GetFieldID(thisClass, "cppPtr", "J");;
    env->SetLongField(thisObj, fidcppPtr, ptr);
}

void Java_com_yahoo_ycsb_db_Iterator_init(JNIEnv *env, jobject thisobj) {
    Iterator *self = kvr->NewIterator();
    _IT_set_java_ptr(env, thisobj, self);
}

void Java_com_yahoo_ycsb_db_Iterator_destory(JNIEnv *env, jobject thisobj) {
    Iterator *self = _IT_get_cpp_ptr(env, thisobj);
    if(self != NULL) {
        delete self;
        _IT_set_java_ptr(env, thisobj, NULL);
    }
}

void Java_com_yahoo_ycsb_db_Iterator_seek(JNIEnv *env, jobject thisobj,
                                        jbyteArray jkey, jint jkey_len) {
    Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    jbyte* key = new jbyte[jkey_len];
    env->GetByteArrayRegion(jkey, 0, jkey_len, key);
    kvr_key kv_key;
	kv_key.key = (char*)key;
    kv_key.length = (unsigned int)jkey_len;

    it->Seek(kv_key);
    // cleanup
    delete[] key;
}

void Java_com_yahoo_ycsb_db_Iterator_next(JNIEnv *env, jobject thisobj) {
    Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    it->Next();
}

jboolean Java_com_yahoo_ycsb_db_Iterator_valid(JNIEnv *env, jobject thisobj) {
    Iterator *it = _IT_get_cpp_ptr(env, thisobj);
    return it->Valid();
}

jbyteArray Java_com_yahoo_ycsb_db_Iterator_key(JNIEnv* env, jobject thisobj) {
	Iterator *it = _IT_get_cpp_ptr(env, thisobj);

    kvr_key kv_key = it->Key();
    jbyteArray jret_value = copyBytes(env, kv_key.key, kv_key.length);
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    return jret_value;
}

jbyteArray Java_com_yahoo_ycsb_db_Iterator_value(JNIEnv* env, jobject thisobj) {
	Iterator *it = _IT_get_cpp_ptr(env, thisobj);

    kvr_value kv_val = it->Value();
    jbyteArray jret_value = copyBytes(env, kv_val.val, kv_val.length);
    if (jret_value == nullptr) {
        // exception occurred
        return nullptr;
    }

    return jret_value;
}
