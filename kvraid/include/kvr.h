/* kvr.h
* 07/07/2019
* by Mian Qin
*/

#ifndef   _kvr_h_   
#define   _kvr_h_   

#include <stdint.h>
#include "kvr_api.h"
#include "kv_writebatch.h"
#include "kv_device.h"
#include "mapping_table.h"

class Iterator {
public:
    Iterator() {};
    virtual ~Iterator() {};
    virtual void Seek(kvr_key &key) = 0;
    virtual void SeekToFirst() = 0;
    virtual void Next() = 0;
    virtual bool Valid() = 0;
    virtual kvr_key Key() = 0;
    virtual kvr_value Value() = 0;
};

class KVR {
public:
    KVR() {};
    virtual ~KVR() {};

    // define KVR interface
    virtual bool kvr_insert(kvr_key *key, kvr_value *value) = 0;
	virtual bool kvr_update(kvr_key *key, kvr_value *value) = 0;
    virtual bool kvr_delete(kvr_key *key) = 0;
	virtual bool kvr_get(kvr_key *key, kvr_value *value) = 0;

    virtual bool kvr_write_batch(WriteBatch *batch) = 0;
    
    virtual bool kvr_erased_get(int erased, kvr_key *key, kvr_value *value) = 0;

    virtual Iterator* NewIterator() = 0;
};

KVR* NewKVRaid(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t, bool GC_ENA);
KVR* NewKVEC(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t);
KVR* NewKVMirror(int num_d, int num_r, KVS_CONT *conts);
KVR* NewKVRaidPack(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t, bool GC_ENA);

KVR* NewKVDummy(int num_d, KVS_CONT *conts);
#endif