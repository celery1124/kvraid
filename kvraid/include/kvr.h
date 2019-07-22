/* kvr.h
* 07/07/2019
* by Mian Qin
*/

#ifndef   _kvr_h_   
#define   _kvr_h_   

#include <stdint.h>
#include "kvr_api.h"
#include "kv_device.h"
#include "mapping_table.h"

class KVR {
public:
    KVR() {};
    virtual ~KVR() {};

    // define KVR interface
    virtual bool kvr_insert(kvr_key *key, kvr_value *value) = 0;
	virtual bool kvr_update(kvr_key *key, kvr_value *value) = 0;
    virtual bool kvr_delete(kvr_key *key) = 0;
	virtual bool kvr_get(kvr_key *key, kvr_value *value) = 0;
};

KVR* NewKVRaid(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t);
KVR* NewKVEC(int num_d, int num_r, int num_slab, int *s_list, KVS_CONT *conts, MetaType meta_t);
KVR* NewKVMirror(int num_d, int num_r, KVS_CONT *conts);

#endif