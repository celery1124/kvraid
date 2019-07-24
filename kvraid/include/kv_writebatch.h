/* kv_writebatch.h
* 07/22/2019
* by Mian Qin
*/

#ifndef   _kv_writebatch_h_   
#define   _kv_writebatch_h_   

#include <stdlib.h>
#include <stdint.h>
#include <vector>
#include <string>
#include "kvr_api.h"

class WriteBatch{
public:
    typedef struct {
        kvr_key *key;
        kvr_value *val;
        char type;
    } WriteBatchInternal;
    std::vector<WriteBatchInternal> list_;
public:
    WriteBatch() {}
    ~WriteBatch() {}

    inline void Put(kvr_key *key, kvr_value *value, char type) {
        WriteBatchInternal item = {key, value, type};
        list_.push_back(item);
    }

    inline void Clear() {
        for (int i = 0; i < list_.size(); i++) {
            delete list_[i].key->key;
            delete list_[i].val->val;
            delete list_[i].key;
            delete list_[i].val;
        }
        list_.clear();
    }

    int Size() {return list_.size();}

};

#endif