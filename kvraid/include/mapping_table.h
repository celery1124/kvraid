/* mapping_table.h
* 07/03/2019
* by Mian Qin
*/

#ifndef   _mapping_table_h_   
#define   _mapping_table_h_   


#include <string>
#include <stdint.h>
#include "phy_kv.h"

typedef enum {
    Mem,
    Storage
} MetaType;

class Map {
public:
    Map() {};
    virtual ~Map() {};

    // define Map interface
    virtual bool lookup(std::string* key, phy_key* val) = 0;
    virtual void insert(std::string* key, phy_key* val) = 0;
    virtual void update(std::string* key, phy_key* val) = 0;
    virtual void erase(std::string* key) = 0;
};

Map* NewMemMap();
Map* NewStorageMap(KVS_CONT *conts, int k, int r);

#endif