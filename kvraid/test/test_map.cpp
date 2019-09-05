#include <stdio.h>
#include <stdlib.h>
#include <new>

#include "../include/kv_device.h"
#include "../include/mapping_table.h"

#define DEV_CAP 107374182400
int main() {
    bool found = false;
    std::string skey = "1234";
    phy_key pkey(1, 1234);
    phy_key upkey(0, 0);
    phy_key rpkey;
    Map* memmap = NewMemMap();

    memmap->insert(&skey, &pkey);
    found = memmap->lookup(&skey, &rpkey);
    printf("memmap found pkey %d, %d\n",rpkey.get_slab_id(), rpkey.get_seq());
    memmap->update(&skey, &upkey);
    found = memmap->lookup(&skey, &rpkey);
    printf("memmap found pkey %d, %d\n",rpkey.get_slab_id(), rpkey.get_seq());

    KVS_CONT *conts = (KVS_CONT *)malloc(sizeof(KVS_CONT)*6);
    for (int i = 0; i < 6; i++) {
        (void) new (&conts[i]) KVS_CONT("/dev/kvemul", 64, DEV_CAP);
    }
    Map* storagemap = NewStorageMap(conts, 4, 2);

    storagemap->insert(&skey, &pkey);
    found = storagemap->lookup(&skey, &rpkey);
    printf("storagemap found pkey %d, %d\n",rpkey.get_slab_id(), rpkey.get_seq());
    storagemap->update(&skey, &upkey);
    found = storagemap->lookup(&skey, &rpkey);
    printf("storagemap found pkey %d, %d\n",rpkey.get_slab_id(), rpkey.get_seq());


    return 0;
}