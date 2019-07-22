/* test_kvdevice.cpp
* 03/25/2019
* by Mian Qin
*/

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <atomic>
#include <kv_device.h>

std::atomic<int> completed(0);

#define VAL_LEN 16

KV_DEVICE** ssds;

void cb (void *) {
    completed++;
}

void do_insert (int mode, int num_ssds, int record_cnt) {
    phy_key *pkey = new phy_key[num_ssds*record_cnt];
    phy_val *pval = new phy_val[num_ssds*record_cnt];
    char **value = (char **)malloc(sizeof(char*) * num_ssds*record_cnt);
    for (int i = 0; i < num_ssds; i++) {
        for (int j = 0; j < record_cnt; j++) {
            value[j+i*record_cnt] = (char *) calloc(sizeof(char), VAL_LEN);
            memset(value[j+i*record_cnt],'a'+j+i*record_cnt,VAL_LEN);
            pkey[j+i*record_cnt] = phy_key(0, j);
            pval[j+i*record_cnt] = phy_val(value[j+i*record_cnt], VAL_LEN);
        }
    }

    for (int i = 0; i < num_ssds; i++) {
        for (int j = 0; j < record_cnt; j++) {

            if (mode == 0) // sync
                ssds[i]->kv_store(&pkey[j+i*record_cnt], &pval[j+i*record_cnt]);
            else if (mode == 1) // async
                ssds[i]->kv_astore(&pkey[j+i*record_cnt], &pval[j+i*record_cnt], cb, NULL);
        }
    }

    if (mode == 1) {// reap
        while (completed != num_ssds*record_cnt) usleep(1);
    }
}

void do_get (int mode, int num_ssds, int record_cnt) {
    phy_key *pkey = new phy_key[num_ssds*record_cnt];
    phy_val *pval = new phy_val[num_ssds*record_cnt];
    char **value = (char **)malloc(sizeof(char*) * num_ssds*record_cnt);
    for (int i = 0; i < num_ssds; i++) {
        for (int j = 0; j < record_cnt; j++) {
            value[j+i*record_cnt] = (char *) calloc(sizeof(char), VAL_LEN);
            pkey[j+i*record_cnt] = phy_key(0, j);
            pval[j+i*record_cnt] = phy_val(value[j+i*record_cnt], VAL_LEN/2);
        }
    }

    for (int i = 0; i < num_ssds; i++) {
        for (int j = 0; j < record_cnt; j++) {
            
            if (mode == 0) {// sync
                ssds[i]->kv_get(&pkey[j+i*record_cnt], &pval[0]);
                printf ("kv_get, dev = %d, key = %s, value = %s, value_size %d\n",i,pkey[j+i*record_cnt].c_key, pval[0].c_val, pval[0].actual_len);
            }
            else if (mode == 1) { //async
                ssds[i]->kv_aget(&pkey[j+i*record_cnt], &pval[j+i*record_cnt], cb, NULL); 
            }
        }
    }
    if (mode == 1) {// reap
        while (completed != num_ssds*record_cnt) usleep(1);
        for (int i = 0; i < num_ssds; i++) {
            for (int j = 0; j < record_cnt; j++) {
            
                printf ("kv_get, dev = %d, key = %s, value = %s\n",i,pkey[j+i*record_cnt].c_key, pval[j+i*record_cnt].c_val);
               
            }
        }
    }
}

int main(int argc, char **argv) {
	int c;
    int num_ssds = 1;
    int num_records = 4;
    int mode = 0;

	char *shortopts =
          "d:"  /* number of devices */
          "n:"  /* number of records */
          "m:"  // mode 0-sync, 1-async
          ;


    /* parse settings */
    while (-1 != (c = getopt(argc, argv, shortopts))) {
    	switch(c) {
            case 'd':
    			num_ssds = atoi(optarg);
    			break;
            case 'n':
    			num_records = atoi(optarg);
    			break;
            case 'm':
    			mode = atoi(optarg);
    			break;
            default:
                break;
            
    	}
    }

    // test
    ssds = (KV_DEVICE**)malloc(num_ssds * sizeof(KV_DEVICE*));
    for (int i = 0; i < num_ssds; i++) {
        if (mode == 0)
            ssds[i] = new KV_DEVICE(i, "/dev/kvemul", 0, 0);
        else if(mode == 1)
            ssds[i] = new KV_DEVICE(i, "/dev/kvemul", 4, 64);
    }
    do_insert(mode, num_ssds, num_records);
    completed = 0;
    do_get(mode, num_ssds, num_records);
    for (int i = 0; i < num_ssds; i++) {
        delete ssds[i];
    }



  
    return 0;

}