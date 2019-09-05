/* kvs_cont.h
* 07/04/2019
* by Mian Qin
*/

#include <stdlib.h>
#include <string.h>
#include "kvssd/kvs_api.h"

class KVS_CONT {
    public:
      char kvs_dev_path[32];
      int64_t log_capacity_; //B
      kvs_device_handle dev;
      kvs_container_handle cont_handle;

    int64_t get_log_capacity() {return log_capacity_;}
    
    public:
      KVS_CONT(char* dev_path, int qdepth, int64_t log_cap) : log_capacity_(log_cap) {
        memset(kvs_dev_path, 0, 32);
        memcpy(kvs_dev_path, dev_path, strlen(dev_path));
        kvs_init_options options;
        kvs_init_env_opts(&options);
        options.memory.use_dpdk = 0;
        // options for asynchronized call
        options.aio.iocoremask = 0;
        options.aio.queuedepth = qdepth;

        const char *configfile = "kvssd_emul.conf";
        options.emul_config_file =  configfile;
        kvs_init_env(&options);

        kvs_container_context ctx;
        kvs_open_device(dev_path, &dev);
        kvs_create_container(dev, "test", 4, &ctx);
        if (kvs_open_container(dev, "test", &cont_handle) == KVS_ERR_CONT_NOT_EXIST) {
          kvs_create_container(dev, "test", 4, &ctx);
          kvs_open_container(dev, "test", &cont_handle);
        }
      }
      ~KVS_CONT() {
          kvs_close_container(cont_handle);
          kvs_close_device(dev);
      }

};