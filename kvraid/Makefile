TARGET_DIR=libs

CC=g++
INCLUDES=-I./include -I./include/jerasure -I./include/gf-complete -I./include/kvssd -I./src
LIBS=-L./libs -lrt -lpthread -lkvapi -lnuma -lJerasure -lgf_complete -lleveldb
CXXFLAG=-O2 -fPIC -w -march=native -std=c++11

CACHE_SRCS=./src/cache/hash.cc ./src/cache/sharded_cache.cc ./src/cache/lru_cache.cc ./src/cache/wlfu_cache.cc 
SRCS=./src/threadpool.c ./src/kv_device.cpp ./src/mapping_table.cpp ./src/json11.cpp ./src/kvraid.cpp ./src/kvec.cpp ./src/kvmirror.cpp ./src/kvraid_pack.cpp ./src/kvdummy.cpp ./src/MurmurHash2.c $(CACHE_SRCS)

shared:
	$(CC) -shared -o $(TARGET_DIR)/libkvraid.so $(SRCS) $(INCLUDES) $(LIBS) $(CXXFLAG)

static:
	$(CC) -c $(SRCS) $(INCLUDES) $(LIBS) $(CXXFLAG)
	ar rvs $(TARGET_DIR)/libkvraid.a *.o 

clean:
	rm -rf $(TARGET_DIR)/libkvraid.*
