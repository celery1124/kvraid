
CC=g++
INCLUDES=-I../include -I../include/kvssd -I../include/jerasure -I../include/gf-complete
SRC=../src
LIBS=-L../libs -lrt -lpthread -lkvapi -lJerasure -lgf_complete -lnuma -lleveldb -std=c++11
CXXFLAG=-O2 -fPIC -w -march=native
CXXFLAG_DBG=-O0 -fPIC -w -march=native -g 

MAP_SRCS=$(SRC)/mapping_table.cpp
DEV_SRCS=$(SRC)/kv_device.cpp $(SRC)/threadpool.c 
KVRAID_SRCS=$(SRC)/kv_device.cpp $(SRC)/threadpool.c $(SRC)/kvraid.cpp
KVEC_SRCS=$(SRC)/kv_device.cpp $(SRC)/threadpool.c $(SRC)/kvec.cpp
KVR_SRCS=$(SRC)/kv_device.cpp $(SRC)/threadpool.c $(SRC)/kvec.cpp $(SRC)/kvraid.cpp $(SRC)/kvmirror.cpp $(SRC)/MurmurHash2.c

test_dev:
	$(CC) test_kvdevice.cpp -o test_kvdevice $(DEV_SRCS)  $(INCLUDES) $(LIBS) $(CXXFLAG_DBG)

test_map:
	$(CC) test_map.cpp -o test_map $(MAP_SRCS) $(INCLUDES) $(LIBS) -g2

test_kvraid:
	$(CC) test_kvraid.cpp -o test_kvraid $(KVRAID_SRCS) $(MAP_SRCS)  $(INCLUDES) $(LIBS) $(CXXFLAG_DBG)

test_kvec:
	$(CC) test_kvec.cpp -o test_kvec $(KVEC_SRCS) $(MAP_SRCS)  $(INCLUDES) $(LIBS) $(CXXFLAG_DBG)


test_kvr:
	$(CC) test_kvr.cpp -o test_kvr $(KVR_SRCS) $(MAP_SRCS)  $(INCLUDES) $(LIBS) $(CXXFLAG_DBG)

clean:
	rm -rf test_kvdevice test_kvraid test_kvec
