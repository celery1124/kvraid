TARGET=libkvraid.so

CC=g++
INCLUDES=-I./include -I./include/jerasure -I./include/gf-complete -I./include/kvssd
LIBS=-L./libs -lrt -lpthread -lkvapi -lnuma -lJerasure -lgf_complete -lleveldb
CXXFLAG=-O2 -fPIC -w -march=native -std=c++11

SRCS=./src/threadpool.c ./src/kv_device.cpp ./src/mapping_table.cpp ./src/json11.cpp ./src/kvraid.cpp ./src/kvec.cpp ./src/kvmirror.cpp ./src/MurmurHash2.c


shared:
	$(CC) -shared -o $(TARGET) $(SRCS) $(INCLUDES) $(LIBS) $(CXXFLAG)

static:
	$(CC) -c $(SRCS) $(INCLUDES) $(LIBS) $(CXXFLAG)
	ar rvs libkvraid.a *.o 

clean:
	rm -rf $(TARGET)
