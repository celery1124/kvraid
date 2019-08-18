TARGET=libkvredund.so

LIB_DIR=../kvraid
CC=g++
INCLUDES=-I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" -I$(LIB_DIR)/include -I$(LIB_DIR)/include/jerasure -I$(LIB_DIR)/include/gf-complete -I$(LIB_DIR)/include/kvssd
LIBS=-L$(LIB_DIR)/libs -L$(LIB_DIR) -lrt -lpthread -lkvapi -lnuma -lJerasure -lgf_complete -lkvraid -lleveldb
SRC=kvredundjni.cc
CXXFLAGS=-std=c++11 -fPIC -O2

all:
		$(CC) -shared -o $(TARGET) $(SRC) $(INCLUDES) $(LIBS) $(CXXFLAGS)
