# KVRAID: High Performance, Write Efficient, Update Friendly Erasure Coding Scheme for KV-SSDs 

This project aims at supporting erasure coding on key-value interfaced storage device array. We compared with naive mirroring/replication, software KV stack on block devices with software RAID.


# YCSB binding

A simple java native interface (JNI) implementation with YCSB client is created for KVRAID in kvredund-jni. 

# Build 

## build Samsung KVSSD

In KVSSD-1.2.0 directory.

For more details, please refer to KVSSD_QUICK_START_GUIDE.pdf by Samsung (under KVSSD root directory).

### build emulator (environment without actual device)

```bash
	# build kvapi library
	export PRJ_HOME=$(pwd)
	export KVSSD_HOME=$PRJ_HOME/KVSSD-1.2.0/PDK/core
	$KVSSD_HOME/tools/install_deps.sh # install kvapi dependency
	mkdir $KVSSD_HOME/build
	cd $KVSSD_HOME/build
	cmake -DWITH_EMU=ON $KVSSD_HOME
	make -j4

	# copy libkvapi.so
	mkdir $PRJ_HOME/libs
	cp $KVSSD_HOME/build/libkvapi.so $PRJ_HOME/libs/
```

### build with real device

```bash
        # build kvssd device driver
        cd $PRJ_HOME/KVSSD-1.2.0/PDK/driver/PCIe/kernel_driver/kernel_v<version>/
        make clean
        make all
        sudo ./re_insmod

        # build kvapi library
        export PRJ_HOME=$(pwd)
        export KVSSD_HOME=$PRJ_HOME/KVSSD-1.2.0/PDK/core
        $KVSSD_HOME/tools/install_deps.sh # install kvapi dependency
        mkdir $KVSSD_HOME/build
        cd $KVSSD_HOME/build
        cmake -DWITH_KDD=ON $KVSSD_HOME
        make -j4

        # copy libkvapi.so
        mkdir $PRJ_HOME/libs
        cp $KVSSD_HOME/build/libkvapi.so $PRJ_HOME/libs/
```

## build kvraid library

In kvraid directory.

```bash
	make
```

# TODO

<del>Refine README with more instructions on build and run</del>

