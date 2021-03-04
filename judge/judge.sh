#!/bin/bash

INCLUDE_DIR="../include"
LIB_PATH=$1
set_per_thread=$2
get_per_thread=$3


g++ -pthread -o judge judge.cpp random.cpp\
	-L $LIB_PATH -l engine \
	-I $INCLUDE_DIR \
    -g \
    -mavx2 \
    -std=c++11 \
    -lpmem

if [ $? -ne 0 ]; then
    echo "Compile Error"
    exit 7 
fi

# rm -f /mnt/pmem/DB
./judge -s $set_per_thread -g $get_per_thread

