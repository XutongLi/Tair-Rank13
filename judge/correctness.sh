#!/bin/bash

INCLUDE_DIR="../include"
LIB_PATH="../lib"
func=$1
count=$2


g++ -o correctness correctness.cpp \
	-L $LIB_PATH -l engine -lpmem \
	-I $INCLUDE_DIR \

if [ $? -ne 0 ]; then
	echo "Compile Error"
	exit 7
fi

./correctness $func $count

