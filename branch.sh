#!/bin/bash +vx
LIB_PATH=$"lib/protobuf-java-3.4.1.jar"
#port
java -classpath bin:$LIB_PATH BranchServer $1 $2
