#!/bin/bash

CURRENT_DIR=$PWD
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
SCRIPT_ROOT="${SCRIPT_DIR}/../.."

if [ -f $SCRIPT_ROOT/pmls-caffe-env.sh ]; then
    source $SCRIPT_ROOT/pmls-caffe-env.sh
fi
CAFFE_ROOT="${PMLS_CAFFE_ROOT_DIR:-${SCRIPT_ROOT}}"
TOOLS=$CAFFE_ROOT/build/tools

DB_PATH=$SCRIPT_ROOT/examples/cifar10/cifar10_train_leveldb
BACKEND=leveldb
NUM_PARTITIONS=10

echo "Partitioning '$DB_PATH'"
GLOG_logtostderr=1 $TOOLS/partition_data \
    --backend=$BACKEND \
    --num_partitions=$NUM_PARTITIONS \
    $DB_PATH
echo "Done."
cd $CURRENT_DIR