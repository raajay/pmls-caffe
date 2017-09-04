#!/bin/bash
# This script converts the cifar data into leveldb/lmdb format.

# get the location of the script
CURRENT_DIR=$PWD
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
SCRIPT_ROOT="${SCRIPT_DIR}/../.."

if [ -f $SCRIPT_ROOT/pmls-caffe-env.sh ]; then
    source $SCRIPT_ROOT/pmls-caffe-env.sh
fi

echo "scripts root=$SCRIPT_ROOT"
echo "caffe root=$PMLS_CAFFE_ROOT_DIR"
CAFFE_ROOT="${PMLS_CAFFE_ROOT_DIR:-${SCRIPT_DIR}/../..}"

# get the corresponding data directory
DATA="${SCRIPT_ROOT}/data/cifar10"
EXAMPLE="${SCRIPT_ROOT}/examples/cifar10"
BACKEND="leveldb"

echo "Example directory=$EXAMPLE"
echo "Data directory=$DATA"

echo "Creating '$BACKEND'..."

# clear out existing databases
rm -rf $EXAMPLE/cifar10_train_$BACKEND $EXAMPLE/cifar10_test_$BACKEND

cd $CAFFE_ROOT
./build/examples/cifar10/convert_cifar_data.bin $DATA $EXAMPLE --backend=${BACKEND}

echo "Computing image mean..."
./build/tools/compute_image_mean $EXAMPLE/cifar10_train_$BACKEND \
  $EXAMPLE/mean.binaryproto $BACKEND

echo "Done."

cd $CURRENT_DIR
