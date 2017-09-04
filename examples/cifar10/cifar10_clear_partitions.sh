#!/bin/bash
CURRENT_DIR=$PWD
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"

rm -rf $SCRIPT_DIR/cifar10_train_leveldb_*
rm -rf $SCRIPT_DIR/cifar10_test_leveldb_*
