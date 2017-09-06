#!/bin/bash

if [ -z "$1" ]; then
    echo "usage: ./setup-cluster <clsuter_spec_directory>"
    echo "The dir should contain 'hosts', 'etc-hosts' files"
    exit 1
fi

CURRENT_DIR=$PWD
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
SCRIPT_ROOT="${SCRIPT_DIR}/.."
HOST_FILE="${SCRIPT_DIR}/$1/hosts"


if [ ! -e "$HOST_FILE" ]; then
    echo "${HOST_FILE} not found."
    exit 1
fi

if [ -f $SCRIPT_ROOT/pmls-caffe-env.sh ]; then
    source $SCRIPT_ROOT/pmls-caffe-env.sh
fi
CAFFE_ROOT="${PMLS_CAFFE_ROOT_DIR:-${SCRIPT_ROOT}}"
BUILD_DIR=$CAFFE_ROOT/.build_release/
TAR_FILE_NAME="pmls-caffe.tar.gz"
TAR_FILE=$SCRIPT_DIR/$TAR_FILE_NAME

rm -f $TAR_FILE
tar --absolute-names -czvf $TAR_FILE \
    --exclude '*.o' \
    --exclude '*.o.warnings.txt' \
    $BUILD_DIR ${CAFFE_ROOT}/build

pdsh -R ssh -w \^${HOST_FILE} "rm -f ~/${TAR_FILE_NAME}"
pdcp -R ssh -w \^${HOST_FILE} $TAR_FILE ~/${TAR_FILE_NAME}
# since we use tar with absolute names, the untar will also use full names
pdsh -R ssh -w \^${HOST_FILE} "tar --absolute-names -x -z -v -f ~/${TAR_FILE_NAME}"
