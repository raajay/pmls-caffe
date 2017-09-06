#!/bin/bash

if [ -z "$1" ]; then
    echo "usage: ./cifar10_deploy_partitions.sh <cluster_spec_directory>"
    exit 1
fi

CURRENT_DIR=$PWD
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
SCRIPT_ROOT="${SCRIPT_DIR}/../.."
DEPLOY_DIR="${SCRIPT_ROOT}/deploy/$1"
HOST_FILE="${DEPLOY_DIR}/hosts"

if [ ! -e "$HOST_FILE" ]; then
    echo "${HOST_FILE} not found."
    exit 1
fi

if [ -f $SCRIPT_ROOT/pmls-caffe-env.sh ]; then
    source $SCRIPT_ROOT/pmls-caffe-env.sh
fi
CAFFE_ROOT="${PMLS_CAFFE_ROOT_DIR:-${SCRIPT_ROOT}}"

# tar the data directory with absolute names
IFS=$'\r\n' GLOBIGNORE='*' HOSTS=($(cat ${HOST_FILE}))
NUM_HOSTS=${#HOSTS[@]}

TEMP_DIR=$( mktemp -d )
echo "Local tar files stored in $TEMP_DIR"

REMOTE_TEMP_DIR="/media/raajay/tmp"

TRAIN_DATA_PREFIX="cifar10_train_leveldb"
for host_id in `seq 0 $(( $NUM_HOSTS - 1 ))`; do
#for host_id in `seq 0 0`; do
    echo $host_id
    echo ${HOSTS[host_id]}
    PARTITION_DIR_NAME=${TRAIN_DATA_PREFIX}_${host_id}
    PARTITION_DIR=${SCRIPT_DIR}/${PARTITION_DIR_NAME}

    TEMP_TAR_FILE_NAME="${TRAIN_DATA_PREFIX}_${host_id}.tar.gz"
    TEMP_TAR_FILE="${TEMP_DIR}/${TEMP_TAR_FILE_NAME}"

    tar --absolute-names -c -z -v -f $TEMP_TAR_FILE $PARTITION_DIR
    # transfer the tar file
    pdsh -R ssh -w ${HOSTS[host_id]} "mkdir -p ${REMOTE_TEMP_DIR}"
    scp -r $TEMP_TAR_FILE ${HOSTS[host_id]}:${REMOTE_TEMP_DIR}

    # clean existing data
    pdsh -R ssh -w ${HOSTS[host_id]} "rm -rf ${SCRIPT_DIR}/${TRAIN_DATA_PREFIX}"
    pdsh -R ssh -w ${HOSTS[host_id]} "rm -rf ${PARTITION_DIR}"

    # untar the file
    pdsh -R ssh -w ${HOSTS[host_id]} "tar --absolute-names -x -z -v -f ${REMOTE_TEMP_DIR}/${TEMP_TAR_FILE_NAME}"

    # rename the directory
    pdsh -R ssh -w ${HOSTS[host_id]} "mv ${PARTITION_DIR} ${SCRIPT_DIR}/${TRAIN_DATA_PREFIX}"

done

echo "Cleaning up..."
rm -rf $TEMP_DIR
echo "Done."

cd $CURRENT_DIR
