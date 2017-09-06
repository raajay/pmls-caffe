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

# generate machine file
MACHINE_FILE="${SCRIPT_DIR}/machinefiles"
IFS=$'\r\n' GLOBIGNORE='*' HOSTS=($(cat ${HOST_FILE}))
NUM_HOSTS=${#HOSTS[@]}

rm -f ${MACHINE_FILE}
for host_id in `seq 0 $(( $NUM_HOSTS - 1 ))`; do
    echo "${host_id} ${HOSTS[host_id]} 9999" >> ${MACHINE_FILE}
done

# create a tar file out of the experiment directory
TEMP_DIR=$( mktemp -d )
TEMP_TAR_FILE=${TEMP_DIR}/experiment.tar.gz
EXPERIMENT_DIR=${SCRIPT_DIR}

tar --absolute-names -c -z -v -f ${TEMP_TAR_FILE} \
    --exclude '*leveldb*' \
    --exclude '*caffemodel*' \
    --exclude '*solverstate*' \
    --exclude '.gitignore' \
    --exclude '*.md' \
    ${EXPERIMENT_DIR} ${SCRIPT_ROOT}/pmls-caffe-env.sh

REMOTE_TEMP_DIR="/media/raajay/tmp"
pdcp -R ssh -w \^${HOST_FILE} ${TEMP_TAR_FILE} ${REMOTE_TEMP_DIR}
pdsh -R ssh -w \^${HOST_FILE} "tar --absolute-names -x -z -v -f ${REMOTE_TEMP_DIR}/experiment.tar.gz"
