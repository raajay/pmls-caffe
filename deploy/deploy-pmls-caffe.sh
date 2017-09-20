#!/bin/bash
function usage() {
  echo "usage: deploy-caffe [-c <cluster spec directory>] [-b <build id>]"
}

CLUSTER=""
BUILD_ID=""

while getopts ":b:c:" opt; do
  case ${opt} in
    c)
      CLUSTER=${OPTARG}
      ;;
    b)
      BUILD_ID=${OPTARG}
      ;;
    \?)
      usage
      ;;
  esac
done
shift $((OPTIND-1))

echo "cluster=${CLUSTER}"
echo "build_id=${BUILD_ID}"

CURRENT_DIR=$PWD
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPT_DIR}/pmls-caffe-dirs.sh

if [ -z "$CLUSTER" ] || [ -z "$BUILD_ID" ]; then
  usage
  exit 1
fi

if [ ! -f $SCRIPT_DIR/$CLUSTER/hosts ] || [ ! -f $SCRIPT_DIR/$CLUSTER/etc-hosts ]; then
  echo "The cluster spec directory does not contain hosts or etc-hosts file"
  exit 1
fi
HOST_FILE="${SCRIPT_DIR}/${CLUSTER}/hosts"

if [ ! -f $BUILD_STORAGE_DIR/${BUILD_ID}.tar.gz ]; then
  echo "Build not found in $BUILD_STORAGE_DIR"
fi
TAR_FILE=$BUILD_STORAGE_DIR/${BUILD_ID}.tar.gz

function transfer() {
  # if the file exists remotely we should just return
  pdsh -R ssh -w \^${HOST_FILE} "mkdir -p ${BUILD_STORAGE_DIR}"
  pdsh -R ssh -w \^${HOST_FILE} "mkdir -p ${BUILD_DEPLOY_DIR}"
  pdcp -R ssh -w \^${HOST_FILE} $TAR_FILE ${BUILD_STORAGE_DIR}/${BUILD_ID}.tar.gz
  # since we use tar without absolute names, we need to change into the directory
  pdsh -R ssh -w \^${HOST_FILE} "tar -x -z -v -f ${BUILD_STORAGE_DIR}/${BUILD_ID}.tar.gz -C ${BUILD_DEPLOY_DIR}"
}

transfer
