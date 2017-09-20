#!/bin/bash
function usage() {
  echo "usage: install-deps [-c <cluster spec directory>]"
}

CLUSTER=""

while getopts ":c:" opt; do
  case ${opt} in
    c)
      CLUSTER=${OPTARG}
      ;;
    \?)
      usage
      ;;
  esac
done
shift $((OPTIND-1))

echo "cluster=${CLUSTER}"

CURRENT_DIR=$PWD
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPT_DIR}/pmls-caffe-dirs.sh

if [ -z "$CLUSTER" ]; then
  usage
  exit 1
fi

if [ ! -f $SCRIPT_DIR/$CLUSTER/hosts ]; then
  echo "The cluster spec directory does not contain hosts file"
  exit 1
fi

HOST_FILE="${SCRIPT_DIR}/${CLUSTER}/hosts"
DEPENDENCIES_FILE="${SCRIPT_DIR}/install-pmls-caffe-dependencies-ubuntu1404.sh"

function install() {
  pdcp -R ssh -w \^${HOST_FILE} $DEPENDENCIES_FILE ~/
  pdsh -R ssh -w \^${HOST_FILE} '~/install-pmls-caffe-dependencies-ubuntu1404.sh'
}

install
