#!/bin/bash
function usage() {
  echo "usage: build-caffe [-b <git branch name>]"
}

BRANCH="master"
PSVERSION="ps-thin"

while getopts ":b:p:" opt; do
  case ${opt} in
    b)
      BRANCH=${OPTARG}
      ;;
    p)
      PSVERSION=${OPTARG}
      ;;
    \?)
      usage
      ;;
  esac
done
shift $((OPTIND-1))

echo "Git branch=${BRANCH}"
echo "PS Version=${PSVERSION}"

CURRENT_DIR=$PWD
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPT_DIR}/pmls-caffe-dirs.sh

function get_uuid () {
  uuid=$( cat /proc/sys/kernel/random/uuid  | awk -F'-' '{print $1}' )
  echo $uuid
}

function git_branch() {
  branch=$( git branch | awk -F' ' '{print $2}' )
  echo $branch
}

function manage_dirs() {
  mkdir -p $BASE_BUILD_DIR
  mkdir -p $BUILD_META_DATA_DIR
  mkdir -p $BUILD_STORAGE_DIR
}

function build_pmls_caffe() {
  rm -rf $BASE_BUILD_DIR
  git clone -b ${BRANCH} git@github.com:raajay/pmls-caffe.git $BASE_BUILD_DIR
  cd $BASE_BUILD_DIR
  mkdir -p $BASE_BUILD_DIR/build && cd $BASE_BUILD_DIR/build
  build_info=$BASE_BUILD_DIR/build/build.info

  echo "time=$(date)" >> ${build_info}
  echo "git_branch=$(git_branch)" >> ${build_info}
  echo "git_commit=$( git rev-parse HEAD )" >> ${build_info}
  echo "ps_version=${PSVERSION}" >> ${build_info}

  cmake -DPS_VERSION:STRING=${PSVERSION} -DCMAKE_EXPORT_COMPILE_COMMANDS=1 $BASE_BUILD_DIR 2>&1 | tee  $BASE_BUILD_DIR/build/cmakeout.log
  make -j20 2>&1 | tee $BASE_BUILD_DIR/build/makeout.log

  # rename files (link some and copy some)
  mv $BASE_BUILD_DIR/build/tools/caffe_main.bin $BASE_BUILD_DIR/build/tools/caffe_main
  cp ${build_info} $BASE_BUILD_DIR/build/${build_id}_build.info
  cp ${build_info} $BUILD_META_DATA_DIR/${build_id}_build.info
}

function tar_and_store() {
  if [ -z "$build_id" ]; then
    echo "Build id is not set"
    exit 1
  fi
  tar_file=$BUILD_STORAGE_DIR/$build_id.tar.gz
  tar -c -z -v -f $tar_file -C ${BASE_BUILD_DIR} \
    build/tools/caffe_main \
    build/tools/caffe_main.bin \
    build/build.info \
    build/makeout.log \
    build/cmakeout.log
}

build_id=$(get_uuid)
echo $build_id
manage_dirs
build_pmls_caffe
# copy_binaries
tar_and_store

cd ${CURRENT_DIR}

# function copy_binaries() {
#   rm -rf $BASE_DEPLOY_DIR
#   mkdir -p $BASE_DEPLOY_DIR/build/tools
#   cp $BASE_BUILD_DIR/build/build.info $BASE_DEPLOY_DIR/build/build.info
#   cp $BASE_BUILD_DIR/build/build.info $BUILD_META_DATA_DIR/${build_id}_build.info
#   cp $BASE_BUILD_DIR/build/cmakeout.log $BASE_DEPLOY_DIR/build/cmakeout.log
#   cp $BASE_BUILD_DIR/build/makeout.log $BASE_DEPLOY_DIR/build/makeout.log
#   cp $BASE_BUILD_DIR/build/tools/caffe_main.bin $BASE_DEPLOY_DIR/build/tools/caffe_main
# }

#if [ -z "$1" ]; then
#    echo "usage: ./setup-cluster <clsuter_spec_directory>"
#    echo "The dir should contain 'hosts', 'etc-hosts' files"
#    exit 1
#fi
#
#CURRENT_DIR=$PWD
#SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
#SCRIPT_ROOT="${SCRIPT_DIR}/.."
#HOST_FILE="${SCRIPT_DIR}/$1/hosts"
#
#
#if [ ! -e "$HOST_FILE" ]; then
#    echo "${HOST_FILE} not found."
#    exit 1
#fi
#
#if [ -f $SCRIPT_ROOT/pmls-caffe-env.sh ]; then
#    source $SCRIPT_ROOT/pmls-caffe-env.sh
#fi
#CAFFE_ROOT="${PMLS_CAFFE_ROOT_DIR:-${SCRIPT_ROOT}}"
#BUILD_DIR=$CAFFE_ROOT/.build_release/
#TAR_FILE_NAME="pmls-caffe.tar.gz"
#TAR_FILE=$SCRIPT_DIR/$TAR_FILE_NAME
#THIRD_PARTY_LIBS="${CAFFE_ROOT}/third_party/lib"
#
#rm -f $TAR_FILE
#tar --absolute-names -czvf $TAR_FILE \
#    --exclude '*.o' \
#    --exclude '*.o.warnings.txt' \
#    $BUILD_DIR ${CAFFE_ROOT}/build ${THIRD_PARTY_LIBS}
#
#pdsh -R ssh -w \^${HOST_FILE} "rm -f ~/${TAR_FILE_NAME}"
#pdcp -R ssh -w \^${HOST_FILE} $TAR_FILE ~/${TAR_FILE_NAME}
## since we use tar with absolute names, the untar will also use full names
#pdsh -R ssh -w \^${HOST_FILE} "tar --absolute-names -x -z -v -f ~/${TAR_FILE_NAME}"
