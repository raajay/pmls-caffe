#!/bin/bash

# thirdparty deps
sudo apt-get install -y g++ make python-dev libxml2-dev libxslt-dev git zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev
sudo apt-get install -y libatlas-base-dev libprotobuf-dev libsnappy-dev libopencv-dev libhdf5-serial-dev

# caffe deps
sudo apt-get install -y libatlas-base-dev libprotobuf-dev libsnappy-dev libopencv-dev libhdf5-serial-dev
sudo apt-get install -y libnuma-dev uuid-dev

# deps for CMake
sudo apt-get install -y libgoogle-glog-dev \
    libzmq3-dev \
    libyaml-cpp-dev \
    libgoogle-perftools-dev \
    libsnappy-dev \
    libsparsehash-dev \
    libgflags-dev \
    libboost-filesystem-dev \
    libboost-system-dev \
    libboost-thread-dev \
    libleveldb-dev \
    libconfig++-dev \
    libghc-hashtables-dev \
    libtcmalloc-minimal4 \
    libevent-pthreads-2.0-5 \
    libeigen3-dev \
    liblmdb-dev

# multi-threaded CPU support
sudo apt-get install -y libopenblas-dev
