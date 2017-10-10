#!/bin/bash
mkdir -p /media/raajay/scripts-pmls-caffe/examples/cifar10/../../output/caffe.cifar10.S1.M1.T1;
mkdir -p /media/raajay/scripts-pmls-caffe/examples/cifar10/../../output/caffe.cifar10.S1.M1.T1/logs.0;
ulimit -c unlimited;
export LD_LIBRARY_PATH=:/media/raajay/pmls-caffe/build/lib:/media/raajay/pmls-caffe/third_party/lib:/usr/local/cuda/lib64:/media/raajay/pmls-caffe/build/lib:/media/raajay/pmls-caffe/third_party/lib:/usr/local/cuda/lib64:/media/raajay/pmls-caffe/build/lib:/media/raajay/pmls-caffe/third_party/lib:/usr/local/cuda/lib64:/media/raajay/pmls-caffe/build/lib:/media/raajay/pmls-caffe/third_party/lib:/usr/local/cuda/lib64;
GLOG_logtostderr=false \
    GLOG_stderrthreshold=0 \
    GLOG_log_dir=/media/raajay/scripts-pmls-caffe/examples/cifar10/../../output/caffe.cifar10.S1.M1.T1/logs.0 \
    GLOG_v=0 \
    GLOG_minloglevel=0 \
    GLOG_vmodule= \
    OPENBLAS_NUM_THREADS=16 \
    /media/raajay/pmls-caffe/build/tools/caffe_main.bin train \
    --consistency_model SSP \
    --init_thread_access_table=true \
    --hostfile /media/raajay/scripts-pmls-caffe/examples/cifar10/localmachinefiles \
    --client_id 0 \
    --num_clients 1 \
    --num_table_threads 2 \
    --table_staleness 0 \
    --num_comm_channels_per_client 2 \
    --num_rows_per_table 10 \
    --svb=false \
    --stats_path /media/raajay/scripts-pmls-caffe/examples/cifar10/../../output/caffe.cifar10.S1.M1.T1/caffe_stats.yaml \
    --solver=/media/raajay/scripts-pmls-caffe/examples/cifar10/../../examples/cifar10/cifar10_quick_solver.prototxt \
    --net_outputs=/media/raajay/scripts-pmls-caffe/examples/cifar10/../../output/caffe.cifar10.S1.M1.T1/cifar10
