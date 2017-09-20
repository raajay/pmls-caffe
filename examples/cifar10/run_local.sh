#!/bin/bash

# Figure out the paths.
script_path=`readlink -f $0`
script_dir=`dirname $script_path`

script_root_dir="$script_dir/../.."
if [ -f "$script_root_dir/pmls-caffe-env.sh" ]; then
    source $script_root_dir/pmls-caffe-env.sh
fi

app_dir="${PMLS_CAFFE_ROOT_DIR:-${script_root_dir}}"
third_party_libs="${app_dir}/third_party/lib"
caffe_libs="${PMLS_CAFFE_ROOT_DIR}/build/lib"

progname=caffe_main
prog_path=${app_dir}/build/tools/${progname}

#host_filename="${script_dir}/machinefiles"
#host_file=$(readlink -f $host_filename)
#
#worker_filename="${script_dir}/workerfiles"
#worker_file=$(readlink -f $worker_filename)

dataset=cifar10

##=====================================
## Parameters
##=====================================

# Input files:
solver_filename="${script_root_dir}/examples/cifar10/cifar10_quick_solver.prototxt"
 # Uncomment this and line-93 if (re-)start training from a snapshot
#snapshot_filename="${app_dir}/examples/cifar10/cifar10_quick_iter_4000.solverstate"

# System parameters:
num_app_threads=1
num_table_threads=$(( num_app_threads + 1 ))
num_openblas_threads=2
param_table_staleness=1
num_comm_channels_per_client=4
num_rows_per_table=100
consistency_model="SSP"
svb=false

##=====================================

ssh_options="-oStrictHostKeyChecking=no \
-oUserKnownHostsFile=/dev/null \
-oLogLevel=quiet"

# Parse hostfile
unique_host_list='127.0.0.1'
num_unique_hosts=1
# User please specify device_id for multi GPUs here.
devices="0"

output_dir=$script_root_dir/output
output_dir="${output_dir}/caffe.${dataset}.S${param_table_staleness}"
output_dir="${output_dir}.M${num_unique_hosts}"
output_dir="${output_dir}.T${num_app_threads}"
log_dir=$output_dir/logs
net_outputs_prefix="${output_dir}/${dataset}"

# Kill previous instances of this program
echo "Killing previous instances of '$progname' on servers, please wait..."
for ip in $unique_host_list; do
  ssh $ssh_options $ip \
    killall -q $progname
done
echo "All done!"

# Spawn program instances
client_id=0
for ip in $unique_host_list; do
  echo Running client $client_id on $ip
  log_path=${log_dir}.${client_id}

  cmd="'mkdir -p ${output_dir}; \
      mkdir -p ${log_path}; \
      ulimit -c unlimited; \
      export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${caffe_libs}:${third_party_libs}:/usr/local/cuda/lib64; \
      GLOG_logtostderr=false \
      GLOG_stderrthreshold=0 \
      GLOG_log_dir=$log_path \
      GLOG_v=20 \
      GLOG_minloglevel=0 \
      GLOG_vmodule="" \
      OPENBLAS_NUM_THREADS=${num_openblas_threads} \
      $prog_path train \
      --consistency_model $consistency_model \
      --init_thread_access_table=true \
      --hostfile $host_file \
      --client_id ${client_id} \
      --num_clients $num_unique_hosts \
      --num_table_threads $num_table_threads \
      --table_staleness $param_table_staleness \
      --num_comm_channels_per_client $num_comm_channels_per_client \
      --num_rows_per_table $num_rows_per_table \
      --svb=$svb \
      --stats_path ${output_dir}/caffe_stats.yaml \
      --solver=${solver_filename} \
      --net_outputs=${net_outputs_prefix} \
      --gpu=${devices}'" #\
      #--snapshot=${snapshot_filename}'"

  ssh $ssh_options $ip bash -c $cmd &
  #eval $cmd  # Use this to run locally (on one machine).

  # Wait a few seconds for the name node (client 0) to set up
  if [ $client_id -eq 0 ]; then
    echo $cmd   # echo the cmd for just the first machine.
    echo "Waiting for name node to set up..."
    sleep 3
    echo "Done."
  fi
  client_id=$(( client_id+1 ))
done
