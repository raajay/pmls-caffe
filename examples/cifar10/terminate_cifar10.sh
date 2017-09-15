#!/bin/bash

# Figure out the paths.
script_path=`readlink -f $0`
script_dir=`dirname $script_path`

script_root_dir="$script_dir/../.."
if [ -f "$script_root_dir/pmls-caffe-env.sh" ]; then
    source $script_root_dir/pmls-caffe-env.sh
fi

app_dir="${PMLS_CAFFE_ROOT_DIR:-${script_root_dir}}"
progname=caffe_main
prog_path=${app_dir}/build/tools/${progname}

worker_filename="${script_dir}/workerfiles"
worker_file=$(readlink -f $worker_filename)

# Parse hostfile
host_list=`cat $worker_file | awk '{ print $2 }'`
unique_host_list=`cat $worker_file | awk '{ print $2 }' | uniq`
num_unique_hosts=`cat $worker_file | awk '{ print $2 }' | uniq | wc -l`

ssh_options="-oStrictHostKeyChecking=no \
-oUserKnownHostsFile=/dev/null \
-oLogLevel=quiet"

echo "Killing instances of '$progname' on servers, please wait..."
for ip in $unique_host_list; do
  ssh $ssh_options $ip \
    killall -q $progname
done
echo "All done!"
