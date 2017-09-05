#!/bin/bash

if [ -z "$1" ]; then
    echo "usage: ./setup-cluster <directory>"
    echo "The dir should contain 'hosts', 'etc-hosts' files"
    exit 1
fi

SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
HOST_FILE="$1/hosts"
ETC_HOST_FILE="$1/etc-hosts"

pdsh -R ssh -w \^${HOST_FILE} 'sudo apt-get update'

# install required software
pdsh -R ssh -w \^${HOST_FILE} 'hostname'
pdsh -R ssh -w \^${HOST_FILE} 'sudo apt-get -y install pdsh'

# install dependencies
pdsh -R ssh -w \^${HOST_FILE} 'sudo apt-get install -y g++ make python-dev libxml2-dev libxslt-dev git zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev'
pdsh -R ssh -w \^${HOST_FILE} 'sudo apt-get install -y libatlas-base-dev libprotobuf-dev libsnappy-dev libopencv-dev libhdf5-serial-dev'
pdsh -R ssh -w \^${HOST_FILE} 'sudo apt-get install -y libnuma-dev uuid-dev'

# setup hard disk
remotehost=$( head -1 ${HOST_FILE} | xargs ping -c 1 | grep "icmp_seq=1" | awk -F' ' '{print $4}' )
#echo $remotehost
disk_setup_script=""

if [[ $remotehost =~ "wisc" ]]; then
    echo "Remote cluter is Wisconsin"
    disk_setup_script="setup-disks-cloudlab-wisc.sh"
fi

if [[ $remotehost =~ "clemson" ]]; then
    echo "Remote cluter is Clemson"
    disk_setup_script="setup-disks-cloudlab-clemson.sh"
fi

if [ -n "$disk_setup_script" ]; then
    echo "disk setup script is not null", $disk_setup_script
    pdcp -R ssh -w \^${HOST_FILE} "${SCRIPT_DIR}/${disk_setup_script}" ~/
    pdsh -R ssh -w \^${HOST_FILE} "echo ~/${disk_setup_script}"
    pdsh -R ssh -w \^${HOST_FILE} "sudo ~/${disk_setup_script}"
fi

# passwordless ssh
PRIVATE_KEY_FILE=$1/id_rsa
rm -f ${PRIVATE_KEY_FILE} ${PRIVATE_KEY_FILE}.pub

ssh-keygen -t rsa -f $PRIVATE_KEY_FILE -q -N ""
pdcp -R ssh -w \^${HOST_FILE} ${PRIVATE_KEY_FILE} ~/.ssh/id_rsa
pdcp -R ssh -w \^${HOST_FILE} ${PRIVATE_KEY_FILE}.pub ~/.ssh/id_rsa.pub
#pdsh -R ssh -w \^${HOST_FILE} "sed -i '/id_rsa/d' ~/.ssh/authorized_keys"
pdsh -R ssh -w \^${HOST_FILE} "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"

# modify etc hosts
pdcp -R ssh -w \^${HOST_FILE} "${ETC_HOST_FILE}" ~/
pdsh -R ssh -w \^${HOST_FILE} "cat ~/etc-hosts | sudo tee -a /etc/hosts"
