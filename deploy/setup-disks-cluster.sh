#!/bin/bash

if [ -z "$1" ]; then
    echo "usage: ./setup-cluster <directory>"
    echo "The dir should contain 'hosts', 'etc-hosts' files"
    exit 1
fi

SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
HOST_FILE="$1/hosts"
ETC_HOST_FILE="$1/etc-hosts"

if [ ! -e "$HOST_FILE" ]; then
    echo "$HOST_FILE does not exist."
    exit 1
fi

if [ ! -e "$ETC_HOST_FILE" ]; then
    echo "$ETC_HOST_FILE does not exist."
    exit 1
fi

# setup hard disk
#remotehost=$( head -1 ${HOST_FILE} | xargs ping -c 1 | grep "icmp_seq=1" | awk -F' ' '{print $4}' )
remotehost="clemson"
echo $remotehost
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

