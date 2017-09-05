#!/bin/bash

echo "Updating partitions..."
(echo t; echo 4; echo 8e; echo p; echo w) | fdisk /dev/sda
(echo n; echo p; echo 1; echo; echo; echo t; echo 8e; echo p; echo w) | fdisk /dev/sdb
(echo o; echo n; echo p; echo 1; echo; echo; echo t; echo 8e; echo p; echo w) | fdisk /dev/sdc
partprobe

echo "Creating logical volume..."
pvcreate /dev/sda4
pvcreate /dev/sdb1
pvcreate /dev/sdc1
vgcreate vginstances /dev/sda4 /dev/sdb1 /dev/sdc1
lvcreate -L 2.0T -n lvinstances vginstances

echo "Formatting logical volume..."
mkfs.ext3 /dev/vginstances/lvinstances

echo "Mounting logical volume..."
mount /dev/vginstances/lvinstances /media

echo "SUCCESS"

# Moving temp to media
mkdir /media/tmp
chmod 777 /media/tmp
rm -rf /tmp
ln -s /media/tmp /tmp

# Moving cache to media
mkdir /media/cache
chmod 777 /media/cache
rm -rf /users/raajay86/.cache
ln -s /media/cache /users/raajay86/.cache

# Setup work environment
mkdir /media/raajay
chown -R raajay86 /media/raajay

df -H > /users/raajay86/disk_settings.txt
