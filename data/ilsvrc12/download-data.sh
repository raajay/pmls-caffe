#!/bin/bash
# Development kit (Task 1 & 2), 2.5MB
wget http://www.image-net.org/challenges/LSVRC/2012/nnoupb/ILSVRC2012_devkit_t12.tar.gz
md5sum ILSVRC2012_devkit_t12.tar.gz

# Development kit (Task 3), 22MB
wget http://www.image-net.org/challenges/LSVRC/2012/nnoupb/ILSVRC2012_devkit_t3.tar.gz
md5sum ILSVRC2012_devkit_t3.tar.gz

# Training images (Task 1 & 2), 138GB
wget http://www.image-net.org/challenges/LSVRC/2012/nnoupb/ILSVRC2012_img_train.tar
md5sum ILSVRC2012_img_train.tar

# Training images (Task 3), 728MB
wget http://www.image-net.org/challenges/LSVRC/2012/nnoupb/ILSVRC2012_img_train_t3.tar
md5sum ILSVRC2012_img_train_t3.tar

# Validation images (all tasks), 6.3GB
wget http://www.image-net.org/challenges/LSVRC/2012/nnoupb/ILSVRC2012_img_val.tar
md5sum ILSVRC2012_img_val.tar

# Test images (all tasks), 13GB
wget http://www.image-net.org/challenges/LSVRC/2012/nnoupb/ILSVRC2012_img_test.tar
md5sum ILSVRC2012_img_test.tar
