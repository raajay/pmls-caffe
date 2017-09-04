#!/bin/bash

DIR="$( cd "$(dirname "$0")" ; pwd -P )"
cd $DIR

echo "Clearning all downloaded files..."
rm -rf *.bin *.txt readme.html
echo "Done."
