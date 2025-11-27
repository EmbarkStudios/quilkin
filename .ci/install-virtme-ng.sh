#!/bin/bash
set -e

source="${BASH_SOURCE[0]}"
virtme_version="v3.18"

echo "::notice file=$source,line=$LINENO::Installing dependencies"
sudo apt-get update && sudo apt-get install python3-pip qemu-system-x86

echo "::notice file=$source,line=$LINENO::Installing virtme-ng $virtme_version"

git clone https://github.com/arighi/virtme-ng
(cd virtme-ng && BUILD_VIRTME_NG_INIT=1 pip3 install .)

