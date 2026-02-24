#!/bin/bash
set -e

source="${BASH_SOURCE[0]}"

echo "::notice file=$source,line=$LINENO::Installing dependencies"
sudo apt-get update && sudo apt-get install python3-pip qemu-system-x86

echo "::notice file=$source,line=$LINENO::Installing virtme-ng $VIRTME_VERSION"

# We need to install virtme-ng from source because the version in ubuntu-24.04
# is, naturally, super out of date. And we need to clone it outside the source
# directory, otherwise the quilkin workspace screws it up
pushd $RUNNER_TEMP
git clone https://github.com/arighi/virtme-ng
(cd virtme-ng && git checkout "$VIRTME_VERSION" && BUILD_VIRTME_NG_INIT=1 pip3 install .)
popd
