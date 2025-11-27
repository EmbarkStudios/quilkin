#!/bin/bash
set -e

# We _cannot_ test on 6.17 due to a kernel bug
KERNEL_VERSIONS=('6.11' '6.12' '6.13' '6.14' '6.15' '6.16')
 
for vers in "${KERNEL_VERSIONS[@]}"
do
  echo "::group::Testing in kernel $vers"
  sudo "$RUNNER_TEMP/virtme-ng/vng" --rw --network user -r "v$vers" --exec .ci/xdp/veth-integ-test.sh
  echo "::endgroup::"
done
