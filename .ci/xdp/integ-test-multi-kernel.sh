#!/bin/bash
set -e

# We _cannot_ test on 6.17 due to a kernel bug
# - 6.12 is the current version supported on Debian Trixie
# - 6.18 is the latest LTS release
# - 6.19 is the latest 6.x release (kind of)
# - 7.0 is the latest
KERNEL_VERSIONS=('6.12' '6.13' '6.14' '6.16' '6.18' '7.0')

for vers in "${KERNEL_VERSIONS[@]}"
do
  echo "::group::Testing in kernel $vers"
  sudo "$RUNNER_TEMP/virtme-ng/vng" --rw --network user -r "v$vers" --exec .ci/xdp/veth-integ-test.sh
  echo "::endgroup::"
done
