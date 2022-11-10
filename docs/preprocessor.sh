#!/usr/bin/env bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Generate config schemas
cargo run -q --manifest-path ../Cargo.toml -- -q generate-config-schema -o ../target

# Output all the command line help
cargo run -q --manifest-path ../Cargo.toml &> ../target/quilkin.commands || true
cargo run -q --manifest-path ../Cargo.toml -- run --help &> ../target/quilkin.run.commands || true
cargo run -q --manifest-path ../Cargo.toml -- manage --help &> ../target/quilkin.manage.commands || true

# Credit: https://github.com/rust-lang/mdBook/issues/1462#issuecomment-778650045
jq -M -c .[1] <&0
