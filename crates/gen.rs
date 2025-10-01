#!/bin/env/cargo run-cargo-script

/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! ```cargo
//! [dependencies]
//! base64-simd = "0.8.0"
//! ```

extern crate base64_simd;

fn main() {
    use std::io::Write as _;

    let mut args = std::env::args().skip(1);

    let count: u64 = args.next().unwrap().parse().unwrap();
    let length: u8 = args.next().unwrap().parse().unwrap();
    let length = length as usize;

    let file = std::fs::File::create("./quilkin-test-config.yaml").unwrap();
    let mut writer = std::io::BufWriter::new(file);

    writeln!(
        &mut writer,
        r#"version: v1alpha1
filters:
  - name: quilkin.filters.capture.v1alpha1.Capture
    config:
        suffix:
          size: {length}
          remove: true
  - name: quilkin.filters.token_router.v1alpha1.TokenRouter
clusters:
  - endpoints:
    - address: 127.0.0.1:8078
      metadata:
        quilkin.dev:
          tokens:"#
    );

    let mut s = String::new();
    for i in 0..count {
        s.clear();
        base64_simd::STANDARD.encode_append(&i.to_le_bytes()[..length], &mut s);

        writeln!(&mut writer, "          - '{s}'");
    }

    writeln!(&mut writer);
}
