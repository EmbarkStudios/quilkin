/*
 * Copyright 2024 Google LLC All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use crate::net::endpoint::metadata::Value;
use bytes::Bytes;

/// Returns whether the capture size is bigger than the packet size.
#[inline]
fn is_valid_size(contents: &[u8], size: u32) -> bool {
    contents.len() >= size as usize
}

/// Capture from the start of the packet.
#[derive(Debug, Eq, PartialEq, serde::Deserialize, schemars::JsonSchema, serde::Serialize)]
pub struct Prefix {
    /// Whether captured bytes are removed from the original packet.
    #[serde(default)]
    pub remove: bool,
    /// The number of bytes to capture.
    pub size: u32,
}

impl super::CaptureStrategy for Prefix {
    fn capture(&self, contents: &[u8]) -> Option<(Value, isize)> {
        is_valid_size(contents, self.size).then(|| {
            let value = Value::Bytes(Bytes::copy_from_slice(&contents[..self.size as _]));

            (
                value,
                if self.remove {
                    -(self.size as isize)
                } else {
                    0
                },
            )
        })
    }
}

/// Capture from the end of the packet.
#[derive(Debug, Eq, PartialEq, serde::Serialize, schemars::JsonSchema, serde::Deserialize)]
pub struct Suffix {
    /// Whether captured bytes are removed from the original packet.
    pub size: u32,
    /// The number of bytes to capture.
    #[serde(default)]
    pub remove: bool,
}

impl super::CaptureStrategy for Suffix {
    fn capture(&self, contents: &[u8]) -> Option<(Value, isize)> {
        is_valid_size(contents, self.size).then(|| {
            let index = contents.len() - self.size as usize;
            let value = Value::Bytes(Bytes::copy_from_slice(&contents[index..]));

            (value, if self.remove { self.size as isize } else { 0 })
        })
    }
}
