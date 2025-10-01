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

/// Capture from the start of the packet.
#[derive(serde::Serialize, serde::Deserialize, Debug, schemars::JsonSchema)]
pub struct Regex {
    /// The regular expression to use for capture.
    #[serde(with = "serde_regex")]
    #[schemars(with = "String")]
    pub pattern: regex::bytes::Regex,
}

impl super::CaptureStrategy for Regex {
    fn capture(&self, contents: &[u8]) -> Option<(Value, isize)> {
        let matches = self
            .pattern
            .find_iter(contents)
            .map(|mat| Value::Bytes(bytes::Bytes::copy_from_slice(mat.as_bytes())))
            .collect::<Vec<_>>();

        if matches.len() > 1 {
            Some((Value::List(matches), 0))
        } else {
            matches.into_iter().next().map(|v| (v, 0))
        }
    }
}

impl PartialEq for Regex {
    fn eq(&self, rhs: &Self) -> bool {
        self.pattern.as_str() == rhs.pattern.as_str()
    }
}
