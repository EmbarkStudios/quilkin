/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use base64::Engine;

#[inline]
pub(crate) fn encode<A: AsRef<[u8]>>(bytes: A) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes.as_ref())
}

#[inline]
pub(crate) fn decode<A: AsRef<[u8]>>(input: A) -> Result<Vec<u8>, base64::DecodeError> {
    base64::engine::general_purpose::STANDARD.decode(input.as_ref())
}
