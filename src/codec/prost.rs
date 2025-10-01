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

//! Extensions to `prost` and related crates.

use prost_types::value::Kind;
use serde_json::Value;

pub fn encode<M: prost::Message>(message: &M) -> Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::with_capacity(message.encoded_len());
    message.encode(&mut buf)?;
    Ok(buf)
}

pub fn mapping_from_kind(kind: Kind) -> Option<serde_json::Map<String, serde_json::Value>> {
    match value_from_kind(kind) {
        Value::Object(mapping) => Some(mapping),
        _ => None,
    }
}

pub fn value_from_kind(kind: Kind) -> Value {
    match kind {
        Kind::NullValue(_) => Value::Null,
        Kind::BoolValue(v) => Value::Bool(v),
        Kind::NumberValue(v) => Value::Number(serde_json::Number::from(v as i64)), // TODO: Call out in documentation or find solution
        Kind::StringValue(v) => Value::String(v),
        Kind::ListValue(v) => Value::Array(
            v.values
                .into_iter()
                .filter_map(|v| v.kind)
                .map(value_from_kind)
                .collect(),
        ),
        Kind::StructValue(v) => Value::Object(
            v.fields
                .into_iter()
                .filter_map(|(k, v)| v.kind.map(value_from_kind).map(|v| (k, v)))
                .collect(),
        ),
    }
}

pub fn value_from_struct(value: prost_types::Struct) -> prost_types::Value {
    prost_types::Value {
        kind: Some(prost_types::value::Kind::StructValue(value)),
    }
}

pub fn from_json(value: Value) -> prost_types::Value {
    prost_types::Value {
        kind: Some(match value {
            Value::Null => Kind::NullValue(<_>::default()),
            Value::Bool(v) => Kind::BoolValue(v),
            // as_f64 never returns None, so unwrap is safe here.
            Value::Number(v) => Kind::NumberValue(v.as_f64().unwrap()),
            Value::String(v) => Kind::StringValue(v),
            Value::Array(v) => Kind::ListValue(prost_types::ListValue {
                values: v.into_iter().map(from_json).collect(),
            }),
            Value::Object(v) => Kind::StructValue(prost_types::Struct {
                fields: v
                    .into_iter()
                    .map(|(key, value)| (key, from_json(value)))
                    .collect(),
            }),
        }),
    }
}
