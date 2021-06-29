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

use std::fmt::{self, Display, Formatter};

#[derive(Debug, PartialEq)]
pub struct ValueInvalidArgs {
    pub field: String,
    pub clarification: Option<String>,
    pub examples: Option<Vec<String>>,
}

/// Validation failure for a Config
#[derive(Debug, PartialEq)]
pub enum ValidationError {
    NotUnique(String),
    EmptyList(String),
    ValueInvalid(ValueInvalidArgs),
    FilterInvalid(crate::filters::Error),
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::NotUnique(field) => write!(f, "field {} is not unique", field),
            ValidationError::EmptyList(field) => write!(f, "field {} is cannot be an empty", field),
            ValidationError::ValueInvalid(args) => write!(
                f,
                "{} has an invalid value{}{}",
                args.field,
                args.clarification
                    .as_ref()
                    .map(|v| format!(": {}", v))
                    .unwrap_or_default(),
                args.examples
                    .as_ref()
                    .map(|v| format!("examples: {}", v.join(",")))
                    .unwrap_or_default()
            ),
            ValidationError::FilterInvalid(reason) => {
                write!(f, "filter configuration is invalid: {}", reason)
            }
        }
    }
}
