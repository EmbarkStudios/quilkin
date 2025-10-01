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

/// A UTC timestamp
#[derive(Copy, Clone)]
pub struct UtcTimestamp {
    inner: time::OffsetDateTime,
}

impl UtcTimestamp {
    #[inline]
    pub fn now() -> Self {
        Self {
            inner: time::OffsetDateTime::now_utc(),
        }
    }

    /// Gets the current [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time)
    #[inline]
    pub fn unix(self) -> i64 {
        self.inner.unix_timestamp()
    }

    /// Gets the current [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time) in nanoseconds.
    ///
    /// Note we truncate to a 64-bit integer, which will be fine unless someone happens
    /// to be running quilkin in a couple of hundred years
    #[inline]
    pub fn unix_nanos(self) -> i64 {
        self.inner.unix_timestamp_nanos() as _
    }

    #[inline]
    pub fn from_nanos(nanos: i64) -> Self {
        Self {
            inner: time::OffsetDateTime::from_unix_timestamp_nanos(nanos as _)
                .expect("hello future person, apologies"),
        }
    }
}

use std::fmt;

impl fmt::Debug for UtcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl PartialEq for UtcTimestamp {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[cfg_attr(test, derive(Debug))]
pub struct DurationNanos(i64);

impl DurationNanos {
    #[inline]
    pub fn from_nanos(n: i64) -> Self {
        Self(n)
    }

    #[inline]
    pub fn nanos(self) -> i64 {
        self.0
    }

    #[inline]
    pub fn duration(self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.0 as _)
    }
}

impl std::ops::Sub for UtcTimestamp {
    type Output = DurationNanos;

    fn sub(self, rhs: Self) -> Self::Output {
        DurationNanos(self.unix_nanos() - rhs.unix_nanos())
    }
}
