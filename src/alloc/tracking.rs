/*
 * Copyright 2023 Google LLC
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

use std::alloc::Layout;

cfg_if::cfg_if! {
    if #[cfg(feature = "mimalloc")] {
        type RealAllocator = mimalloc::MiMalloc;
    } else {
        type RealAllocator = std::alloc::System;
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "mimalloc")] {
        #[global_allocator]
        pub static GLOBAL_ALLOCATOR: Allocator = Allocator {
            inner: mimalloc::MiMalloc,
            stats_accumulator: StatsAccumulator::new(),
        };
    } else {
        #[global_allocator]
        pub static GLOBAL_ALLOCATOR: Allocator = Allocator {
            inner: std::alloc::System,
            stats_accumulator: StatsAccumulator::new(),
        };
    }
}

/// Provides basic tracking of allocation stats
pub(super) struct Allocator {
    /// The actual backing allocator doing the real work
    inner: RealAllocator,
    /// Accumulator for the allocation stats
    stats_accumulator: StatsAccumulator,
}

impl Allocator {
    #[inline]
    pub fn stats() -> Stats {
        GLOBAL_ALLOCATOR.stats_accumulator.gather()
    }
}

// SAFETY: std::alloc::Allocator is unsafe, this implementation itself uses
// no unsafe, other than calling into the actual allocator
unsafe impl std::alloc::GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Nested allocator call, safety contract is same as on the unsafe function itself
        let ptr = unsafe { self.inner.alloc(layout) };
        self.stats_accumulator.alloc(layout.size(), ptr as usize);
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Nested allocator call, safety contract is same as on the unsafe function itself
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        self.stats_accumulator.alloc(layout.size(), ptr as usize);
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Nested allocator dealloc call, safety contract is same as on the unsafe function itself
        unsafe { self.inner.dealloc(ptr, layout) };
        self.stats_accumulator.dealloc(layout.size(), ptr as usize);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: Nested allocator dealloc call, safety contract is same as on the unsafe function itself
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        self.stats_accumulator
            .realloc(layout.size(), new_ptr as usize, new_size);
        new_ptr
    }
}

pub struct Stats {
    pub cumul_alloc_count: u64,
    pub cumul_alloc_size: u64,
    pub cumul_free_count: u64,
    pub cumul_free_size: u64,
}

// Utilities to compute some common stats from the cumulative sums.
impl Stats {
    #[inline]
    pub fn current_allocated_size(&self) -> u64 {
        self.cumul_alloc_size - self.cumul_free_size
    }

    #[inline]
    pub fn current_allocation_count(&self) -> u64 {
        self.cumul_alloc_count - self.cumul_free_count
    }
}

impl std::ops::Add for Stats {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            cumul_alloc_count: self.cumul_alloc_count + other.cumul_alloc_count,
            cumul_alloc_size: self.cumul_alloc_size + other.cumul_alloc_size,
            cumul_free_count: self.cumul_free_count + other.cumul_free_count,
            cumul_free_size: self.cumul_free_size + other.cumul_free_size,
        }
    }
}

use crossbeam_utils::CachePadded;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Counter {
    pub count: u64,
    pub size: u64,
}

/// Packed allocation counter
///
/// This contains both the accumulation count and size, packed into single 64-bit atomic for performance
/// so we can do a single read-modify-write 64-bit operation instead of 2
///
/// # Packing
///
/// - Bits  0..29 = count, max: 1 073 741 823
/// - Bits 30..63 = size,  max: 17 179 869 183
struct PackedAtomicCounter(CachePadded<AtomicU64>);

impl PackedAtomicCounter {
    pub const fn new() -> Self {
        Self(CachePadded::new(AtomicU64::new(0)))
    }

    #[inline(always)]
    pub fn add(&self, counter: Counter) {
        let packed = counter.count | (counter.size << 30);
        // note: this single atomic operation may overflow and affect the results
        // of the 2 components that are packed together in here
        self.0.fetch_add(packed, Relaxed);
    }

    #[inline(always)]
    pub fn get_and_reset(&self) -> Counter {
        let packed = self.0.swap(0, Relaxed);
        Counter {
            count: (packed & ((1 << 30) - 1)),
            size: ((packed >> 30) & ((1 << 34) - 1)),
        }
    }
}

struct Bucket {
    cumul_alloc: PackedAtomicCounter,
    cumul_free: PackedAtomicCounter,
}

impl Bucket {
    pub const fn new() -> Self {
        Self {
            cumul_alloc: PackedAtomicCounter::new(),
            cumul_free: PackedAtomicCounter::new(),
        }
    }

    pub fn gather_and_reset(&self) -> Stats {
        let cumul_alloc = self.cumul_alloc.get_and_reset();
        let cumul_free = self.cumul_free.get_and_reset();
        Stats {
            cumul_alloc_count: cumul_alloc.count,
            cumul_alloc_size: cumul_alloc.size,
            cumul_free_count: cumul_free.count,
            cumul_free_size: cumul_free.size,
        }
    }
}

pub struct StatsAccumulator {
    /// Buckets of packed atomic counters
    ///
    /// These are used by individual allocations and free operations from all threads
    /// to reduce atomic conflicts and to improve performance
    buckets: [Bucket; 16],

    // Gathered overall stats.
    //
    // The counters in the buckets as drained to this single set of
    // atomics that is wider and readily available to access
    cumul_alloc_count: CachePadded<AtomicU64>,
    cumul_alloc_size: CachePadded<AtomicU64>,
    cumul_free_count: CachePadded<AtomicU64>,
    cumul_free_size: CachePadded<AtomicU64>,
}

impl StatsAccumulator {
    pub const fn new() -> Self {
        Self {
            buckets: [
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
                Bucket::new(),
            ],
            cumul_alloc_count: CachePadded::new(AtomicU64::new(0)),
            cumul_alloc_size: CachePadded::new(AtomicU64::new(0)),
            cumul_free_count: CachePadded::new(AtomicU64::new(0)),
            cumul_free_size: CachePadded::new(AtomicU64::new(0)),
        }
    }

    #[inline]
    pub fn gather(&self) -> Stats {
        // gather stats from all buckets and reset them to 0
        let mut stats = self.buckets.iter().fold(
            Stats {
                cumul_alloc_count: 0,
                cumul_alloc_size: 0,
                cumul_free_count: 0,
                cumul_free_size: 0,
            },
            |acc, x| acc + x.gather_and_reset(),
        );

        // accumulate the stats to our stored higher-precision atomic counters
        // as well as to the final retrieved stats objects for the user
        stats.cumul_alloc_count += self
            .cumul_alloc_count
            .fetch_add(stats.cumul_alloc_count, Relaxed);
        stats.cumul_alloc_size += self
            .cumul_alloc_size
            .fetch_add(stats.cumul_alloc_size, Relaxed);

        stats.cumul_free_count += self
            .cumul_free_count
            .fetch_add(stats.cumul_free_count, Relaxed);
        stats.cumul_free_size += self
            .cumul_free_size
            .fetch_add(stats.cumul_free_size, Relaxed);

        stats
    }

    #[inline(always)]
    fn bucket(&self, ptr: usize) -> &Bucket {
        // murmur64 hash
        // this just needs to be some simple hash on the pointer value to
        // distribute and pick which bucket of stats to write to
        let mut h = ptr;
        h ^= h.wrapping_shr(33);
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h.wrapping_shr(33);
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h.wrapping_shr(33);

        // SAFETY: Accessing the array without bounds checking is fine as limit access to valid range
        unsafe { self.buckets.get_unchecked(h % self.buckets.len()) }
    }

    #[inline(always)]
    pub fn alloc(&self, size: usize, ptr: usize) {
        let bucket = self.bucket(ptr);
        let size = size as u64;
        bucket.cumul_alloc.add(Counter { count: 1, size });
    }

    #[inline(always)]
    pub fn dealloc(&self, size: usize, ptr: usize) {
        let bucket = self.bucket(ptr);
        let size = size as u64;
        bucket.cumul_free.add(Counter { count: 1, size });
    }

    #[inline(always)]
    pub fn realloc(&self, size: usize, ptr: usize, new_size: usize) {
        let bucket = self.bucket(ptr);
        // for simplicity and correctness count a realloc as a free + alloc.
        // this is a bit inefficient though, have separate counter for reallocs or pick alloc or free based on size?
        bucket.cumul_free.add(Counter {
            count: 1,
            size: size as u64,
        });
        bucket.cumul_alloc.add(Counter {
            count: 1,
            size: new_size as u64,
        });
    }
}
