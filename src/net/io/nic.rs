#[cfg(target_os = "linux")]
pub mod xdp;

use std::fmt;

/// Different scheduling policies that can be used for XDP worker threads
///
/// Note there are other scheduling policies, but these are the only ones relevant for quilkin
#[derive(Copy, Clone)]
#[repr(i32)]
pub enum ThreadPolicy {
    /// `SCHED_OTHER` default scheduling policy in Linux
    Default = 0,
    /// [`SCHED_FIFO`](https://man.archlinux.org/man/sched.7.en#SCHED_FIFO:_First_in-first_out_scheduling) real time scheduling policy
    Fifo = 1,
    /// [`SCHED_RR`](https://man.archlinux.org/man/sched.7.en#SCHED_RR:_Round-robin_scheduling), FIFO but with time slicing
    RoundRobin = 2,
}

impl fmt::Debug for ThreadPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for ThreadPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("SCHED_OTHER"),
            Self::Fifo => f.write_str("SCHED_FIFO"),
            Self::RoundRobin => f.write_str("SCHED_RR"),
        }
    }
}

impl clap::ValueEnum for ThreadPolicy {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Default, Self::Fifo, Self::RoundRobin]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        use clap::builder::PossibleValue as pv;
        Some(match self {
            Self::Default => pv::new("SCHED_OTHER"),
            Self::Fifo => pv::new("SCHED_FIFO"),
            Self::RoundRobin => pv::new("SCHED_RR"),
        })
    }
}
