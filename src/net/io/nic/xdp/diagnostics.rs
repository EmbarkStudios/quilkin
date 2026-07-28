//! Live diagnosis for an `EPERM` from the kernel's `bpf` syscall during XDP
//! setup. The bare OS error doesn't say which of several unrelated controls
//! (capabilities, an LSM policy, seccomp) is responsible, so this inspects
//! the process's actual state for each one.

use std::fmt;

/// A capability bit relevant to loading an XDP program. Discriminants are
/// bit positions from `include/uapi/linux/capability.h`; not exposed by the
/// `libc` crate.
#[derive(Clone, Copy)]
#[repr(u64)]
enum Capability {
    NetRaw = 13,
    Bpf = 39,
}

impl Capability {
    const ALL: [Self; 2] = [Self::Bpf, Self::NetRaw];

    const fn name(self) -> &'static str {
        match self {
            Self::NetRaw => "CAP_NET_RAW",
            Self::Bpf => "CAP_BPF",
        }
    }

    fn is_set(self, cap_eff: u64) -> bool {
        cap_eff & (1 << self as u64) != 0
    }
}

/// A security control that could plausibly be responsible for an `EPERM`:
/// open (not restricting), closed (likely blocking, with detail), or
/// unknown (couldn't be read).
enum Gate {
    Open,
    Closed(String),
    Unknown(String),
}

impl fmt::Display for Gate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open => f.write_str("open"),
            Self::Closed(detail) => write!(f, "CLOSED, may be blocking `bpf` ({detail})"),
            Self::Unknown(reason) => write!(f, "unknown ({reason})"),
        }
    }
}

/// Live process state relevant to diagnosing a `bpf()` `EPERM`.
pub struct XdpPermissionDiagnosis {
    capabilities: Vec<(&'static str, Gate)>,
    seccomp: Gate,
    lsm: Gate,
    kernel_log: KernelLogScan,
}

impl fmt::Display for XdpPermissionDiagnosis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EPERM from the kernel's `bpf` syscall — live diagnosis: "
        )?;
        for (name, gate) in &self.capabilities {
            write!(f, "{name} {gate}; ")?;
        }
        write!(
            f,
            "seccomp {}; LSM confinement {}; ",
            self.seccomp, self.lsm
        )?;
        if self.kernel_log.matches.is_empty() {
            f.write_str("no bpf-related kernel log lines found")?;
        } else {
            write!(
                f,
                "recent bpf-related kernel log lines: {:?}",
                self.kernel_log.matches
            )?;
        }
        if self.kernel_log.truncated {
            f.write_str(" (scan hit its record cap before catching up, log may hold more)")?;
        }
        Ok(())
    }
}

/// Returns a diagnosis if `err`'s chain contains an `EPERM`, `None` otherwise
/// (i.e. this wasn't a permission error, so there's nothing to diagnose).
pub fn diagnose(err: &eyre::Report) -> Option<XdpPermissionDiagnosis> {
    let is_eperm = err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| io_err.raw_os_error() == Some(libc::EPERM))
    });

    if !is_eperm {
        return None;
    }

    let status = std::fs::read_to_string("/proc/self/status");
    let status_unreadable = |error: &std::io::Error| {
        Gate::Unknown(format!("could not read /proc/self/status: {error}"))
    };

    let capabilities = Capability::ALL
        .into_iter()
        .map(|cap| {
            let gate = match &status {
                Ok(status) => capability_gate(status, cap),
                Err(error) => status_unreadable(error),
            };
            (cap.name(), gate)
        })
        .collect();

    let seccomp = match &status {
        Ok(status) => seccomp_gate(status),
        Err(error) => status_unreadable(error),
    };

    let lsm = std::fs::read_to_string("/proc/self/attr/current").map_or_else(
        |error| Gate::Unknown(error.to_string()),
        |raw| lsm_gate(&raw),
    );

    Some(XdpPermissionDiagnosis {
        capabilities,
        seccomp,
        lsm,
        kernel_log: recent_bpf_related_kernel_log_lines(),
    })
}

/// Parses the `CapEff:` line of `/proc/[pid]/status` (a hex capability bitmask).
fn parse_cap_eff(status: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .and_then(|value| u64::from_str_radix(value.trim(), 16).ok())
}

fn capability_gate(status: &str, cap: Capability) -> Gate {
    match parse_cap_eff(status) {
        Some(mask) if cap.is_set(mask) => Gate::Open,
        Some(_) => Gate::Closed(format!("{} is not in CapEff", cap.name())),
        None => Gate::Unknown("could not parse CapEff".into()),
    }
}

/// Parses the `Seccomp:` line of `/proc/[pid]/status`.
fn seccomp_gate(status: &str) -> Gate {
    match status
        .lines()
        .find_map(|line| line.strip_prefix("Seccomp:"))
        .map(str::trim)
    {
        Some("0") => Gate::Open,
        Some(mode) => Gate::Closed(format!("filter active, mode {mode}")),
        None => Gate::Unknown("no Seccomp field in /proc/self/status".into()),
    }
}

/// Interprets `/proc/[pid]/attr/current` (the active LSM's confinement label).
///
/// `AppArmor` writes the literal `unconfined`; `SELinux` writes a full
/// context (e.g. `unconfined_u:unconfined_r:unconfined_t:s0`) where
/// "unconfined" is only ever a substring, never the whole label, so a
/// substring check covers both instead of exact-matching `AppArmor`'s
/// convention alone.
fn lsm_gate(raw: &str) -> Gate {
    let profile = raw.trim();
    if profile.is_empty() || profile.contains("unconfined") {
        Gate::Open
    } else {
        Gate::Closed(profile.to_owned())
    }
}

/// If `line` is a `/dev/kmsg` record mentioning bpf, returns its message with
/// the "facility,seq,timestamp,flags;" record header stripped.
fn kmsg_bpf_match(line: &str) -> Option<String> {
    line.to_ascii_lowercase().contains("bpf").then(|| {
        let msg = line.split_once(';').map_or(line, |(_, msg)| msg);
        msg.trim().to_owned()
    })
}

/// Cap on `/dev/kmsg` records scanned per call, to bound worst-case latency
/// on a long-lived kernel log rather than reading until EOF.
const MAX_KMSG_RECORDS_SCANNED: usize = 4096;
/// Only the most recent matches are relevant; older ones just add noise.
const MAX_KMSG_MATCHES_KEPT: usize = 5;

/// Result of scanning `/dev/kmsg`, distinguishing "found nothing" from "hit
/// the scan cap before catching up" so a truncated scan can't look like a
/// clean negative result.
struct KernelLogScan {
    matches: Vec<String>,
    truncated: bool,
}

/// Best-effort scan of `/dev/kmsg` for recent bpf-related lines. Returns an
/// empty, non-truncated scan on any failure to open the file rather than
/// erroring.
fn recent_bpf_related_kernel_log_lines() -> KernelLogScan {
    use std::io::Read as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    let Ok(mut kmsg) = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open("/dev/kmsg")
    else {
        return KernelLogScan {
            matches: Vec::new(),
            truncated: false,
        };
    };

    let mut buf = [0u8; 8192];
    let mut matches = Vec::new();
    let mut truncated = true;

    // Each read() returns one record, oldest first. WouldBlock means we've
    // caught up with the log; any other error (e.g. EINVAL for a record too
    // large for `buf`) drops just that one record, not the whole scan.
    for _ in 0..MAX_KMSG_RECORDS_SCANNED {
        match kmsg.read(&mut buf) {
            Ok(0) => {
                truncated = false;
                break;
            }
            Ok(n) => {
                if let Ok(line) = std::str::from_utf8(&buf[..n])
                    && let Some(matched) = kmsg_bpf_match(line)
                {
                    matches.push(matched);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                truncated = false;
                break;
            }
            Err(_) => {}
        }
    }

    let keep_from = matches.len().saturating_sub(MAX_KMSG_MATCHES_KEPT);
    KernelLogScan {
        matches: matches.split_off(keep_from),
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cap_eff() {
        let status = "Name:\tquilkin\nCapEff:\t000001ffffffffff\nSeccomp:\t0\n";
        assert_eq!(parse_cap_eff(status), Some(0x0000_01ff_ffff_ffff));
    }

    #[test]
    fn missing_cap_eff_line_is_none() {
        assert_eq!(parse_cap_eff("Name:\tquilkin\n"), None);
    }

    #[test]
    fn capability_gate_open_when_bit_set() {
        let status = "CapEff:\tffffffffffffffff\n";
        assert!(matches!(
            capability_gate(status, Capability::Bpf),
            Gate::Open
        ));
    }

    #[test]
    fn capability_gate_closed_when_bit_unset() {
        let status = "CapEff:\t0000000000000000\n";
        assert!(matches!(
            capability_gate(status, Capability::Bpf),
            Gate::Closed(_)
        ));
    }

    #[test]
    fn capability_gate_unknown_when_field_missing() {
        assert!(matches!(
            capability_gate("Name:\tquilkin\n", Capability::Bpf),
            Gate::Unknown(_)
        ));
    }

    #[test]
    fn lsm_gate_selinux_unconfined_context_is_open() {
        assert!(matches!(
            lsm_gate("unconfined_u:unconfined_r:unconfined_t:s0\n"),
            Gate::Open
        ));
    }

    #[test]
    fn lsm_gate_selinux_confined_context_is_closed() {
        assert!(matches!(
            lsm_gate("system_u:system_r:container_t:s0:c1,c2\n"),
            Gate::Closed(_)
        ));
    }

    #[test]
    fn seccomp_disabled_is_open() {
        assert!(matches!(seccomp_gate("Seccomp:\t0\n"), Gate::Open));
    }

    #[test]
    fn seccomp_filter_is_closed() {
        assert!(matches!(seccomp_gate("Seccomp:\t2\n"), Gate::Closed(_)));
    }

    #[test]
    fn unconfined_lsm_is_open() {
        assert!(matches!(lsm_gate("unconfined\n"), Gate::Open));
    }

    #[test]
    fn confined_lsm_is_closed() {
        assert!(matches!(lsm_gate("docker-default\n"), Gate::Closed(_)));
    }

    #[test]
    fn kmsg_line_without_bpf_does_not_match() {
        assert_eq!(kmsg_bpf_match("6,123,456,-;usb 1-1: new device\n"), None);
    }

    #[test]
    fn kmsg_line_with_bpf_strips_header() {
        assert_eq!(
            kmsg_bpf_match("6,123,456,-;Cilium: bpf map create denied\n"),
            Some("Cilium: bpf map create denied".to_owned())
        );
    }
}
