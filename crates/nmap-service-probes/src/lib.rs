/*
 * Copyright 2025 Google LLC
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

#![doc = include_str!("../README.md")]
#![no_std]
#![warn(missing_docs)]

extern crate alloc;

mod parser;

use alloc::{string::String, vec::Vec};

use parser::Directive;

pub use parser::{Match, Port, PortProtocol, Protocol, Rarity, Regex};

/// Accepts a [nmap-service-probes][syntax] document, returning [NmapServiceProbes]
/// if successful transforming the line based document format into a set of
/// datatypes with associated fields.
///
/// [syntax]: https://nmap.org/book/vscan-fileformat.html#vscan-fileformat-example
pub fn parse(mut input: &str) -> Result<NmapServiceProbes, Error> {
    let directives = parser::nmap_service_probes(&mut input)?;
    let mut current_probe = None;
    let mut exclude_ports = Vec::new();
    let mut service_probes = Vec::new();

    for directive in directives {
        match directive {
            Directive::Exclude(mut ports) => exclude_ports.append(&mut ports),
            Directive::Probe {
                protocol,
                name,
                string,
                no_payload,
            } => {
                if let Some(probe) = current_probe.take() {
                    service_probes.push(probe);
                }

                current_probe = Some(ServiceProbe::new(protocol, name, string, no_payload));
            }
            ref directive @ Directive::Match(ref r#match) => {
                let probe = current_probe
                    .as_mut()
                    .ok_or_else(|| Error::DirectiveWithoutProbe(directive.clone()))?;
                probe.matches.push(MatchKind::Exact(r#match.clone()));
            }
            ref directive @ Directive::Softmatch(ref r#match) => {
                let probe = current_probe
                    .as_mut()
                    .ok_or_else(|| Error::DirectiveWithoutProbe(directive.clone()))?;
                probe.matches.push(MatchKind::Soft(r#match.clone()));
            }
            ref directive @ Directive::Ports(ref ports) => {
                let probe = current_probe
                    .as_mut()
                    .ok_or_else(|| Error::DirectiveWithoutProbe(directive.clone()))?;
                probe.ports.append(&mut ports.clone())
            }
            ref directive @ Directive::SslPorts(ref ports) => {
                let probe = current_probe
                    .as_mut()
                    .ok_or_else(|| Error::DirectiveWithoutProbe(directive.clone()))?;
                probe.ssl_ports.append(&mut ports.clone())
            }
            directive @ Directive::TotalWaitMs(millis) => {
                let probe = current_probe
                    .as_mut()
                    .ok_or_else(|| Error::DirectiveWithoutProbe(directive.clone()))?;
                probe.total_wait_millis = Some(millis);
            }
            directive @ Directive::TcpWrappedMs(millis) => {
                let probe = current_probe
                    .as_mut()
                    .ok_or_else(|| Error::DirectiveWithoutProbe(directive.clone()))?;
                probe.tcp_wrapped_millis = Some(millis);
            }
            directive @ Directive::Rarity(rarity) => {
                let probe = current_probe
                    .as_mut()
                    .ok_or_else(|| Error::DirectiveWithoutProbe(directive.clone()))?;
                probe.rarity = Some(rarity);
            }
            ref directive @ Directive::Fallback(ref fallbacks) => {
                let probe = current_probe
                    .as_mut()
                    .ok_or_else(|| Error::DirectiveWithoutProbe(directive.clone()))?;
                probe.fallbacks.append(&mut fallbacks.clone());
            }
        }
    }

    if let Some(probe) = current_probe {
        service_probes.push(probe);
    }

    Ok(NmapServiceProbes {
        exclude_ports,
        service_probes,
    })
}

type WinnowError = winnow::error::ErrMode<winnow::error::ContextError>;

/// Potential errors that can occur when constructing [NmapServiceProbes] from
/// a nmap-service-probes document.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum Error {
    /// An error occurred while parsing the file, typically invalid syntax.
    #[error("parse error: {}", .0)]
    Parse(WinnowError),
    /// A directive which requires a probe to be associated with, has no probe
    /// above it.
    #[error("directive set without a probe set: {:?}", .0)]
    DirectiveWithoutProbe(Directive),
}

impl From<WinnowError> for Error {
    fn from(error: WinnowError) -> Self {
        Self::Parse(error)
    }
}

/// A structured representation of a nmap-service-probes document.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NmapServiceProbes {
    /// Ports that should be excluded from scanning.
    pub exclude_ports: Vec<PortProtocol>,
    /// Probes for services.
    pub service_probes: Vec<ServiceProbe>,
}

/// Represents the data needed to define a probe for a given service.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceProbe {
    /// The name of the service.
    pub name: String,
    /// The payload string to send to the service to ping it.
    pub string: String,
    /// The transport protocol for the service.
    pub protocol: Protocol,
    /// Whether to send a payload to ping or not (used to prevent pings to
    /// services which could trigger alerts).
    pub no_payload: bool,
    /// Match patterns for the response from a given service.
    pub matches: Vec<MatchKind>,
    /// The ports associated with a probe.
    pub ports: Vec<Port>,
    /// The ports associated with a probe that require SSL.
    pub ssl_ports: Vec<Port>,
    /// The timeout duration on waiting for a response to a given probe.
    pub total_wait_millis: Option<u64>,
    /// Only typically used with NULL probes, if a connection is terminated
    /// before this duration expires, the service should be labelled TCP wrapped.
    pub tcp_wrapped_millis: Option<u64>,
    /// Corresponds to how rare a given service is considered, and thus how
    /// frequently the probe is expected to run. The higher the number, the rarer.
    pub rarity: Option<Rarity>,
    /// Probes which should be used as fallbacks if no matches are found for the
    /// current probe.
    pub fallbacks: Vec<String>,
}

impl ServiceProbe {
    /// Constructs a new [ServiceProbe] with the required parameters.
    pub fn new(protocol: Protocol, name: String, string: String, no_payload: bool) -> Self {
        Self {
            protocol,
            name,
            string,
            no_payload,
            matches: <_>::default(),
            ports: <_>::default(),
            ssl_ports: <_>::default(),
            total_wait_millis: <_>::default(),
            tcp_wrapped_millis: <_>::default(),
            rarity: <_>::default(),
            fallbacks: <_>::default(),
        }
    }
}

/// The kind of match behaviour that should be used.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MatchKind {
    /// Exact matches should stop scanning after being successful.
    Exact(Match),
    /// scanning continues after a softmatch success, but only on probes which
    /// match the service from the softmatch.
    Soft(Match),
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn complete() {
        let input = r#"# The Exclude directive takes a comma separated list of ports.
# The format is exactly the same as the -p switch.
Exclude T:9100-9107

# This is the NULL probe that just compares any banners given to us
##############################NEXT PROBE##############################
Probe TCP NULL q||
# Wait for at least 5 seconds for data.  Otherwise an Nmap default is used.
totalwaitms 5000
# Windows 2003
match ftp m/^220[ -]Microsoft FTP Service\r\n/ p/Microsoft ftpd/
match ftp m/^220 ProFTPD (\d\S+) Server/ p/ProFTPD/ v/$1/
softmatch ftp m/^220 [-.\w ]+ftp.*\r\n$/i
match ident m|^flock\(\) on closed filehandle .*midentd| p/midentd/ i/broken/
match imap m|^\* OK Welcome to Binc IMAP v(\d[-.\w]+)| p/Binc IMAPd/ v$1/
softmatch imap m/^\* OK [-.\w ]+imap[-.\w ]+\r\n$/i
match lucent-fwadm m|^0001;2$| p/Lucent Secure Management Server/
match meetingmaker m/^\xc1,$/ p/Meeting Maker calendaring/
# lopster 1.2.0.1 on Linux 1.1
match napster m|^1$| p/Lopster Napster P2P client/

Probe UDP Help q|help\r\n\r\n|
rarity 3
ports 7,13,37
match chargen m|@ABCDEFGHIJKLMNOPQRSTUVWXYZ|
match echo m|^help\r\n\r\n$|"#;

        let nmap_service_probes = parse(input).unwrap();

        assert_eq!(
            nmap_service_probes.exclude_ports[0],
            PortProtocol::Tcp(Port::Range(9100, 9107))
        );

        let tcp_null = nmap_service_probes.service_probes[0].clone();

        assert_eq!(
            tcp_null,
            ServiceProbe {
                protocol: Protocol::Tcp,
                name: "NULL".into(),
                string: "".into(),
                no_payload: false,
                total_wait_millis: Some(5000),
                fallbacks: <_>::default(),
                ports: <_>::default(),
                rarity: <_>::default(),
                ssl_ports: <_>::default(),
                tcp_wrapped_millis: <_>::default(),
                matches: vec![
                    MatchKind::Exact(Match {
                        service: "ftp".into(),
                        regex: Regex::new("^220[ -]Microsoft FTP Service\\r\\n"),
                        version_info: Some("p/Microsoft ftpd/".into()),
                    }),
                    MatchKind::Exact(Match {
                        service: "ftp".into(),
                        regex: Regex::new(r"^220 ProFTPD (\d\S+) Server"),
                        version_info: Some("p/ProFTPD/ v/$1/".into()),
                    }),
                    MatchKind::Soft(Match {
                        service: "ftp".into(),
                        regex: Regex::new(r"^220 [-.\w ]+ftp.*\r\n$").case_insensitive(),
                        version_info: None,
                    }),
                    MatchKind::Exact(Match {
                        service: "ident".into(),
                        regex: Regex::new(r"^flock\(\) on closed filehandle .*midentd"),
                        version_info: Some("p/midentd/ i/broken/".into()),
                    }),
                    MatchKind::Exact(Match {
                        service: "imap".into(),
                        regex: Regex::new(r"^\* OK Welcome to Binc IMAP v(\d[-.\w]+)"),
                        version_info: Some("p/Binc IMAPd/ v$1/".into()),
                    }),
                    MatchKind::Soft(Match {
                        service: "imap".into(),
                        regex: Regex::new(r"^\* OK [-.\w ]+imap[-.\w ]+\r\n$").case_insensitive(),
                        version_info: None,
                    }),
                    MatchKind::Exact(Match {
                        service: "lucent-fwadm".into(),
                        regex: Regex::new(r"^0001;2$"),
                        version_info: Some("p/Lucent Secure Management Server/".into()),
                    }),
                    MatchKind::Exact(Match {
                        service: "meetingmaker".into(),
                        regex: Regex::new(r"^\xc1,$"),
                        version_info: Some("p/Meeting Maker calendaring/".into()),
                    }),
                    MatchKind::Exact(Match {
                        service: "napster".into(),
                        regex: Regex::new(r"^1$"),
                        version_info: Some("p/Lopster Napster P2P client/".into()),
                    }),
                ]
            }
        );

        let udp_help = nmap_service_probes.service_probes[1].clone();

        assert_eq!(
            udp_help,
            ServiceProbe {
                protocol: Protocol::Udp,
                name: "Help".into(),
                string: "help\r\n\r\n".into(),
                no_payload: false,
                total_wait_millis: None,
                fallbacks: <_>::default(),
                ports: vec![Port::Single(7), Port::Single(13), Port::Single(37),],
                rarity: Some(Rarity::Three),
                ssl_ports: <_>::default(),
                tcp_wrapped_millis: <_>::default(),
                matches: vec![
                    MatchKind::Exact(Match {
                        service: "chargen".into(),
                        regex: Regex::new(r"@ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                        version_info: None,
                    }),
                    MatchKind::Exact(Match {
                        service: "echo".into(),
                        regex: Regex::new(r"^help\r\n\r\n$"),
                        version_info: None,
                    }),
                ]
            }
        );
    }
}
