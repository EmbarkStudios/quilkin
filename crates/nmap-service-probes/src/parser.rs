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

mod hex;

use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use winnow::prelude::*;
use winnow::{
    ascii::{
        alpha1, dec_uint, escaped_transform, hex_uint, line_ending, space0, space1,
        till_line_ending,
    },
    combinator::{alt, delimited, dispatch, empty, fail, opt, peek, repeat, separated, trace},
    error::{AddContext, ParserError, StrContext},
    stream::AsChar,
    token::{take, take_till, take_while},
};

type Stream<'input> = &'input str;

/// Represents a single line in a nmap-service-probes file.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Line<'i> {
    Comment(&'i str),
    Directive(Directive),
    Blank,
}

impl Line<'_> {
    fn into_directive(self) -> Option<Directive> {
        match self {
            Self::Directive(directive) => Some(directive),
            _ => None,
        }
    }
}

/// Represents each of the possible directives that can be included in a
/// nmap-service-probes file.
///
/// This enum is based directly on the line based nature of the probes file, and
/// thus has no context of which directive is associated with which probe. Use
/// [crate::ServiceProbe] for associated data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Directive {
    /// Ports that should be excluded from scanning.
    Exclude(Vec<PortProtocol>),
    /// A probe for a specific service.
    Probe {
        /// The transport protocol for the service.
        protocol: Protocol,
        /// The name of the service.
        name: String,
        /// The payload string to send to the service to ping it.
        string: String,
        /// Whether to send a payload to ping or not (used to prevent pings to
        /// services which could trigger alerts).
        no_payload: bool,
    },
    /// Checks for a exact match to the response from a request/packet.
    Match(Match),
    /// Checks for a potential match to the response, scanning continues after
    /// a softmatch success, but only on probes which match the service from the
    /// softmatch.
    Softmatch(Match),
    /// The ports associated with a probe.
    Ports(Vec<Port>),
    /// The ports associated with a probe that require SSL.
    SslPorts(Vec<Port>),
    /// The timeout duration on waiting for a response to a given probe.
    TotalWaitMs(u64),
    /// Only typically used with NULL probes, if a connection is terminated
    /// before this duration expires, the service should be labelled TCP wrapped.
    TcpWrappedMs(u64),
    /// Corresponds to how rare a given service is considered, and thus how
    /// frequently the probe is expected to run. The higher the number, the rarer.
    Rarity(Rarity),
    /// Probes which should be used as fallbacks if no matches are found for the
    /// current probe.
    Fallback(Vec<String>),
}

/// Represents how to recognize services based on responses to the string sent
/// by the probe.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Match {
    /// The name of the matching service.
    pub service: String,
    /// The regex to match against the response from the service.
    pub regex: Regex,
    /// info on the specific version.
    ///
    /// **Note** Currently this field is an opaque blob of all of the data from
    /// the version_info in the document and not parsed, a future version should
    /// parse this info out into more structured data.
    pub version_info: Option<String>,
}

/// The transport protocol of the service.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Protocol {
    Tcp,
    Udp,
}

/// Represents the set of ports and protocols used for exclusion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PortProtocol {
    /// Exclude port on both TCP and UDP.
    All(Port),
    /// TCP exclusion.
    Tcp(Port),
    /// UDP exclusion.
    Udp(Port),
}

/// The port number or range of numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Port {
    /// A single port.
    Single(u16),
    /// A range of ports representing [start, end) (e.g. end is inclusive).
    Range(u16, u16),
}

/// A regular expression. These patterns are "Perl" or PCRE regular expressions.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Regex {
    /// The regex pattern.
    pattern: String,
    /// Whether to enable case insensitivitity.
    case_insensitive: bool,
    /// Whether to enable including newlines in matches.
    include_newlines: bool,
}

impl Regex {
    /// Constructs a new [Regex] from a given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            case_insensitive: false,
            include_newlines: false,
        }
    }

    /// Enables case insensitivity.
    pub fn case_insensitive(mut self) -> Self {
        self.case_insensitive = true;
        self
    }

    /// Enables including newlines in matches.
    pub fn include_newlines(mut self) -> Self {
        self.include_newlines = true;
        self
    }
}

/// Corresponds to how rare a given service is considered, and thus how
/// frequently the probe is expected to run. The higher the number, the rarer.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rarity {
    One,
    Two,
    Three,
    Four,
    Five,
    Six,
    Seven,
    Eight,
    Nine,
}

pub fn nmap_service_probes<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Vec<Directive>, E> {
    let mut lines = Vec::new();

    while !input.is_empty() {
        lines.push(line.parse_next(input)?);
    }

    Ok(lines
        .into_iter()
        .filter_map(|line: Line| line.into_directive())
        .collect())
}

fn line<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Line<'i>, E> {
    (
        space0,
        alt((
            comment.map(Line::Comment),
            directive.map(Line::Directive),
            space0.map(|_| Line::Blank),
        )),
        space0,
        opt(line_ending),
    )
        .parse_next(input)
        .map(|(_, line, _, _)| line)
}

fn comment<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Stream<'i>, E> {
    (space0, "#", till_line_ending)
        .parse_next(input)
        .map(|(_, _, line)| line)
}

fn directive<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Directive, E> {
    dispatch!((alpha1::<Stream<'i>, E>, space1);
        ("Exclude", _) => exclude::directive,
        ("Probe", _) => probe::directive,
        ("match", _) => r#match::directive,
        ("softmatch", _) => r#match::softmatch,
        ("ports", _) => ports,
        ("sslports", _) => sslports,
        ("totalwaitms", _) => totalwaitms,
        ("tcpwrappedms", _) => tcpwrappedms,
        ("rarity", _) => rarity,
        ("fallback", _) => fallback,
        _ => fail,
    )
    .parse_next(input)
}

fn prefix_string<
    'i,
    O: Default,
    P: Parser<Stream<'i>, O, E>,
    E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>,
>(
    mut prefix: &str,
    input: &mut Stream<'i>,
    string: impl FnOnce(char) -> P,
) -> PResult<O, E> {
    prefix.parse_next(input)?;

    let marker = peek(take(1usize))
        .parse_next(input)?
        .chars()
        .next()
        .unwrap();
    let mut first_two = peek(take(2usize)).parse_next(input)?.chars();

    // Catches empty strings (e.g. ||), escaped_transform expects a non-empty string.
    if marker == first_two.next().unwrap() && marker == first_two.next().unwrap() {
        take(2usize).parse_next(input)?;
        return Ok(O::default());
    }

    delimited(marker, (string)(marker), marker).parse_next(input)
}

fn port<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Port, E> {
    alt((port_range, port_single)).parse_next(input)
}

fn port_range<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Port, E> {
    (unsigned16, "-", unsigned16)
        .parse_next(input)
        .map(|(start, _, end)| Port::Range(start, end))
}

fn port_single<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Port, E> {
    unsigned16(input).map(Port::Single)
}

fn unsigned16<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<u16, E> {
    dec_uint.parse_next(input)
}

fn identifier<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<String, E> {
    take_while(1.., |c: char| !c.as_char().is_whitespace() && c != ',')
        .parse_to()
        .parse_next(input)
}

mod exclude {
    use super::*;

    pub fn directive<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<Directive, E> {
        separated(1.., excluded_port, ",")
            .parse_next(input)
            .map(Directive::Exclude)
    }

    fn excluded_port<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<PortProtocol, E> {
        if let Some((_, port)) = opt(("T:", port)).parse_next(input)? {
            Ok(PortProtocol::Tcp(port))
        } else if let Some((_, port)) = opt(("U:", port)).parse_next(input)? {
            Ok(PortProtocol::Udp(port))
        } else {
            port.parse_next(input).map(PortProtocol::All)
        }
    }
}

mod probe {
    use super::*;

    pub fn directive<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<Directive, E> {
        (
            probe_protocol,
            space1,
            probe_name,
            space1,
            probe_string,
            opt((space1, "no-payload")),
        )
            .parse_next(input)
            .map(
                |(protocol, _, name, _, string, no_payload)| Directive::Probe {
                    protocol,
                    name,
                    string,
                    no_payload: no_payload.is_some(),
                },
            )
    }

    fn probe_protocol<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<Protocol, E> {
        alt(("TCP".value(Protocol::Tcp), "UDP".value(Protocol::Udp))).parse_next(input)
    }

    pub fn probe_name<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<String, E> {
        identifier(input)
    }

    fn probe_string<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<String, E> {
        prefix_string("q", input, |marker| {
            escaped_transform(
                take_till(1.., move |c| c == marker || c == '\\'),
                '\\',
                alt((
                    ("x", hex_uint).map(|(_, hex)| hex::to_str(hex)),
                    dispatch!(take(1usize);
                        "\\" => empty.value("\\"),
                        "n" => empty.value("\n"),
                        "r" => empty.value("\r"),
                        "0" => empty.value("\0"),
                        "a" => empty.value("\x07"),
                        "b" => empty.value("\x08"),
                        "f" => empty.value("\x0C"),
                        "t" => empty.value("\x09"),
                        "v" => empty.value("\x0B"),
                        _ => fail::<_, &'static str, _>,
                    ),
                )),
            )
        })
    }
}

mod r#match {
    use super::*;

    pub fn directive<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<Directive, E> {
        (
            service,
            space1,
            trace("pattern", pattern),
            trace("version_info", opt((space1, version_info))),
        )
            .parse_next(input)
            .map(|(service, _, regex, version_info)| {
                Directive::Match(Match {
                    service,
                    regex,
                    version_info: version_info.map(|(_, vi)| vi),
                })
            })
    }

    pub fn softmatch<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<Directive, E> {
        (service, space1, pattern)
            .parse_next(input)
            .map(|(service, _, regex)| {
                Directive::Softmatch(Match {
                    service,
                    regex,
                    version_info: None,
                })
            })
    }

    fn service<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<String, E> {
        identifier(input)
    }

    fn pattern<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<Regex, E> {
        let pattern =
            prefix_string("m", input, |marker| take_till(1.., move |c| c == marker))?.to_string();
        let flags: Vec<_> = repeat(0..=2, alt(("i", "s"))).parse_next(input)?;
        let mut case_insensitive = false;
        let mut include_newlines = false;

        for flag in flags {
            match flag {
                "i" => case_insensitive = true,
                "s" => include_newlines = true,
                _ => unreachable!(),
            }
        }

        Ok(Regex {
            pattern,
            case_insensitive,
            include_newlines,
        })
    }

    fn version_info<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
        input: &mut Stream<'i>,
    ) -> PResult<String, E> {
        till_line_ending.parse_to().parse_next(input)
    }
}

pub fn ports<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Directive, E> {
    separated(1.., port, ",")
        .parse_next(input)
        .map(Directive::Ports)
}

pub fn sslports<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Directive, E> {
    separated(1.., port, ",")
        .parse_next(input)
        .map(Directive::SslPorts)
}

pub fn totalwaitms<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Directive, E> {
    dec_uint.parse_next(input).map(Directive::TotalWaitMs)
}

pub fn tcpwrappedms<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Directive, E> {
    dec_uint.parse_next(input).map(Directive::TcpWrappedMs)
}

pub fn rarity<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Directive, E> {
    dispatch!(take(1usize);
        "1" => empty.value(Rarity::One),
        "2" => empty.value(Rarity::Two),
        "3" => empty.value(Rarity::Three),
        "4" => empty.value(Rarity::Four),
        "5" => empty.value(Rarity::Five),
        "6" => empty.value(Rarity::Six),
        "7" => empty.value(Rarity::Seven),
        "8" => empty.value(Rarity::Eight),
        "9" => empty.value(Rarity::Nine),
        _ => fail,
    )
    .parse_next(input)
    .map(Directive::Rarity)
}

pub fn fallback<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<Directive, E> {
    separated(1.., probe::probe_name, ",")
        .parse_next(input)
        .map(Directive::Fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclude() -> PResult<()> {
        let input = "Exclude 53,T:9100,U:30000-40000";
        let input = &mut (&*input);

        let directives = nmap_service_probes(input)?;
        let directive = directives[0].clone();

        assert!(input.is_empty());
        assert_eq!(
            directive,
            Directive::Exclude(vec![
                PortProtocol::All(Port::Single(53)),
                PortProtocol::Tcp(Port::Single(9100)),
                PortProtocol::Udp(Port::Range(30000, 40000))
            ])
        );

        Ok(())
    }

    #[test]
    fn probe() -> Result<(), winnow::error::ErrMode<winnow::error::ErrorKind>> {
        let input = r#"Probe TCP GetRequest q|GET / HTTP/1.0\r\n\r\n|
Probe UDP DNSStatusRequest q|\0\0\x10\0\0\0\0\0\0\0\0\0|
Probe TCP NULL q||
Probe UDP Sqlping q|\x02| no-payload"#;

        let input = &mut (&*input);

        let directives = nmap_service_probes(input)?;

        assert!(input.is_empty(), "{}", input);

        let probes = [
            Directive::Probe {
                protocol: Protocol::Tcp,
                name: "GetRequest".into(),
                string: "GET / HTTP/1.0\r\n\r\n".into(),
                no_payload: false,
            },
            Directive::Probe {
                protocol: Protocol::Udp,
                name: "DNSStatusRequest".into(),
                string: "\0\0\x10\0\0\0\0\0\0\0\0\0".into(),
                no_payload: false,
            },
            Directive::Probe {
                protocol: Protocol::Tcp,
                name: "NULL".into(),
                string: "".into(),
                no_payload: false,
            },
            Directive::Probe {
                protocol: Protocol::Udp,
                name: "Sqlping".into(),
                string: "\x02".into(),
                no_payload: true,
            },
        ];

        for (directive, probe) in directives.into_iter().zip(probes) {
            assert_eq!(directive, probe);
        }

        Ok(())
    }

    #[test]
    fn r#match() -> Result<(), winnow::error::ErrMode<winnow::error::ErrorKind>> {
        let input = r#"match ftp m/^220.*Welcome to .*Pure-?FTPd (\d\S+\s*)/ p/Pure-FTPd/ v/$1/ cpe:/a:pureftpd:pure-ftpd:$1/
match ssh m/^SSH-([\d.]+)-OpenSSH[_-]([\w.]+)\r?\n/i p/OpenSSH/ v/$2/ i/protocol $1/ cpe:/a:openbsd:openssh:$2/
match mysql m|^\x10\0\0\x01\xff\x13\x04Bad handshake$| p/MySQL/ cpe:/a:mysql:mysql/
match chargen m|@ABCDEFGHIJKLMNOPQRSTUVWXYZ|
match uucp m|^login: login: login: $| p/NetBSD uucpd/ o/NetBSD/ cpe:/o:netbsd:netbsd/a
match printer m|^([\w-_.]+): lpd: Illegal service request\n$| p/lpd/ h/$1/
match afs m|^[\d\D]{28}\s*(OpenAFS)([\d\.]{3}[^\s\0]*)\0| p/$1/ v/$2/"#;

        let input = &mut (&*input);

        let _directives = nmap_service_probes(input)?;

        assert!(input.is_empty(), "{}", input);

        Ok(())
    }

    #[test]
    fn softmatch() -> Result<(), winnow::error::ErrMode<winnow::error::ErrorKind>> {
        let input = r#"softmatch ftp m/^220 [-.\w ]+ftp.*\r\n$/i
softmatch smtp m|^220 [-.\w ]+SMTP.*\r\n|
softmatch pop3 m|^\+OK [-\[\]\(\)!,/+:<>@.\w ]+\r\n$|"#;

        let input = &mut (&*input);

        let _directives = nmap_service_probes(input)?;

        assert!(input.is_empty(), "{}", input);

        Ok(())
    }

    #[test]
    fn ports() -> Result<(), winnow::error::ErrMode<winnow::error::ErrorKind>> {
        let input = r#"ports 21,43,110,113,199,505,540,1248,5432,30444
ports 111,4045,32750-32810,38978
sslports 443"#;

        let input = &mut (&*input);

        let directives = nmap_service_probes(input)?;

        assert!(input.is_empty(), "{}", input);

        let ports = [
            Directive::Ports(vec![
                Port::Single(21),
                Port::Single(43),
                Port::Single(110),
                Port::Single(113),
                Port::Single(199),
                Port::Single(505),
                Port::Single(540),
                Port::Single(1248),
                Port::Single(5432),
                Port::Single(30444),
            ]),
            Directive::Ports(vec![
                Port::Single(111),
                Port::Single(4045),
                Port::Range(32750, 32810),
                Port::Single(38978),
            ]),
            Directive::SslPorts(vec![Port::Single(443)]),
        ];

        for (directive, port) in directives.into_iter().zip(ports) {
            assert_eq!(directive, port);
        }

        Ok(())
    }

    #[test]
    fn totalwaitms() -> Result<(), winnow::error::ErrMode<winnow::error::ErrorKind>> {
        let input = r#"totalwaitms 6000"#;

        let input = &mut (&*input);

        let directives = nmap_service_probes(input)?;

        assert!(input.is_empty(), "{}", input);

        assert_eq!(directives[0], Directive::TotalWaitMs(6000));

        Ok(())
    }

    #[test]
    fn tcpwrappedms() -> Result<(), winnow::error::ErrMode<winnow::error::ErrorKind>> {
        let input = r#"tcpwrappedms 3000"#;

        let input = &mut (&*input);

        let directives = nmap_service_probes(input)?;

        assert!(input.is_empty(), "{}", input);

        assert_eq!(directives[0], Directive::TcpWrappedMs(3000));

        Ok(())
    }

    #[test]
    fn rarity() -> Result<(), winnow::error::ErrMode<winnow::error::ErrorKind>> {
        let input = r#"rarity 6"#;

        let input = &mut (&*input);

        let directives = nmap_service_probes(input)?;

        assert!(input.is_empty(), "{}", input);

        assert_eq!(directives[0], Directive::Rarity(Rarity::Six));

        Ok(())
    }

    #[test]
    fn fallback() -> Result<(), winnow::error::ErrMode<winnow::error::ErrorKind>> {
        let input = r#"fallback GetRequest,GenericLines"#;

        let input = &mut (&*input);

        let directives = nmap_service_probes(input)?;

        assert!(input.is_empty(), "{}", input);

        assert_eq!(
            directives[0],
            Directive::Fallback(vec!["GetRequest".into(), "GenericLines".into()])
        );

        Ok(())
    }
}
