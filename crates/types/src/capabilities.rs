use std::{fmt, str::FromStr};

/// What a node offers other nodes, advertised when it joins gossip.
///
/// Deliberately coarser than configuration: a peer cares that a node takes
/// client traffic, not which IO backend it uses, and that gameservers are
/// reachable there, not whether they come from Kubernetes or a file.
///
/// Encoded as a comma separated set, so a name a reader does not know is
/// skipped rather than making the whole set unreadable.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Capabilities(u8);

impl Capabilities {
    /// Accepts game traffic from clients.
    pub const INGRESS: Self = Self(1 << 0);
    /// Gameservers are reachable here. Fixed at startup: it says a provider is
    /// configured, not that any gameserver is running right now.
    pub const GAMESERVERS: Self = Self(1 << 1);
    /// Answers QCMP, so other nodes can measure their latency to it.
    pub const QCMP: Self = Self(1 << 2);
    /// Solves a network coordinate.
    pub const PHOENIX: Self = Self(1 << 3);

    const NAMED: [(Self, &'static str); 4] = [
        (Self::INGRESS, "ingress"),
        (Self::GAMESERVERS, "gameservers"),
        (Self::QCMP, "qcmp"),
        (Self::PHOENIX, "phoenix"),
    ];

    pub const fn empty() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn with(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[must_use]
    pub const fn with_if(self, other: Self, present: bool) -> Self {
        if present { self.with(other) } else { self }
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;

        for (capability, name) in Self::NAMED {
            if self.contains(capability) {
                if !first {
                    f.write_str(",")?;
                }
                f.write_str(name)?;
                first = false;
            }
        }

        Ok(())
    }
}

impl FromStr for Capabilities {
    type Err = std::convert::Infallible;

    /// Unknown names are skipped: a node from a later version advertising
    /// something this one has never heard of is still worth knowing about.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut capabilities = Self::empty();

        for name in value.split(',').map(str::trim).filter(|n| !n.is_empty()) {
            if let Some((capability, _)) = Self::NAMED.iter().find(|(_, known)| *known == name) {
                capabilities = capabilities.with(*capability);
            }
        }

        Ok(capabilities)
    }
}

impl serde::Serialize for Capabilities {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Capabilities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let encoded = <std::borrow::Cow<'_, str>>::deserialize(deserializer)?;
        Ok(encoded.parse().expect("parsing cannot fail"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_text() {
        let capabilities = Capabilities::empty()
            .with(Capabilities::INGRESS)
            .with(Capabilities::QCMP);

        assert_eq!(capabilities.to_string(), "ingress,qcmp");
        assert_eq!(
            capabilities.to_string().parse::<Capabilities>().unwrap(),
            capabilities,
        );

        assert_eq!(Capabilities::empty().to_string(), "");
        assert!("".parse::<Capabilities>().unwrap().is_empty());
    }

    #[test]
    fn keeps_what_it_recognises() {
        // A node from a later version advertising something new is still
        // usable for what it has in common with this one.
        let parsed: Capabilities = "ingress,warp-drive,qcmp".parse().unwrap();

        assert!(parsed.contains(Capabilities::INGRESS));
        assert!(parsed.contains(Capabilities::QCMP));
        assert!(!parsed.contains(Capabilities::GAMESERVERS));
    }

    #[test]
    fn contains_is_a_subset_test() {
        let both = Capabilities::empty()
            .with(Capabilities::INGRESS)
            .with(Capabilities::GAMESERVERS);

        assert!(both.contains(Capabilities::INGRESS));
        assert!(both.contains(both));
        assert!(!Capabilities::INGRESS.contains(both));
    }
}
