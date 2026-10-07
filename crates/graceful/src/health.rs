use tokio::sync::mpsc;

type HealthState = (&'static str, bool, Option<String>);

/// Used to enqueue the various tasks that contribute to the readiness and health of Quilkin as a whole.
///
/// Once all services have finished initialization Quilkin is marked as both ready and healthy, and each service can
/// transition all of Quilkin to/from an unhealthy state periodically.
pub struct ChecksInit {
    registered: Vec<&'static str>,
    rx: mpsc::UnboundedReceiver<HealthState>,
    tx: mpsc::UnboundedSender<HealthState>,
}

impl ChecksInit {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            registered: Vec::new(),
            rx,
            tx,
        }
    }

    #[inline]
    pub fn add(&mut self, name: &'static str) -> HealthToken {
        assert!(
            !self.registered.contains(&name),
            "{name} already exists in the set"
        );
        self.registered.push(name);

        HealthToken {
            name,
            tx: self.tx.clone(),
        }
    }

    /// Use when setup of all health checks have completed
    pub fn finished(self) -> Checks {
        let mut registered: Vec<_> = self
            .registered
            .into_iter()
            .map(|name| (name, false))
            .collect();
        registered.sort_by_key(|(k, _)| *k);

        Checks {
            registered,
            rx: self.rx,
        }
    }
}

#[derive(Clone)]
pub struct HealthToken {
    name: &'static str,
    tx: mpsc::UnboundedSender<HealthState>,
}

impl HealthToken {
    /// Gets the name associated with this token
    #[inline]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Marks the service as ready.
    ///
    /// This assumes that the service is healthy once ready
    #[inline]
    pub fn ready(&self) {
        drop(self.tx.send((self.name, true, None)));
    }

    /// Mark as un/healthy with an optional reason
    #[inline]
    pub fn mark_healthiness(&self, healthy: bool, reason: Option<String>) {
        drop(self.tx.send((self.name, healthy, reason)));
    }

    /// Creates a non-functioning health token
    #[inline]
    pub fn testing() -> Self {
        Self {
            name: "testing-token",
            tx: mpsc::unbounded_channel().0,
        }
    }
}

pub struct Checks {
    pub registered: Vec<(&'static str, bool)>,
    pub rx: mpsc::UnboundedReceiver<HealthState>,
}

impl Checks {
    #[inline]
    pub fn get(&mut self, name: &'static str) -> Option<&mut (&'static str, bool)> {
        self.registered
            .binary_search_by_key(&name, |(k, _)| k)
            .ok()
            .map(|i| &mut self.registered[i])
    }
}
