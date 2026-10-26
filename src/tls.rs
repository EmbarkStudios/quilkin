//! PEM handling shared by the control plane's own identity and the trust its
//! xDS and mDS clients verify servers against.

/// Checks that `pem` holds a certificate.
pub(crate) fn validate_pem_cert(pem: &[u8]) -> Result<(), String> {
    use rustls::pki_types::{CertificateDer, pem::PemObject as _};

    CertificateDer::from_pem_slice(pem)
        .map(drop)
        .map_err(|error| format!("not a PEM encoded certificate: {error}"))
}

/// Checks that `pem` holds a private key.
pub(crate) fn validate_pem_key(pem: &[u8]) -> Result<(), String> {
    use rustls::pki_types::{PrivateKeyDer, pem::PemObject as _};

    PrivateKeyDer::from_pem_slice(pem)
        .map(drop)
        .map_err(|error| format!("not a PEM encoded private key: {error}"))
}

/// Checks that `raw` holds a PEM certificate, returning it unchanged.
///
/// Parsing here rather than at the point of use means clap reports the failure
/// against the flag the value came from.
pub(crate) fn parse_pem_cert(raw: &str) -> Result<String, String> {
    validate_pem_cert(raw.as_bytes()).map(|()| raw.into())
}

/// Checks that `raw` holds a PEM private key, returning it unchanged.
pub(crate) fn parse_pem_key(raw: &str) -> Result<String, String> {
    validate_pem_key(raw.as_bytes()).map(|()| raw.into())
}

/// Reads a PEM file, naming the flag it came from when it cannot be used.
///
/// The path flags are checked here rather than in a clap value parser, which
/// sees the path and not the content behind it.
pub(crate) fn read_pem_file(
    path: &std::path::Path,
    flag: &str,
    validate: fn(&[u8]) -> Result<(), String>,
) -> crate::Result<Vec<u8>> {
    use eyre::WrapErr as _;

    let pem = std::fs::read(path)
        .with_context(|| format!("--{flag}: failed to read {}", path.display()))?;

    match validate(&pem) {
        Ok(()) => Ok(pem),
        Err(error) => Err(eyre::eyre!("--{flag}: {}: {error}", path.display())),
    }
}

/// How long the certificate files have to stay quiet before they are read.
///
/// A certificate and its key are written separately, and reading between the
/// two writes would see a pair that does not match.
pub(crate) const RELOAD_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(5);

/// The directories holding `paths`, deduplicated.
///
/// Directories rather than the files themselves: a certificate mounted from a
/// Kubernetes secret is rotated by swapping a symlinked directory, which a
/// watch on the file never sees.
pub(crate) fn watched_directories(
    paths: impl IntoIterator<Item = std::path::PathBuf>,
) -> Vec<std::path::PathBuf> {
    let mut dirs: Vec<_> = paths
        .into_iter()
        .filter_map(|path| path.parent().map(std::path::Path::to_path_buf))
        .collect();

    dirs.sort();
    dirs.dedup();
    dirs
}

/// Watches `dirs`, reporting changes to the files in them.
fn watch(
    dirs: &[std::path::PathBuf],
) -> crate::Result<(
    notify::RecommendedWatcher,
    tokio::sync::mpsc::UnboundedReceiver<()>,
)> {
    use notify::Watcher as _;

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut watcher = notify::RecommendedWatcher::new(
        move |event: notify::Result<notify::Event>| {
            if event.is_ok_and(|event| is_change(event.kind)) {
                let _ = tx.send(());
            }
        },
        <_>::default(),
    )?;

    for dir in dirs {
        watcher.watch(dir, notify::RecursiveMode::NonRecursive)?;
    }

    Ok((watcher, rx))
}

/// Calls `apply` once a burst of changes has gone quiet, until `shutdown`.
///
/// Split from the watcher so the part with the decisions in it can be driven
/// by a channel and a clock rather than a filesystem.
pub(crate) async fn debounced(
    mut changes: tokio::sync::mpsc::UnboundedReceiver<()>,
    mut shutdown: tokio::sync::watch::Receiver<()>,
    quiet_for: std::time::Duration,
    mut apply: impl FnMut(),
) {
    loop {
        // Nothing pending, so wait for the change that opens a burst.
        tokio::select! {
            change = changes.recv() => if change.is_none() { return },
            _ = shutdown.changed() => return,
        }

        // Every further change restarts the quiet period, so a certificate
        // and the key written after it settle as one rotation.
        loop {
            tokio::select! {
                () = tokio::time::sleep(quiet_for) => break,
                change = changes.recv() => if change.is_none() { return },
                _ = shutdown.changed() => return,
            }
        }

        apply();
    }
}

/// Reloads attempted, by what was reloaded and whether it took effect.
///
/// A rotation that keeps failing is otherwise invisible until the running
/// certificate expires.
fn reloads_total(subject: &str, outcome: &'static str) -> prometheus::IntCounter {
    static METRIC: once_cell::sync::Lazy<prometheus::IntCounterVec> =
        once_cell::sync::Lazy::new(|| {
            prometheus::register_int_counter_vec_with_registry! {
                prometheus::opts! {
                    "quilkin_tls_reloads_total",
                    "Total number of certificate reloads by `subject` and `outcome`",
                },
                &["subject", "outcome"],
                crate::metrics::registry(),
            }
            .unwrap()
        });

    METRIC.with_label_values(&[subject, outcome])
}

/// Seconds until the certificate in use expires.
///
/// Reports whether rotation is keeping up, which the reload counter alone does
/// not say: a rotation can succeed and still install something near expiry.
fn expiry_seconds(subject: &str) -> prometheus::IntGauge {
    static METRIC: once_cell::sync::Lazy<prometheus::IntGaugeVec> =
        once_cell::sync::Lazy::new(|| {
            prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "quilkin_tls_certificate_expiry_seconds",
                    "Seconds until the certificate in use for `subject` expires",
                },
                &["subject"],
                crate::metrics::registry(),
            }
            .unwrap()
        });

    METRIC.with_label_values(&[subject])
}

/// Reports the certificate now in use, so a rotation is visible as more than a
/// log line.
pub(crate) fn report_certificate(subject: &str, cert: &[u8]) {
    use rustls::pki_types::{CertificateDer, pem::PemObject as _};

    let Ok(der) = CertificateDer::from_pem_slice(cert) else {
        return;
    };
    let Ok((_, parsed)) = x509_parser::parse_x509_certificate(&der) else {
        return;
    };

    let expires_in = parsed.validity().time_to_expiration();
    expiry_seconds(subject).set(
        expires_in
            .map(|left| left.whole_seconds())
            .unwrap_or_default(),
    );

    tracing::info!(
        %subject,
        not_after = %parsed.validity().not_after,
        "certificate in use"
    );
}

/// Reloads from `paths` whenever what is behind them changes.
///
/// `apply` re-reads and installs; reporting is shared so both the listener's
/// own certificate and the one a client presents say the same thing.
pub(crate) fn spawn_reload(
    subject: String,
    paths: impl IntoIterator<Item = std::path::PathBuf>,
    shutdown: tokio::sync::watch::Receiver<()>,
    mut apply: impl FnMut() -> crate::Result<Vec<u8>> + Send + 'static,
) -> crate::Result<()> {
    let dirs = watched_directories(paths);
    let (watcher, changes) = watch(&dirs)?;

    for dir in &dirs {
        tracing::info!(%subject, path = %dir.display(), "watching for certificate changes");
    }

    tokio::spawn(async move {
        // Dropping the watcher stops the events, so it lives as long as the
        // task does.
        let _watcher = watcher;

        debounced(changes, shutdown, RELOAD_DEBOUNCE, move || match apply() {
            Ok(cert) => {
                reloads_total(&subject, "ok").inc();
                tracing::info!(%subject, "serving with the rotated certificate");
                report_certificate(&subject, &cert);
            }
            Err(error) => {
                reloads_total(&subject, "failed").inc();
                tracing::error!(
                    %subject,
                    %error,
                    "keeping the running certificate: the files on disk are not usable"
                );
            }
        })
        .await;
    });

    Ok(())
}

/// Whether a filesystem event is a change to the certificate files rather than
/// a read of them.
///
/// Reloading reads the files, and on Linux a read is itself an event, so
/// reacting to everything makes a watcher that reloads forever at the debounce
/// interval. Renames count: a certificate mounted from a Kubernetes secret is
/// rotated by swapping a symlinked directory.
pub(crate) fn is_change(kind: notify::EventKind) -> bool {
    use notify::EventKind;

    matches!(
        kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    const QUIET: Duration = Duration::from_secs(5);

    /// Drives [`debounced`] with a channel and a clock, counting the reloads.
    fn reloader() -> (
        tokio::sync::mpsc::UnboundedSender<()>,
        tokio::sync::watch::Sender<()>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let (changes_tx, changes) = tokio::sync::mpsc::unbounded_channel();
        let (stop, shutdown) = tokio::sync::watch::channel(());
        let reloads = Arc::new(AtomicUsize::new(0));

        let counted = reloads.clone();
        let task = tokio::spawn(debounced(changes, shutdown, QUIET, move || {
            counted.fetch_add(1, Ordering::SeqCst);
        }));

        (changes_tx, stop, reloads, task)
    }

    async fn settle() {
        tokio::task::yield_now().await;
    }

    /// A rotation writes the certificate and the key separately, so one
    /// rotation has to mean one reload however many events it produces.
    #[tokio::test(start_paused = true)]
    async fn a_burst_settles_into_one_reload() {
        let (changes, _stop, reloads, _task) = reloader();

        changes.send(()).unwrap();
        settle().await;

        // The key lands a second later, restarting the quiet period.
        tokio::time::advance(Duration::from_secs(1)).await;
        changes.send(()).unwrap();
        settle().await;

        // Which means the original deadline passes without a reload.
        tokio::time::advance(QUIET - Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(
            reloads.load(Ordering::SeqCst),
            0,
            "a reload ran while the files were still being written"
        );

        tokio::time::advance(QUIET).await;
        settle().await;
        assert_eq!(
            reloads.load(Ordering::SeqCst),
            1,
            "the settled burst should have reloaded exactly once"
        );
    }

    /// The quiet period re-arms, so the rotation after this one still lands.
    #[tokio::test(start_paused = true)]
    async fn a_later_rotation_reloads_again() {
        let (changes, _stop, reloads, _task) = reloader();

        for _ in 0..2 {
            changes.send(()).unwrap();
            settle().await;
            tokio::time::advance(QUIET * 2).await;
            settle().await;
        }

        assert_eq!(reloads.load(Ordering::SeqCst), 2);
    }

    /// Shutdown stops the watcher rather than leaving it behind, even with a
    /// burst still waiting to settle.
    #[tokio::test(start_paused = true)]
    async fn shutdown_ends_the_watch() {
        let (changes, stop, reloads, task) = reloader();

        changes.send(()).unwrap();
        settle().await;
        stop.send(()).unwrap();

        tokio::time::timeout(QUIET * 2, task)
            .await
            .expect("shutdown should have ended the loop")
            .unwrap();

        assert_eq!(
            reloads.load(Ordering::SeqCst),
            0,
            "a burst interrupted by shutdown should not reload"
        );
    }

    /// Reloading reads the certificate files, and on Linux a read is itself an
    /// event, so a watcher that reacts to everything reloads forever.
    #[test]
    fn reading_the_certificates_is_not_a_change() {
        use notify::{EventKind, event};

        assert!(!is_change(EventKind::Access(event::AccessKind::Read)));
        assert!(!is_change(EventKind::Access(event::AccessKind::Open(
            event::AccessMode::Read
        ))));

        assert!(is_change(EventKind::Modify(event::ModifyKind::Data(
            event::DataChange::Content
        ))));
        // A secret is rotated by swapping a symlinked directory, not by
        // writing through the file.
        assert!(is_change(EventKind::Modify(event::ModifyKind::Name(
            event::RenameMode::To
        ))));
        assert!(is_change(EventKind::Create(event::CreateKind::File)));
    }

    /// The directory is what gets watched, and watching one twice would
    /// deliver every change twice.
    #[test]
    fn certificates_in_one_directory_are_watched_once() {
        let dirs = watched_directories(
            ["/etc/certs/tls.crt", "/etc/certs/tls.key", "/etc/ca/ca.crt"]
                .into_iter()
                .map(Into::into),
        );

        assert_eq!(
            dirs,
            vec![
                std::path::PathBuf::from("/etc/ca"),
                std::path::PathBuf::from("/etc/certs")
            ]
        );
    }
}
