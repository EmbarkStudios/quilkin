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
