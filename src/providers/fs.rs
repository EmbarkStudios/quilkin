/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use notify::Watcher;
use tracing::Instrument;

use crate::Config;

pub async fn watch(
    config: Arc<Config>,
    health_check: Arc<AtomicBool>,
    path: impl Into<std::path::PathBuf>,
    locality: Option<crate::net::endpoint::Locality>,
) -> crate::Result<()> {
    let path = path.into();
    let span = tracing::info_span!("config_provider", path = %path.display(), id = %config.id());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    async fn watch_inner(
        config: &Config,
        path: &std::path::Path,
        locality: Option<crate::net::endpoint::Locality>,
        tx: tokio::sync::mpsc::UnboundedSender<Result<notify::Event, notify::Error>>,
    ) -> crate::Result<notify::RecommendedWatcher> {
        tracing::info!("discovering configuration through filesystem");
        let mut watcher = notify::RecommendedWatcher::new(
            move |res| {
                tx.send(res).unwrap();
            },
            Default::default(),
        )
        .unwrap();

        tracing::trace!("reading file");
        let buf = tokio::fs::read(path).await?;
        tracing::info!("applying initial configuration");
        config.update_from_json(serde_yaml::from_slice(&buf)?, locality)?;
        watcher.watch(path, notify::RecursiveMode::Recursive)?;
        tracing::info!("watching file");
        Ok(watcher)
    }

    let _watcher = watch_inner(&config, &path, locality.clone(), tx)
        .instrument(span.clone())
        .await?;

    health_check.store(true, Ordering::SeqCst);

    while let Some(event) = rx.recv().instrument(span.clone()).await.transpose()? {
        tracing::trace!(event = ?event.kind, "new file event");

        if !matches!(
            event.kind,
            notify::EventKind::Modify(notify::event::ModifyKind::Data(_))
        ) {
            continue;
        }

        for path in event.paths {
            // At least on macOS it's not always safe to
            // immediately read file after the change, a small
            // delay fixes that.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            tracing::info!(path = %path.display(), "file changed, updating config");
            let buf = tokio::fs::read(path).await?;
            config.update_from_json(serde_yaml::from_slice(&buf)?, locality.clone())?;
        }
    }

    Err(eyre::eyre!("filesystem watch unexpectedly stopped"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn basic() {
        let providers = Default::default();
        let service = Default::default();
        let source = crate::Config::new_rc(
            Some("basic".into()),
            Default::default(),
            &providers,
            &service,
            tokio_util::sync::CancellationToken::new(),
        );
        let dest = crate::Config::new_rc(
            Some("basic".into()),
            Default::default(),
            &providers,
            &service,
            tokio_util::sync::CancellationToken::new(),
        );
        assert_eq!(source, dest);

        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.keep().join("config.yaml");
        tokio::fs::write(&file_path, serde_yaml::to_string(&source).unwrap())
            .await
            .unwrap();
        let _handle = tokio::spawn(watch(dest.clone(), <_>::default(), file_path.clone(), None));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        source.dyn_cfg.clusters().unwrap().modify(|clusters| {
            clusters.insert_default(
                [crate::net::endpoint::Endpoint::with_metadata(
                    (std::net::Ipv4Addr::LOCALHOST, 4321).into(),
                    crate::net::endpoint::Metadata {
                        tokens: <_>::from([Vec::from(*b"1x7ijy6")]),
                    },
                )]
                .into(),
            );
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        tokio::fs::write(&file_path, serde_yaml::to_string(&source).unwrap())
            .await
            .unwrap();

        let mut tx = dest.dyn_cfg.clusters().unwrap().watch();
        tokio::time::timeout(std::time::Duration::from_millis(1000), tx.changed())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(source, dest);
    }
}
