pub struct DbMetrics {
    db_bytes: prometheus::IntGaugeVec,
    db_wal_bytes: prometheus::IntGaugeVec,
    db_shm_bytes: prometheus::IntGaugeVec,
    subs_count: prometheus::IntGaugeVec,
    subs_bytes: prometheus::IntGaugeVec,
    subs_wal_bytes: prometheus::IntGaugeVec,
    subs_shm_bytes: prometheus::IntGaugeVec,
    db_path: crate::PathBuf,
    db_wal_path: crate::PathBuf,
    db_shm_path: crate::PathBuf,
    sub_path: crate::PathBuf,
}

impl DbMetrics {
    pub fn new(
        registry: &'static prometheus::Registry,
        db_path: crate::PathBuf,
        sub_path: crate::PathBuf,
    ) -> &'static Self {
        static THIS: std::sync::OnceLock<DbMetrics> = std::sync::OnceLock::new();

        THIS.get_or_init(|| {
            let db_bytes = prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_db_bytes",
                    "Size in bytes of the main corrosion database file",
                },
                &[],
                registry,
            }
            .unwrap();
            let db_wal_bytes = prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_db_wal_bytes",
                    "Size in bytes of the main corrosion database WAL file",
                },
                &[],
                registry,
            }
            .unwrap();
            let db_shm_bytes = prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_db_shm_bytes",
                    "Size in bytes of the main corrosion database SHM file",
                },
                &[],
                registry,
            }
            .unwrap();
            let subs_count = prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_sub_count",
                    "Count of current corrosion database subscribers",
                },
                &[],
                registry,
            }
            .unwrap();
            let subs_bytes = prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_sub_db_bytes",
                    "Size in bytes of all corrosion subscriber databases",
                },
                &[],
                registry,
            }
            .unwrap();
            let subs_wal_bytes = prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_sub_db_wal_bytes",
                    "Size in bytes of all corrosion subscriber database WAL files",
                },
                &[],
                registry,
            }
            .unwrap();
            let subs_shm_bytes = prometheus::register_int_gauge_vec_with_registry! {
                prometheus::opts! {
                    "corrosion_sub_db_shm_bytes",
                    "Size in bytes of all corrosion subscriber database SHM files",
                },
                &[],
                registry,
            }
            .unwrap();

            let mut db_wal_path = db_path.clone();
            db_wal_path.set_extension(format!("{}-wal", db_path.extension().unwrap_or_default()));
            let mut db_shm_path = db_path.clone();
            db_shm_path.set_extension(format!("{}-shm", db_path.extension().unwrap_or_default()));

            Self {
                db_bytes,
                db_wal_bytes,
                db_shm_bytes,
                subs_count,
                subs_bytes,
                subs_wal_bytes,
                subs_shm_bytes,
                db_path,
                db_wal_path,
                db_shm_path,
                sub_path,
            }
        })
    }

    /// Updates the metrics from the current on disk values
    pub fn update(&self) {
        fn file_size(path: &crate::Path, cb: impl FnOnce(u64)) {
            match path.metadata() {
                Ok(md) => {
                    cb(md.len());
                }
                Err(error) => {
                    tracing::warn!(%error, %path, "failed to retrieve file size");
                }
            }
        }

        file_size(&self.db_path, |len| {
            self.db_bytes.with_label_values::<&str>(&[]).set(len as _);
        });
        file_size(&self.db_wal_path, |len| {
            self.db_wal_bytes
                .with_label_values::<&str>(&[])
                .set(len as _);
        });
        file_size(&self.db_shm_path, |len| {
            self.db_shm_bytes
                .with_label_values::<&str>(&[])
                .set(len as _);
        });

        let Ok(dir) = self.sub_path.read_dir_utf8() else {
            return;
        };

        let mut sub_count = 0;
        let mut db_sizes = 0;
        let mut wal_sizes = 0;
        let mut shm_sizes = 0;

        for entry in dir {
            let Ok(entry) = entry else {
                continue;
            };

            // Each directory is 32-byte hex UUID
            if entry.file_type().map_or(true, |ft| !ft.is_dir())
                || entry.file_name().len() != 32
                || entry.file_name().contains(|c: char| !c.is_ascii_hexdigit())
            {
                continue;
            }

            sub_count += 1;

            let Ok(sdir) = entry.path().read_dir_utf8() else {
                continue;
            };

            for sentry in sdir {
                let Ok(sentry) = sentry else {
                    continue;
                };

                let Some(ext) = sentry.path().extension() else {
                    continue;
                };

                let counter = match ext {
                    "sqlite" => &mut db_sizes,
                    "sqlite-wal" => &mut wal_sizes,
                    "sqlite-shm" => &mut shm_sizes,
                    _ => continue,
                };

                file_size(sentry.path(), |len| {
                    *counter += len;
                });
            }
        }

        self.subs_count
            .with_label_values::<&str>(&[])
            .set(sub_count);
        self.subs_bytes
            .with_label_values::<&str>(&[])
            .set(db_sizes as _);
        self.subs_wal_bytes
            .with_label_values::<&str>(&[])
            .set(wal_sizes as _);
        self.subs_shm_bytes
            .with_label_values::<&str>(&[])
            .set(shm_sizes as _);
    }
}
