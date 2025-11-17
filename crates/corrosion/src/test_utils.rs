/// Corrosion uses a "tripwire" handle to signal to end async tasks, this just
/// wraps it so it's easier to use, and removes boilerplate
pub struct Trip {
    tripwire: tripwire::Tripwire,
    worker: tripwire::TripwireWorker<tokio_stream::wrappers::ReceiverStream<()>>,
    tx: tokio::sync::mpsc::Sender<()>,
}

impl Trip {
    #[inline]
    pub fn new() -> Self {
        let (tripwire, worker, tx) = tripwire::Tripwire::new_simple();
        Self {
            tripwire,
            worker,
            tx,
        }
    }

    #[inline]
    pub fn tripwire(&self) -> tripwire::Tripwire {
        self.tripwire.clone()
    }

    #[inline]
    pub async fn shutdown(self) {
        self.tx.send(()).await.ok();
        self.worker.await;
        spawn::wait_for_all_pending_handles().await;
    }
}

pub struct TestDbPool {
    #[allow(dead_code)]
    temp: tempfile::TempDir,
    sub_path: camino::Utf8PathBuf,
}

impl TestDbPool {
    pub async fn new(schema: &str) -> Self {
        let mut schema = corro_types::schema::parse_sql(schema).expect("failed to parse schema");
        let temp = tempfile::TempDir::new().expect("failed to create temp dir");

        let sub_path = camino::Utf8Path::from_path(temp.path())
            .expect("non-utf8 path")
            .join("subs");

        let pool = corro_types::agent::SplitPool::create(
            temp.path().join("db.db"),
            std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        )
        .await
        .expect("failed to create DB pool");
        let clock = std::sync::Arc::new(uhlc::HLC::default());

        {
            let mut conn = pool
                .write_priority()
                .await
                .expect("failed to get DB connection");
            corro_types::sqlite::setup_conn(&conn).expect("failed to setup connection");
            corro_types::agent::migrate(clock, &mut conn).expect("failed to migrate");
            let tx = conn.transaction().expect("failed to start transaction");
            corro_types::schema::apply_schema(
                &tx,
                &corro_types::schema::Schema::default(),
                &mut schema,
            )
            .expect("failed to apply schema");
            tx.commit().expect("failed to commit schema change");
        }

        Self { temp, sub_path }
    }

    #[inline]
    pub fn subscriptions(&self) -> &camino::Utf8Path {
        &self.sub_path
    }
}
