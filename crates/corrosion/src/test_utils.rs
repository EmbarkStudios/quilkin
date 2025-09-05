use crate::{
    api::{self, Statement},
    types,
};

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
    subs: types::pubsub::SubsManager,
    schema: types::schema::Schema,
    pool: types::agent::SplitPool,
    trip: Trip,
}

impl TestDbPool {
    pub async fn new(schema: &str) -> Self {
        let mut schema = types::schema::parse_sql(schema).expect("failed to parse schema");
        let temp = tempfile::TempDir::new().expect("failed to create temp dir");

        let sub_path = camino::Utf8Path::from_path(temp.path())
            .expect("non-utf8 path")
            .join("subs");

        let pool = types::agent::SplitPool::create(
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
            types::sqlite::setup_conn(&conn).expect("failed to setup connection");
            types::agent::migrate(clock, &mut conn).expect("failed to migrate");
            let tx = conn.transaction().expect("failed to start transaction");
            types::schema::apply_schema(&tx, &types::schema::Schema::default(), &mut schema)
                .expect("failed to apply schema");
            tx.commit().expect("failed to commit schema change");
        }

        Self {
            temp,
            sub_path,
            subs: types::pubsub::SubsManager::default(),
            schema,
            pool,
            trip: Trip::new(),
        }
    }

    #[inline]
    pub fn subscribe_new(
        &self,
        sql: &str,
    ) -> (
        types::pubsub::MatcherHandle,
        tokio::sync::mpsc::Receiver<api::QueryEvent>,
    ) {
        let (handle, maybe) = self
            .subs
            .get_or_insert(
                sql,
                &self.sub_path,
                &self.schema,
                &self.pool,
                self.trip.tripwire(),
            )
            .expect("failed to create subscription");
        let created = maybe.expect("did not create a new matcher").evt_rx;
        (handle, created)
    }

    pub async fn transaction(&self, ops: impl Iterator<Item = Statement>) {
        let mut conn = self
            .pool
            .write_priority()
            .await
            .expect("failed to get connection");
        let tx = conn.transaction().expect("failed to get transaction");

        for stmt in ops {
            let mut prepped = tx
                .prepare(stmt.query())
                .expect("failed to pepare transaction");
            match stmt {
                Statement::Simple(_)
                | Statement::Verbose {
                    params: None,
                    named_params: None,
                    ..
                } => prepped.execute([]),
                Statement::WithParams(_, params)
                | Statement::Verbose {
                    params: Some(params),
                    ..
                } => prepped.execute(rusqlite::params_from_iter(params)),
                Statement::WithNamedParams(_, params)
                | Statement::Verbose {
                    named_params: Some(params),
                    ..
                } => prepped.execute(
                    params
                        .iter()
                        .map(|(k, v)| (k.as_str(), v as &dyn rusqlite::ToSql))
                        .collect::<Vec<(&str, &dyn rusqlite::ToSql)>>()
                        .as_slice(),
                ),
            }
            .expect("failed to execute");
        }

        tx.commit().expect("failed to commit transaction");
    }

    #[inline]
    pub async fn shutdown(self) {
        self.subs.drop_handles().await;
        self.trip.shutdown().await;
    }
}
