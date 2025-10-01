use corro_api_types::{self as api, Statement};
use corro_types::{self as types, pubsub, updates::Handle as _};

pub use prettytable::Cell;

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

pub struct TestSubsDb {
    #[allow(dead_code)]
    temp: tempfile::TempDir,
    db_path: camino::Utf8PathBuf,
    sub_path: camino::Utf8PathBuf,
    subs: pubsub::SubsManager,
    schema: types::schema::Schema,
    pool: types::agent::SplitPool,
    matcher_conns: std::collections::BTreeMap<uuid::Uuid, types::sqlite::CrConn>,
    db_version: usize,
    trip: Trip,
}

impl TestSubsDb {
    pub async fn new(schema: &str) -> Self {
        let temp = tempfile::TempDir::new().expect("failed to create temp dir");

        let root = camino::Utf8Path::from_path(temp.path()).expect("non-utf8 path");
        let sub_path = root.join("subs");
        let db_path = root.join("db.db");
        let pool = types::agent::SplitPool::create(
            &db_path,
            std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        )
        .await
        .expect("failed to create DB pool");

        let schema = setup(schema, &pool).await;

        Self {
            temp,
            sub_path,
            db_path,
            subs: types::pubsub::SubsManager::default(),
            schema,
            pool,
            matcher_conns: Default::default(),
            db_version: 0,
            trip: Trip::new(),
        }
    }

    #[inline]
    pub fn subscribe_new(
        &mut self,
        sql: &str,
    ) -> (
        pubsub::MatcherHandle,
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

        let matcher_conn = corro_types::sqlite::CrConn::init(
            rusqlite::Connection::open(&self.db_path).expect("could not open conn"),
        )
        .expect("could not init crconn");

        corro_types::sqlite::setup_conn(&matcher_conn).expect("failed to setup matcher connection");
        self.matcher_conns.insert(handle.id(), matcher_conn);

        (handle, created)
    }

    pub async fn transaction(&mut self, ops: impl Iterator<Item = &Statement>) {
        let mut conn = self
            .pool
            .write_priority()
            .await
            .expect("failed to get connection");
        let tx = conn.transaction().expect("failed to get transaction");

        exec(&tx, ops).expect("failed to exec statement");

        tx.commit().expect("failed to commit transaction");
        self.db_version += 1;
    }

    pub fn send_changes(&self, matcher: &pubsub::MatcherHandle) {
        let mut candidates = pubsub::MatchCandidates::new();

        let conn = self
            .matcher_conns
            .get(&matcher.id())
            .expect("matcher was not created");

        let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl FROM crsql_changes WHERE db_version = ? ORDER BY seq ASC"#).expect("failed to prep");
        let rows = prepped
            .query_map(
                rusqlite::params![self.db_version],
                types::change::row_to_change,
            )
            .expect("failed to run query");

        for row in rows {
            let change = row.expect("failed to deserialize change row");
            matcher.filter_matchable_change(&mut candidates, (&change).into());
        }

        matcher
            .changes_tx()
            .try_send(candidates)
            .expect("failed to send changes")
    }

    #[inline]
    pub async fn shutdown(self) {
        self.subs.drop_handles().await;
        self.trip.shutdown().await;
    }

    #[inline]
    pub async fn remove_handle(&self, handle: pubsub::MatcherHandle) {
        handle.cleanup().await;
        self.subs.remove(&handle.id());
    }
}

pub async fn setup(schema: &str, pool: &types::agent::SplitPool) -> types::schema::Schema {
    let mut schema = types::schema::parse_sql(schema).expect("failed to parse schema");
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

    schema
}

pub async fn new_split_pool(name: &str, schema: &str) -> corro_types::agent::SplitPool {
    let sp = corro_types::agent::SplitPool::create_in_memory(
        name,
        std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
    )
    .await
    .expect("failed to create split pool");

    setup(schema, &sp).await;
    sp
}

pub fn exec<'t, 's>(
    tx: &rusqlite::Transaction<'t>,
    ops: impl Iterator<Item = &'s Statement>,
) -> rusqlite::Result<usize> {
    let mut rows = 0;
    for stmt in ops {
        let mut prepped = tx.prepare(stmt.query())?;
        rows += match stmt {
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
        }?;
    }

    Ok(rows)
}

use prettytable as pt;

pub fn query_to_string(
    mut statement: rusqlite::Statement<'_>,
    conv: impl Fn(&rusqlite::Row<'_>, &mut pt::Row),
) -> String {
    let mut tab = pt::Table::new();
    tab.set_titles(pt::Row::new(
        statement
            .column_names()
            .into_iter()
            .map(|name| pt::Cell::new(name))
            .collect(),
    ));

    let mut rows = statement.query([]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let mut ptrow = pt::Row::empty();
        conv(row, &mut ptrow);
        tab.add_row(ptrow);
    }

    let mut out = Vec::new();
    tab.print(&mut out).unwrap();
    String::from_utf8(out).unwrap()
}
