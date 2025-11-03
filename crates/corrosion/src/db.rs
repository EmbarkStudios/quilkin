use corro_types::{
    actor::ActorId,
    agent::{SplitPool, WriteConn},
    schema::Schema,
    sqlite::CrConn,
};
use std::sync::Arc;

pub mod read;
pub mod write;

pub struct InitializedDb {
    pub pool: SplitPool,
    pub clock: Arc<uhlc::HLC>,
    pub schema: Arc<Schema>,
    pub actor_id: ActorId,
}

impl InitializedDb {
    pub async fn setup(db_path: &crate::Path, schema: &str) -> eyre::Result<Self> {
        let partial_schema = corro_types::schema::parse_sql(schema)?;

        let actor_id = {
            // we need to set auto_vacuum before any tables are created
            let db_conn = rusqlite::Connection::open(db_path)?;
            db_conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL")?;

            let conn = CrConn::init(db_conn)?;
            conn.query_row("SELECT crsql_site_id();", [], |row| {
                row.get::<_, ActorId>(0)
            })?
        };

        let write_sema = Arc::new(tokio::sync::Semaphore::new(1));
        let pool = SplitPool::create(&db_path, write_sema.clone()).await?;

        let clock = Arc::new(
            uhlc::HLCBuilder::default()
                .with_id(actor_id.try_into().unwrap())
                .with_max_delta(std::time::Duration::from_millis(300))
                .build(),
        );

        let schema = {
            let mut conn = pool.write_priority().await?;

            let old_schema = {
                corro_types::agent::migrate(clock.clone(), &mut conn)?;
                let mut schema = corro_types::schema::init_schema(&conn)?;
                schema.constrain()?;

                schema
            };

            tokio::task::block_in_place(|| update_schema(&mut conn, old_schema, partial_schema))?
        };

        Ok(Self {
            pool,
            clock,
            schema: Arc::new(schema),
            actor_id,
        })
    }
}

pub fn update_schema(
    conn: &mut WriteConn,
    old_schema: Schema,
    new_schema: Schema,
) -> eyre::Result<Schema> {
    // clone the previous schema and apply
    let mut new_schema = {
        let mut schema = old_schema.clone();
        for (name, def) in new_schema.tables.iter() {
            // overwrite table because users are expected to return a full table def
            schema.tables.insert(name.clone(), def.clone());
        }
        schema
    };

    new_schema.constrain()?;

    let tx = conn.immediate_transaction()?;

    corro_types::schema::apply_schema(&tx, &old_schema, &mut new_schema)?;

    for tbl_name in new_schema.tables.keys() {
        tx.execute("DELETE FROM __corro_schema WHERE tbl_name = ?", [tbl_name])?;

        let n = tx.execute("INSERT INTO __corro_schema SELECT tbl_name, type, name, sql, 'api' AS source FROM sqlite_schema WHERE tbl_name = ? AND type IN ('table', 'index') AND name IS NOT NULL AND sql IS NOT NULL", [tbl_name])?;
        tracing::info!("Updated {n} rows in __corro_schema for table {tbl_name}");
    }

    tx.commit()?;
    Ok(new_schema)
}
