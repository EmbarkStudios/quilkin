use corro_types as ct;

/// Tests subscriptions to server notifications work properly
#[tokio::test]
async fn subscriptions() {
    let tw = corrosion::test_utils::Trip::new();
    let mut pool = corrosion::test_utils::TestDbPool::new(corrosion::schema::SCHEMA).await;

    let sql = "SELECT sandwich FROM sw WHERE pk=\"mad\"";

    let subs = ct::pubsub::SubsManager::default();

    {
        let tx = conn.transaction().unwrap();
        tx.execute_batch(r#"
                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-1', 'app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-1"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-1', 'mv-1');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-1', 'mv-1');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-1', 'mv-1', 'started');

                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-2', 'not-app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-2"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-2', 'mv-2');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-2', 'mv-2');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-2', 'mv-2', 'started');
                    "#).unwrap();
        tx.commit().unwrap();
    }

    {
        let (handle, maybe_created) = subs.get_or_insert(
            sql,
            subscriptions_path.as_path(),
            &schema,
            &pool,
            tw.tripwire(),
        )?;

        assert!(maybe_created.is_some());

        handle.cleanup().await;
        subs.remove(&handle.id());
    }

    tw.shutdown().await;
}
