//! Verifies `BufferingSubStream` drains already-queued events without waiting
//! on its flush timer.

use quilkin_corrosion::pubsub::{BufferingSubStream, ChangeId, QueryEvent, Subscription};
use tokio_stream::StreamExt as _;

#[tokio::test(flavor = "multi_thread")]
async fn drains_burst_quickly() {
    let (tx, rx) = tokio::sync::mpsc::channel(10240);

    const EVENTS: usize = 100;

    // Build realistic ~150 byte row-change events, pre-serialized the same way
    // process_sub_channel does it
    let mut buf = quilkin_corrosion::codec::PrefixedBuf::new();
    for i in 0..EVENTS {
        let evt = QueryEvent::Change(
            quilkin_corrosion::pubsub::ChangeType::Insert,
            corro_api_types::RowId(i as u64),
            vec![
                corro_api_types::SqliteValue::Text(
                    format!("|1234:5678:9abc:def0::{i:x}:7777").into(),
                ),
                corro_api_types::SqliteValue::Text("XXXX".into()),
                corro_api_types::SqliteValue::Text("dG9rZW4tdmFsdWUtd2l0aC1zb21lLWxlbmd0aA".into()),
            ],
            ChangeId(i as u64 + 1),
        );
        let sub_evt = quilkin_corrosion::pubsub::query_to_sub_event(&mut buf, evt).unwrap();
        tx.send(sub_evt).await.unwrap();
    }
    drop(tx);

    let sub = Subscription {
        id: uuid::Uuid::nil(),
        query_hash: String::new(),
        rx,
    };

    // The defaults used by the real server: max_buffer ~1480 bytes, max_time 10ms
    let mut bs = BufferingSubStream::new(
        sub,
        quilkin_corrosion::pubsub::max_buffer(),
        quilkin_corrosion::pubsub::max_time(),
    );

    let start = std::time::Instant::now();
    let mut events = 0usize;
    while let Some(mut frame) = bs.next().await {
        let stream = quilkin_corrosion::pubsub::SubscriptionStream::length_prefixed(&mut frame)
            .expect("emitted frames are length prefixed");
        events += stream.filter(Result::is_ok).count();
    }
    let elapsed = start.elapsed();

    assert_eq!(events, EVENTS, "all queued events must be delivered");
    // All events are already queued, so they should be emitted as ~10 full
    // frames with no timer waits in between
    assert!(
        elapsed < std::time::Duration::from_millis(100),
        "draining {EVENTS} pre-queued events took {elapsed:?}"
    );
}
