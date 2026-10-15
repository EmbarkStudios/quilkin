use corrosion::persistent::proto::{self, VersionedBuf, VersionedRequest, VersionedResponse};
use quilkin_types::{Capabilities, IcaoCode};

/// A peer predating the capability fields is still understood, which is what
/// lets a mixed fleet keep talking during a rollout.
#[test]
fn capabilities_default_when_absent() {
    use corrosion::persistent::proto::v1;

    let mutate: v1::Request =
        serde_json::from_str(r#"{"t":"m","r":{"q":8998,"i":"HHHH"}}"#).unwrap();
    let v1::Request::Mutate(mutate) = mutate else {
        panic!("expected a mutate request");
    };
    assert!(mutate.capabilities.is_empty());
    assert_eq!(mutate.qcmp_port, 8998);

    // Build a current request, then drop the keys an older peer would not have
    // sent, rather than hand writing the encoding.
    let mut encoded = serde_json::to_value(v1::Request::Subscribe(v1::SubscribeRequest {
        params: corrosion::pubsub::SubParamsv1 {
            query: corrosion::api::Statement::Simple("SELECT 1".into()),
            from: None,
            skip_rows: false,
            max_buffer: 0,
            max_time: std::time::Duration::from_millis(10),
            process_interval: std::time::Duration::from_millis(10),
            change_threshold: 0,
        },
        qcmp_port: Some(8998),
        icao: Some(IcaoCode::new_testing([b'H'; 4])),
        capabilities: Capabilities::INGRESS,
    }))
    .unwrap();

    let fields = encoded
        .get_mut("r")
        .and_then(serde_json::Value::as_object_mut)
        .unwrap();
    for key in ["nq", "ni", "nc"] {
        assert!(fields.remove(key).is_some(), "{key} is not being sent");
    }

    let subscribe: v1::Request = serde_json::from_value(encoded).unwrap();
    let v1::Request::Subscribe(subscribe) = subscribe else {
        panic!("expected a subscribe request");
    };
    assert!(subscribe.capabilities.is_empty());
    assert_eq!(subscribe.qcmp_port, None);
    assert_eq!(subscribe.icao, None);
}

/// Validates that v1 requests and responses can be serialized and deserialized
#[test]
fn version1_proto() {
    use proto::v1;

    let icao = IcaoCode::new_testing([b'H'; 4]);

    {
        let req = v1::Request::Mutate(v1::MutateRequest {
            qcmp_port: 8998,
            icao,
            capabilities: Capabilities::INGRESS,
        })
        .write()
        .expect("failed to write mutate request");

        let vb = VersionedBuf::try_parse(&req[2..])
            .expect("failed to deserialize versioned buf for mutate request");
        assert_eq!(vb.version, 1);

        let req = vb
            .deserialize_request()
            .expect("failed to deserialize mutate request");
        let proto::Request::V1(v1::Request::Mutate(req)) = req else {
            panic!("expected a mutate request");
        };

        assert_eq!(req.qcmp_port, 8998);
        assert_eq!(req.icao, icao);
        assert_eq!(req.capabilities, Capabilities::INGRESS);

        let res = v1::Response::Ok(v1::OkResponse::Mutate(v1::MutateResponse))
            .write()
            .expect("failed to serialize mutate response");
        let vb = VersionedBuf::try_parse(&res[2..])
            .expect("failed to deserialize versioned buf for mutate response");
        assert_eq!(vb.version, 1);

        let res = vb
            .deserialize_response()
            .expect("failed to deserialize mutate response");
        assert!(matches!(
            res,
            proto::Response::V1(v1::Response::Ok(v1::OkResponse::Mutate(v1::MutateResponse)))
        ));
    }
}
