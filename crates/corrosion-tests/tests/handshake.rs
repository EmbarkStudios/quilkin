use corrosion::persistent::proto::{self, VersionedBuf, VersionedRequest, VersionedResponse};
use quilkin_types::IcaoCode;

/// Validates that v1 requests and responses can be serialized and deserialized
#[test]
fn version1_proto() {
    use proto::v1;

    let icao = IcaoCode::new_testing([b'H'; 4]);

    {
        let req = v1::Request::Mutate(v1::MutateRequest {
            qcmp_port: 8998,
            icao,
        })
        .write()
        .expect("failed to write mutate request");

        let vb = VersionedBuf::try_parse(&req[2..])
            .expect("failed to deserialize versioned buf for mutate request");
        assert_eq!(vb.version, 1);

        let req = vb
            .deserialize_request()
            .expect("failed to deserialize mutate request");
        #[allow(unused_variables)]
        {
            assert!(matches!(
                req,
                proto::Request::V1(v1::Request::Mutate(v1::MutateRequest {
                    qcmp_port: 8998,
                    icao,
                }))
            ));
        }

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
