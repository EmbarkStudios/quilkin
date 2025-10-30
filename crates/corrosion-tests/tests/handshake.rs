use corrosion::persistent::*;
use quilkin_types::IcaoCode;

#[test]
fn version1_handshake() {
    let icao = IcaoCode::new_testing([b'H'; 4]);

    let chs = ClientHandshakeRequestV1 {
        qcmp_port: 8998,
        icao,
    }
    .write();

    let (version, chs) = ClientHandshake::read(1, &chs).unwrap();

    assert_eq!(version, 1);
    assert_eq!(chs.client_details(), (8998, icao));

    let shs = ServerHandshakeResponseV1 { accept: true }.write();

    let shs = ServerHandshake::read(1, &shs).unwrap();
    let ServerHandshake::V1(v1) = shs;
    assert!(v1.accept);
}
