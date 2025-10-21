#![cfg(target_pointer_width = "64")]

use divan::Bencher;
use quilkin::{net::cluster::ClusterMap, xds::Resource};

mod shared;

use shared::TokenKind;

#[divan::bench_group(sample_count = 10)]
mod serde {
    use super::*;
    use prost_types::Any;
    use quilkin::net::cluster::proto::Cluster;
    use shared::gen_cluster_map;

    fn serialize_to_protobuf(cm: &ClusterMap) -> Vec<Any> {
        let mut resources = Vec::new();

        for resource in cm.iter_with(|locality, endpoint_set| {
            Resource::Cluster(Cluster {
                locality: locality.clone().map(From::from),
                endpoints: endpoint_set
                    .endpoints
                    .iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()
                    .unwrap(),
            })
        }) {
            resources.push(resource.try_encode().unwrap());
        }

        resources
    }

    fn deserialize_from_protobuf(pv: Vec<Any>) -> ClusterMap {
        let cm = ClusterMap::default();

        for any in pv {
            let c = quilkin::xds::Resource::try_decode(any).unwrap();

            let quilkin::xds::Resource::Cluster(cluster) = c else {
                unreachable!()
            };
            cm.insert(
                None,
                cluster.locality.map(From::from),
                cluster
                    .endpoints
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()
                    .unwrap(),
            );
        }

        cm
    }

    fn serialize_to_json(cm: &ClusterMap) -> serde_json::Value {
        serde_json::to_value(cm).unwrap()
    }

    fn deserialize_from_json(json: serde_json::Value) -> ClusterMap {
        serde_json::from_value(json.clone()).unwrap()
    }

    #[divan::bench(consts = SEEDS)]
    fn serialize_proto<const S: u64>(b: Bencher<'_, '_>) {
        let gc = gen_cluster_map::<S>(TokenKind::None);
        b.counter(gc.total_endpoints)
            .bench(|| divan::black_box(serialize_to_protobuf(&gc.cm)));
    }

    #[divan::bench(consts = SEEDS)]
    fn serialize_json<const S: u64>(b: Bencher<'_, '_>) {
        let gc = gen_cluster_map::<S>(TokenKind::None);
        b.counter(gc.total_endpoints)
            .bench(|| divan::black_box(serialize_to_json(&gc.cm)));
    }

    #[divan::bench(consts = SEEDS)]
    fn deserialize_json<const S: u64>(b: Bencher<'_, '_>) {
        let gc = gen_cluster_map::<S>(TokenKind::None);
        let json = serialize_to_json(&gc.cm);

        b.with_inputs(|| json.clone())
            .counter(gc.total_endpoints)
            .bench_values(|json| divan::black_box(deserialize_from_json(json)));
    }

    #[divan::bench(consts = SEEDS)]
    fn deserialize_proto<const S: u64>(b: Bencher<'_, '_>) {
        let gc = gen_cluster_map::<S>(TokenKind::None);
        let pv = serialize_to_protobuf(&gc.cm);

        b.with_inputs(|| pv.clone())
            .counter(gc.total_endpoints)
            .bench_values(|pv| divan::black_box(deserialize_from_protobuf(pv)));
    }
}

const SEEDS: &[u64] = &[100, 200, 300, 400, 500];

#[divan::bench_group(sample_count = 10)]
mod ops {
    use super::*;
    use shared::{GenCluster, gen_cluster_map};

    fn compute_hash<const S: u64>(gc: &GenCluster) -> usize {
        let total_endpoints = gc
            .cm
            .iter_with(|_locality, endpoint_set| endpoint_set.len())
            .iter()
            .sum();

        assert_eq!(total_endpoints, gc.total_endpoints);
        total_endpoints
    }

    // #[allow(clippy::eq_op)]
    // fn is_equal(gc: &GenCluster) -> usize {
    //     assert_eq!(gc.cm, gc.cm);
    //     gc.total_endpoints
    // }

    #[divan::bench(consts = SEEDS)]
    fn iterate<const S: u64>(b: Bencher<'_, '_>) {
        let cm = gen_cluster_map::<S>(TokenKind::None);

        b.counter(cm.total_endpoints)
            .bench_local(|| divan::black_box(compute_hash::<S>(&cm)));

        drop(cm);
    }

    #[divan::bench(consts = SEEDS)]
    fn iterate_par<const S: u64>(b: Bencher<'_, '_>) {
        let cm = gen_cluster_map::<S>(TokenKind::None);

        b.counter(cm.total_endpoints)
            .bench(|| divan::black_box(compute_hash::<S>(&cm)));
    }

    // #[divan::bench(consts = SEEDS)]
    // fn partial_eq<const S: u64>(b: Bencher) {
    //     let cm = gen_cluster_map::<S>();

    //     b.counter(cm.total_endpoints)
    //         .bench(|| divan::black_box(is_equal(&cm)))
    // }
}

fn main() {
    divan::main();
}
