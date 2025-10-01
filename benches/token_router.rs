use divan::Bencher;
use quilkin::filters::token_router::TokenRouter;
use rand::SeedableRng;

mod shared;

#[divan::bench(args = ["single:duplicates", "single:unique", "multi:2..128:duplicates", "multi:2..128:unique"])]
fn token_router(b: Bencher<'_, '_>, token_kind: &str) {
    let filter = TokenRouter::default();
    let gc = shared::gen_cluster_map::<42>(token_kind.parse().unwrap());

    let mut tokens = Vec::new();

    let cm = std::sync::Arc::new(gc.cm);

    // Calculate the amount of bytes for all the tokens
    for eps in cm.iter() {
        for ep in &eps.value().endpoints {
            for tok in ep.metadata.known.tokens.iter() {
                tokens.push(tok.clone());
            }
        }
    }

    let total_token_size: usize = tokens.iter().map(|t| t.len()).sum();
    let pool = std::sync::Arc::new(quilkin::collections::BufferPool::new(1, 1));

    let mut rand = rand::rngs::SmallRng::seed_from_u64(42);

    b.with_inputs(|| {
        use rand::seq::IndexedRandom as _;
        let tok = tokens.choose(&mut rand).unwrap();

        let mut metadata = quilkin::net::endpoint::DynamicMetadata::default();
        metadata.insert(
            quilkin::net::endpoint::metadata::Key::from_static(
                quilkin::filters::capture::CAPTURED_BYTES,
            ),
            quilkin::net::endpoint::metadata::Value::Bytes((*tok).clone().into()),
        );

        (
            cm.clone(),
            pool.clone().alloc(),
            Vec::with_capacity(1),
            metadata,
        )
    })
    .counter(divan::counter::BytesCount::new(total_token_size))
    .bench_local_values(|(cm, buffer, mut dest, metadata)| {
        let mut rc = quilkin::filters::ReadContext {
            endpoints: &cm,
            destinations: &mut dest,
            source: quilkin::net::EndpointAddress::LOCALHOST,
            contents: buffer,
            metadata,
        };

        use quilkin::filters::Filter;
        let _unused = divan::black_box(filter.read(&mut rc));
    });
}

fn main() {
    divan::main();
}
