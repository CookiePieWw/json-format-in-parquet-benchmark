use criterion::{criterion_group, criterion_main, Criterion};
use json_format_in_parquet_benchmark::codec::{read as codec_read, JsonCodec};
use json_format_in_parquet_benchmark::formats::jsonb::JsonbVector;

fn criterion_benchmark(c: &mut Criterion) {
    let json_strs = codec_read("logs.txt").unwrap();
    let jsonb = JsonbVector::encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
    let path = format!("logs_{}.parquet", JsonbVector::name());

    c.bench_function(&(JsonbVector::name() + "flush"), |b| {
        b.iter(|| {
            jsonb.flush(&path);
        })
    });

    c.bench_function(&(JsonbVector::name() + "load"), |b| {
        b.iter(|| {
            JsonbVector::load(&path);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
