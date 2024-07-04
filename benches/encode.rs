use criterion::{criterion_group, criterion_main, Criterion};
use json_format_in_parquet_benchmark::codec::{read as codec_read, JsonCodec};
use json_format_in_parquet_benchmark::formats::jsonb::JsonbVector;

fn criterion_benchmark(c: &mut Criterion) {
    let json_strs = codec_read("logs.txt").unwrap();

    c.bench_function("jsonb encode", |b| {
        b.iter(|| JsonbVector::encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>()))
    });

    let jsonb = JsonbVector::encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
    c.bench_function("jsonb decode", |b| b.iter(|| jsonb.decode()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
