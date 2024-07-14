use criterion::{criterion_group, criterion_main, Criterion};
use json_format_in_parquet_benchmark::codec::read as codec_read;
use json_format_in_parquet_benchmark::consts::AVAILABE_FORMATS;
use json_format_in_parquet_benchmark::format::formats::Format;

fn criterion_benchmark(c: &mut Criterion) {
    let json_strs = codec_read("logs.txt").unwrap();

    for available_format in AVAILABE_FORMATS.iter() {
        let mut format = Format::get_format(available_format);

        c.bench_function(&(available_format.to_string() + " encode"), |b| {
            b.iter(|| {
                format.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
            })
        });

        format.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
        c.bench_function(&(available_format.to_string() + " decode"), |b| {
            b.iter(|| {
                format.decode();
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
