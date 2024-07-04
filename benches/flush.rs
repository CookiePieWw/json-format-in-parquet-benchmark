use criterion::{criterion_group, criterion_main, Criterion};
use json_format_in_parquet_benchmark::codec::read as codec_read;
use json_format_in_parquet_benchmark::format::formats::Format;
use json_format_in_parquet_benchmark::consts::AVAILABE_FORMATS;

fn criterion_benchmark(c: &mut Criterion) {
    let json_strs = codec_read("logs.txt").unwrap();

    for available_format in AVAILABE_FORMATS.iter() {
        let mut format = Format::get_format(available_format);
        format.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
        let path = format!("logs_{}.parquet", available_format);

        c.bench_function(&(available_format.to_string() + " flush"), |b| {
            b.iter(|| {
                format.flush(&path);
            })
        });

        c.bench_function(&(available_format.to_string() + " load"), |b| {
            b.iter(|| {
                format.load(&path);
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
