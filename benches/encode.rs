use criterion::{criterion_group, criterion_main, Criterion};
use json_format_in_parquet_benchmark::codec::read as codec_read;
use json_format_in_parquet_benchmark::consts::{AVAILABLE_FORMATS, AVAILABLE_JSONS};
use json_format_in_parquet_benchmark::format::formats::Format;

fn criterion_benchmark(c: &mut Criterion) {
    for json in AVAILABLE_JSONS.iter() {
        let json_strs = codec_read(json).expect(&format!("Failed to read json file {}", json));

        for available_format in AVAILABLE_FORMATS.iter() {
            let mut format = Format::get_format(available_format);

            c.bench_function(&(json.to_string() + " " + available_format + " encode"), |b| {
                b.iter(|| {
                    format.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
                })
            });

            format.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
            c.bench_function(&(json.to_string() + " " + available_format + " decode"), |b| {
                b.iter(|| {
                    format.decode();
                })
            });
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
