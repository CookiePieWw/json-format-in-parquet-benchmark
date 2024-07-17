use criterion::{criterion_group, criterion_main, Criterion};
use jsonc::value::Jsonc;

use json_format_in_parquet_benchmark::codec::read as codec_read;
use parquet::data_type::AsBytes;

fn jsonb_get(data: &[u8], paths: &[&str], expected: &str) {
    let paths = paths
        .iter()
        .map(|p| jsonb::jsonpath::Path::DotField(std::borrow::Cow::Borrowed(p)))
        .collect::<Vec<_>>();
    let json_path = jsonb::jsonpath::JsonPath { paths };

    let mut result_data = vec![];
    let mut result_offsets = vec![];

    jsonb::get_by_path(data, json_path, &mut result_data, &mut result_offsets);

    let s = jsonb::as_str(&result_data).unwrap();
    assert_eq!(s, expected);
}

fn jsonc_get(jsonc: &Jsonc, paths: &[&str], expected: &str) {
    let jsonc_paths: Vec<_> = paths.iter().map(|p| format!("\"{}\"", p)).collect();
    let jsonc_paths_slice = jsonc_paths.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let s = jsonc.get(&jsonc_paths_slice).unwrap();
    assert_eq!(s, expected);
}

fn serde_json_get(data: &[u8], paths: &Vec<&str>, expected: &str) {
    let mut v: serde_json::Value = serde_json::from_slice(data).unwrap();
    for path in paths {
        v = v.get(path).unwrap().clone();
    }
    let s = v.as_str().unwrap();
    assert_eq!(s, expected);
}

struct TestSuite<'a> {
    file: &'a str,
    paths: Vec<&'a str>,
    expected: &'a str,
}

fn criterion_benchmark(c: &mut Criterion) {
    let test_suites = vec![
        TestSuite {
            file: "canada",
            paths: vec!["type"],
            expected: "FeatureCollection",
        },
        TestSuite {
            file: "citm_catalog",
            paths: vec!["areaNames", "205705994"],
            expected: "1er balcon central",
        },
        TestSuite {
            file: "citm_catalog",
            paths: vec!["topicNames", "324846100"],
            expected: "Formations musicales",
        },
        TestSuite {
            file: "twitter",
            paths: vec!["search_metadata", "max_id_str"],
            expected: "505874924095815681",
        },
    ];

    for test_suite in test_suites {
        let json_strs = codec_read(&format!("{}.json", test_suite.file)).unwrap();
        let json_str = &json_strs[0];
        let jsonb_val = jsonb::parse_value(json_str.as_bytes()).unwrap();
        let jsonb_data = jsonb_val.to_vec();
        let paths = &test_suite.paths;
        let expected = test_suite.expected;

        c.bench_function(
            &format!(
                "jsonb get {}->{}",
                test_suite.file,
                test_suite.paths.join("->")
            ),
            |b| {
                b.iter(|| {
                    jsonb_get(&jsonb_data, &paths, expected);
                })
            },
        );

        let jsonc = jsonc::parser::parse_value(json_str.as_bytes());
        c.bench_function(
            &format!(
                "jsonc get {}->{}",
                test_suite.file,
                test_suite.paths.join("->")
            ),
            |b| {
                b.iter(|| {
                    jsonc_get(&jsonc, &paths, &format!("\"{}\"", expected));
                })
            },
        );

        c.bench_function(
            &format!(
                "serde get {}->{}",
                test_suite.file,
                test_suite.paths.join("->")
            ),
            |b| {
                b.iter(|| {
                    serde_json_get(&json_str, &paths, expected);
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
