/// [Variant in Doris](https://doris.apache.org/blog/variant-in-apache-doris-2.1/#design--implementation-of-variant)
/// Currently we just use pre-defined schema to test for simplicity.
use std::{fs::File, sync::Arc};

use arrow::{
    array::{
        ArrayRef, Float32Builder, Float64Array, Float64Builder, ListArray, ListBuilder, RecordBatch, StringArray, StringBuilder, StructArray, StructBuilder, UInt8Array, UInt8Builder
    },
    datatypes::{DataType, Field, Schema},
};
use jsonc::value::{Jsonc, Node};
use jsonc::parser::parse_value;
use jsonc::decoder::decode;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::{codec::JsonCodec, consts::PARQUET_DIR};

fn const_schema() -> Vec<Field> {
    vec![
        Field::new(
            "name",
            DataType::Utf8,
            false,
        ),
        Field::new(
            "timestamp",
            DataType::Utf8,
            false,
        ),
        Field::new(
            "attributes",
            DataType::Struct(
                vec![
                    Field::new(
                        "event_attributes",
                        DataType::Float32,
                        false,
                    )
                ].into()
            ),
            false,
        ),
    ]
}

fn schema_as_arrow_type() -> DataType {
    DataType::Struct(const_schema().into())
}

#[derive(Debug, Default)]
pub struct VariantVector {
    data: Vec<Jsonc>,
}

impl JsonCodec for VariantVector {
    fn encode(&mut self, json_str: &[&[u8]]) {
        self.data.clear();
        for json_str in json_str {
            let jsonc = parse_value(json_str);
            self.data.push(jsonc);
        }
    }

    fn decode(&self) -> Vec<String> {
        self.data.iter().map(|jsonc| decode(jsonc)).collect()
    }

    fn flush(&self, path: &str) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "",
                schema_as_arrow_type(),
                false,
            )
        ]));
        let jsonc_list = self.data.clone();
        let mut name_builder = StringBuilder::new();
        let mut timestamp_builder = StringBuilder::new();
        let mut attributes_builder = StructBuilder::from_fields(
            vec![
                Field::new(
                    "event_attributes",
                    DataType::Float32,
                    false,
                )
            ],
            0
        );
        let event_attributes_builder = attributes_builder.field_builder::<Float32Builder>(0).unwrap();
        for jsonc in &jsonc_list {
            name_builder.append_value(jsonc.get(&["\"name\""]).unwrap().to_string());
            timestamp_builder.append_value(jsonc.get(&["\"timestamp\""]).unwrap().to_string());
            let event_attributes = jsonc.get(&["\"attributes\"", "\"event_attributes\""]).unwrap().parse::<f32>().unwrap();
            event_attributes_builder.append_value(event_attributes);
        }

        for _ in jsonc_list {
            attributes_builder.append(true);
        }

        let name_array = name_builder.finish();
        let timestamp_array = timestamp_builder.finish();
        let attributes_array = attributes_builder.finish();

        let array = StructArray::new(
            const_schema().into(),
            vec![
                Arc::new(name_array) as ArrayRef,
                Arc::new(timestamp_array) as ArrayRef,
                Arc::new(attributes_array) as ArrayRef,
            ],
            None,
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let path = format!("{}/{}", PARQUET_DIR, path);
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn load(&mut self, path: &str) {
        unimplemented!()
    }

    fn name() -> String {
        "variant".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_jsonc_vector() {
        let json_strs = vec![
            r#"{"name":"nKG7e","timestamp":"2024-07-25T04:33:11.370048Z","attributes":{"event_attributes":415.32588395798473}}
"#.as_bytes(),
        ];
        let mut jsonc_vec = VariantVector::default();
        jsonc_vec.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
        jsonc_vec.flush("test_jsonc_vector.parquet");
        let mut loaded_jsonc_vec = VariantVector::default();
        // loaded_jsonc_vec.load("test_jsonc_vector.parquet");
        // assert_eq!(loaded_jsonc_vec.decode(), jsonc_vec.decode());

        std::fs::remove_file(format!("{}/test_jsonc_vector.parquet", PARQUET_DIR)).unwrap();
    }
}
