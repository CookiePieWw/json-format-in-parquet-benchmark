use std::{fs::File, sync::Arc};

use crate::codec::JsonCodec;
use crate::consts::PARQUET_DIR;
use arrow::{
    array::{ArrayRef, BinaryArray, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use jsonb::{parse_value, to_string};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

#[derive(PartialEq, Eq, Debug, Default)]
pub struct JsonbVector {
    data: Vec<Vec<u8>>,
}

impl From<&BinaryArray> for JsonbVector {
    fn from(array: &BinaryArray) -> Self {
        let data = array.iter().map(|v| v.unwrap().to_vec()).collect();
        Self { data }
    }
}

impl JsonCodec for JsonbVector {
    fn encode(&mut self, json_strs: &[&[u8]]) {
        self.data.clear();
        for json_str in json_strs {
            let value = parse_value(json_str).unwrap();
            self.data.push(value.to_vec());
        }
    }

    fn decode(&self) -> Vec<String> {
        let mut result = Vec::new();
        for value in &self.data {
            result.push(to_string(value));
        }
        result
    }

    fn flush(&self, path: &str) {
        let schema = Arc::new(Schema::new(vec![Field::new("", DataType::Binary, false)]));
        let array = BinaryArray::from(self.data.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).unwrap();

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
        let path = format!("{}/{}", PARQUET_DIR, path);
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        self.data = array.iter().map(|v| v.unwrap().to_vec()).collect();
    }

    fn name() -> String {
        "jsonb".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jsonb_vector() {
        let json_strs = vec![
            r#"{"a":1,"b":"foo"}"#.as_bytes(),
            r#"{"a":2,"b":"bar"}"#.as_bytes(),
            r#"{"a":3,"b":"baz"}"#.as_bytes(),
        ];
        let mut jsonb_vec = JsonbVector::default();
        jsonb_vec.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
        let json_strs_decoded = jsonb_vec.decode();
        assert_eq!(
            json_strs,
            json_strs_decoded
                .iter()
                .map(|v| v.as_bytes())
                .collect::<Vec<&[u8]>>()
        );

        jsonb_vec.flush("test_jsonb_vector.parquet");
        let mut loaded_jsonb_vec = JsonbVector::default();
        loaded_jsonb_vec.load("test_jsonb_vector.parquet");
        assert_eq!(loaded_jsonb_vec, jsonb_vec);

        std::fs::remove_file(format!("{}/test_jsonb_vector.parquet", PARQUET_DIR)).unwrap();
    }
}
