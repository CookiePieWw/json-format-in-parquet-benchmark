use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use std::{fs::File, sync::Arc};

use crate::codec::JsonCodec;
use crate::consts::PARQUET_DIR;

use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

#[derive(PartialEq, Eq, Debug, Default)]
pub struct PlainJsonVector {
    data: Vec<String>,
}

impl JsonCodec for PlainJsonVector {
    fn encode(&mut self, json_str: &[&[u8]]) {
        for json in json_str {
            self.data.push(String::from_utf8(json.to_vec()).unwrap());
        }
    }

    fn decode(&self) -> Vec<String> {
        self.data.clone()
    }

    fn flush(&self, path: &str) {
        let schema = Arc::new(Schema::new(vec![Field::new("", DataType::Utf8, false)]));
        let array = StringArray::from(self.data.clone());
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
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        self.data = array.iter().map(|v| v.unwrap().to_string()).collect();
    }

    fn name() -> String {
        "plain json str".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_json_vector() {
        let json_strs = vec![
            r#"{"a":1,"b":"foo"}"#.as_bytes(),
            r#"{"a":2,"b":"bar"}"#.as_bytes(),
            r#"{"a":3,"b":"baz"}"#.as_bytes(),
        ];
        let mut plain_json_vec = PlainJsonVector::default();
        plain_json_vec.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
        let json_strs_decoded = plain_json_vec.decode();
        assert_eq!(
            json_strs,
            json_strs_decoded
                .iter()
                .map(|v| v.as_bytes())
                .collect::<Vec<&[u8]>>()
        );

        plain_json_vec.flush("test_plain_json_vector.parquet");
        let mut loaded_plain_json_vec = PlainJsonVector::default();
        loaded_plain_json_vec.load("test_plain_json_vector.parquet");
        assert_eq!(loaded_plain_json_vec, plain_json_vec);

        std::fs::remove_file(format!("{}/test_plain_json_vector.parquet", PARQUET_DIR)).unwrap();
    }
}
