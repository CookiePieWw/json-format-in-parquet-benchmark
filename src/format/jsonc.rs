use std::{fs::File, sync::Arc};

use arrow::{
    array::{
        ArrayRef, Float64Array, Float64Builder, ListArray, ListBuilder, RecordBatch, StringArray,
        StringBuilder, StructArray, UInt8Array, UInt8Builder,
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

fn jsonc_fields() -> Vec<Field> {
    // Builders use "item" as default field name
    vec![
        Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            false,
        ),
        Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ),
    ]
}

fn jsonc_as_arrow_type() -> DataType {
    DataType::Struct(jsonc_fields().into())
}

#[derive(Debug, Default)]
pub struct JsoncVector {
    data: Vec<Jsonc>,
}

impl JsonCodec for JsoncVector {
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "",
            jsonc_as_arrow_type(),
            false,
        )]));
        let jsonc_list = self.data.clone();
        let mut node_builder = ListBuilder::new(UInt8Builder::with_capacity(jsonc_list.len()));
        jsonc_list.iter().for_each(|jsonc| {
            node_builder.append_value(jsonc.node_opt_list());
        });
        let node_array = node_builder.finish();

        let mut string_builder = ListBuilder::new(StringBuilder::new());
        jsonc_list.iter().for_each(|jsonc| {
            string_builder.append_value(jsonc.string_opt_list());
        });
        let string_array = string_builder.finish();

        let mut number_builder = ListBuilder::new(Float64Builder::with_capacity(jsonc_list.len()));
        jsonc_list.iter().for_each(|jsonc| {
            number_builder.append_value(jsonc.number_opt_list());
        });
        let number_array = number_builder.finish();

        let array = StructArray::new(
            jsonc_fields().into(),
            vec![
                Arc::new(node_array) as ArrayRef,
                Arc::new(string_array) as ArrayRef,
                Arc::new(number_array) as ArrayRef,
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
        let path = format!("{}/{}", PARQUET_DIR, path);
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let node_array = array
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let string_array = array
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let number_array = array
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        for ((nodes_opt, strings_opt), numbers_opt) in node_array
            .iter()
            .zip(string_array.iter())
            .zip(number_array.iter())
        {
            let nodes = nodes_opt.unwrap();
            let nodes = nodes.as_any().downcast_ref::<UInt8Array>().unwrap();
            let strings = strings_opt.unwrap();
            let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
            let numbers = numbers_opt.unwrap();
            let numbers = numbers.as_any().downcast_ref::<Float64Array>().unwrap();
            let jsonc = Jsonc::new_with_values(
                nodes.iter().map(|n| Node::from(&n.unwrap())).collect(),
                strings.iter().map(|s| s.unwrap().to_string()).collect(),
                numbers.iter().map(|n| n.unwrap()).collect(),
            );
            self.data.push(jsonc);
        }
    }

    fn name() -> String {
        "jsonc".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_jsonc_vector() {
        let json_strs = vec![
            r#"{"a":1.0,"b":[2.0,3.0],"c":{"d":4.0}}"#.as_bytes(),
            r#"{"e":5.0,"f":[6.0,7.0],"g":{"h":8.0}}"#.as_bytes(),
            r#"{"i":9.0,"j":[10.0,11.0],"k":{"l":12.0}}"#.as_bytes(),
        ];
        let mut jsonc_vec = JsoncVector::default();
        jsonc_vec.encode(&json_strs.iter().map(|v| &v[..]).collect::<Vec<&[u8]>>());
        jsonc_vec.flush("test_jsonc_vector.parquet");
        let mut loaded_jsonc_vec = JsoncVector::default();
        loaded_jsonc_vec.load("test_jsonc_vector.parquet");
        assert_eq!(loaded_jsonc_vec.decode(), jsonc_vec.decode());

        std::fs::remove_file(format!("{}/test_jsonc_vector.parquet", PARQUET_DIR)).unwrap();
    }
}
