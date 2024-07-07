use std::{fs::File, sync::Arc};

use arrow::{
    array::{
        ArrayRef, Float64Builder, ListBuilder, RecordBatch, StringBuilder, StructArray,
        UInt8Builder,
    },
    datatypes::{DataType, Field, Schema},
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde_json::Value;

use crate::{codec::JsonCodec, consts::PARQUET_DIR, serde_ende};

#[derive(Debug, PartialEq)]
enum Node {
    Null,
    StartArray,
    EndArray,
    StartObject,
    EndObject,
    Key,
    String,
    Number,
    True,
    False,
}

impl Node {
    fn to_u8(&self) -> u8 {
        match self {
            Node::Null => 0,
            Node::StartArray => 1,
            Node::EndArray => 2,
            Node::StartObject => 3,
            Node::EndObject => 4,
            Node::Key => 5,
            Node::String => 6,
            Node::Number => 7,
            Node::True => 8,
            Node::False => 9,
        }
    }
}

#[derive(Debug, PartialEq, Default)]
struct Jsonc {
    pub nodes: Vec<Node>,
    pub strings: Vec<String>,
    pub numbers: Vec<f64>,
}

impl Jsonc {
    fn append(&mut self, other: &mut Jsonc) {
        self.nodes.append(&mut other.nodes);
        self.strings.append(&mut other.strings);
        self.numbers.append(&mut other.numbers);
    }

    fn node_opt_list(&self) -> Vec<Option<u8>> {
        let mut node_list = Vec::new();
        for node in self.nodes.iter() {
            node_list.push(Some(node.to_u8()));
        }
        node_list
    }

    fn string_opt_list(&self) -> Vec<Option<String>> {
        self.strings.clone().into_iter().map(Some).collect()
    }

    fn number_opt_list(&self) -> Vec<Option<f64>> {
        self.numbers.clone().into_iter().map(Some).collect()
    }

    fn fields() -> Vec<Field> {
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

    fn as_arrow_type() -> DataType {
        DataType::Struct(Jsonc::fields().into())
    }
}

impl From<Value> for Jsonc {
    fn from(value: Value) -> Self {
        let mut jsonc = Jsonc::default();
        match value {
            Value::Null => {
                jsonc.nodes.push(Node::Null);
            }
            Value::Bool(true) => {
                jsonc.nodes.push(Node::True);
            }
            Value::Bool(false) => {
                jsonc.nodes.push(Node::False);
            }
            Value::Number(n) => {
                jsonc.nodes.push(Node::Number);
                jsonc.numbers.push(n.as_f64().unwrap());
            }
            Value::String(s) => {
                jsonc.nodes.push(Node::String);
                jsonc.strings.push(s);
            }
            Value::Array(arr) => {
                jsonc.nodes.push(Node::StartArray);
                for v in arr {
                    let mut jsonc_v = Jsonc::from(v);
                    jsonc.append(&mut jsonc_v);
                }
                jsonc.nodes.push(Node::EndArray);
            }
            Value::Object(obj) => {
                jsonc.nodes.push(Node::StartObject);
                for (k, v) in obj {
                    jsonc.nodes.push(Node::Key);
                    jsonc.strings.push(k);
                    let mut jsonc_v = Jsonc::from(v);
                    jsonc.append(&mut jsonc_v);
                }
                jsonc.nodes.push(Node::EndObject);
            }
        }

        jsonc
    }
}

#[derive(PartialEq, Eq, Debug, Default)]
pub struct JsoncVector {
    data: Vec<Value>,
}

impl JsoncVector {
    fn to_jsonc_list(&self) -> Vec<Jsonc> {
        self.data.iter().map(|v| Jsonc::from(v.clone())).collect()
    }
}

impl JsonCodec for JsoncVector {
    serde_ende!();

    fn flush(&self, path: &str) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "",
            Jsonc::as_arrow_type(),
            false,
        )]));
        let jsonc_list = self.to_jsonc_list();
        let mut node_builder = ListBuilder::new(UInt8Builder::new());
        jsonc_list.iter().for_each(|jsonc| {
            node_builder.append_value(jsonc.node_opt_list());
        });
        let node_array = node_builder.finish();

        let mut string_builder = ListBuilder::new(StringBuilder::new());
        jsonc_list.iter().for_each(|jsonc| {
            string_builder.append_value(jsonc.string_opt_list());
        });
        let string_array = string_builder.finish();

        let mut number_builder = ListBuilder::new(Float64Builder::new());
        jsonc_list.iter().for_each(|jsonc| {
            number_builder.append_value(jsonc.number_opt_list());
        });
        let number_array = number_builder.finish();

        let array = StructArray::new(
            Jsonc::fields().into(),
            vec![
                Arc::new(node_array) as ArrayRef,
                Arc::new(string_array) as ArrayRef,
                Arc::new(number_array) as ArrayRef,
            ],
            None,
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let path = format!("{}/{}", PARQUET_DIR, path);
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn load(&mut self, path: &str) {
        todo!()
    }

    fn name() -> String {
        "jsonc".to_string()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn test_jsonc_from_serde_json() {
        let value: Value = serde_json::from_str(r#"{"a":1,"b":[2,3],"c":{"d":4}}"#).unwrap();
        let jsonc = Jsonc::from(value);

        assert_eq!(
            jsonc,
            Jsonc {
                nodes: vec![
                    Node::StartObject,
                    Node::Key,
                    Node::Number,
                    Node::Key,
                    Node::StartArray,
                    Node::Number,
                    Node::Number,
                    Node::EndArray,
                    Node::Key,
                    Node::StartObject,
                    Node::Key,
                    Node::Number,
                    Node::EndObject,
                    Node::EndObject
                ],
                strings: vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "d".to_string()
                ],
                numbers: vec![1.0, 2.0, 3.0, 4.0]
            }
        );
    }

    #[test]
    fn test_jsonc_flush() {
        let jsonc_vec = JsoncVector {
            data: vec![
                serde_json::from_str(r#"{"a":1,"b":[2,3],"c":{"d":4}}"#).unwrap(),
                serde_json::from_str(r#"{"e":5,"f":[6,7],"g":{"h":8}}"#).unwrap(),
            ],
        };
        jsonc_vec.flush("test_jsonc_vector.parquet");
        std::fs::remove_file(format!("{}/test_jsonc_vector.parquet", PARQUET_DIR)).unwrap();
    }
}
