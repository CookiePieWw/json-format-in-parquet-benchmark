use std::{fs::File, sync::Arc};

use arrow::{
    array::{
        ArrayRef, Float64Array, Float64Builder, ListArray, ListBuilder, RecordBatch, StringArray,
        StringBuilder, StructArray, UInt8Array, UInt8Builder,
    },
    datatypes::{DataType, Field, Schema},
};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use serde_json::{Number, Value};

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

impl From<&Node> for u8 {
    fn from(node: &Node) -> u8 {
        match node {
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

impl From<&u8> for Node {
    fn from(n: &u8) -> Node {
        match n {
            0 => Node::Null,
            1 => Node::StartArray,
            2 => Node::EndArray,
            3 => Node::StartObject,
            4 => Node::EndObject,
            5 => Node::Key,
            6 => Node::String,
            7 => Node::Number,
            8 => Node::True,
            9 => Node::False,
            _ => panic!("Invalid node value"),
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
    fn new(nodes: Vec<Node>, strings: Vec<String>, numbers: Vec<f64>) -> Self {
        Jsonc {
            nodes,
            strings,
            numbers,
        }
    }

    fn append(&mut self, other: &mut Jsonc) {
        self.nodes.append(&mut other.nodes);
        self.strings.append(&mut other.strings);
        self.numbers.append(&mut other.numbers);
    }

    fn node_opt_list(&self) -> Vec<Option<u8>> {
        let mut node_list = Vec::new();
        for node in self.nodes.iter() {
            node_list.push(Some(node.into()));
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

/// Returns value, the number of nodes consumed, strings consumed, and numbers consumed
fn jsonc_list_to_serde_value(
    nodes: &[Node],
    strings: &[String],
    numbers: &[f64],
) -> (Value, usize, usize, usize) {
    match nodes[0] {
        Node::Null => (Value::Null, 1, 0, 0),
        Node::True => (Value::Bool(true), 1, 0, 0),
        Node::False => (Value::Bool(false), 1, 0, 0),
        Node::Number => (
            Value::Number(Number::from_f64(numbers[0]).unwrap()),
            1,
            0,
            1,
        ),
        Node::String => (Value::String(strings[0].clone()), 1, 1, 0),
        Node::StartArray => {
            let mut arr = Vec::new();
            let mut node_idx = 1;
            let mut string_idx = 0;
            let mut number_idx = 0;
            while nodes[node_idx] != Node::EndArray {
                let (value, i, j, k) = jsonc_list_to_serde_value(
                    &nodes[node_idx..],
                    &strings[string_idx..],
                    &numbers[number_idx..],
                );
                node_idx += i;
                string_idx += j;
                number_idx += k;
                arr.push(value);
            }
            (Value::Array(arr), node_idx + 1, string_idx, number_idx)
        }
        Node::StartObject => {
            let mut obj = serde_json::Map::new();
            let mut node_idx = 1;
            let mut string_idx = 0;
            let mut number_idx = 0;

            while nodes[node_idx] != Node::EndObject {
                let key = strings[string_idx].clone();
                string_idx += 1;
                node_idx += 1;
                let (value, i, j, k) = jsonc_list_to_serde_value(
                    &nodes[node_idx..],
                    &strings[string_idx..],
                    &numbers[number_idx..],
                );
                node_idx += i;
                string_idx += j;
                number_idx += k;
                obj.insert(key, value);
            };

            (Value::Object(obj), node_idx + 1, string_idx, number_idx)
        }
        _ => panic!("Invalid node value"),
    }
}

impl From<Jsonc> for Value {
    fn from(jsonc: Jsonc) -> Self {
        let (value, _, _, _) =
            jsonc_list_to_serde_value(&jsonc.nodes, &jsonc.strings, &jsonc.numbers);
        value
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
            let jsonc = Jsonc::new(
                nodes.iter().map(|n| Node::from(&n.unwrap())).collect(),
                strings.iter().map(|s| s.unwrap().to_string()).collect(),
                numbers.iter().map(|n| n.unwrap()).collect(),
            );
            self.data.push(jsonc.into());
        }
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
    fn test_jsonc_to_serde_json() {
        let value: Value = serde_json::from_str(r#"{"a":1.0,"b":[2.0,3.0],"c":{"d":4.0}}"#).unwrap();
        let jsonc = Jsonc {
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
                Node::EndObject,
            ],
            strings: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ],
            numbers: vec![1.0, 2.0, 3.0, 4.0],
        };
        let value_from_jsonc: Value = jsonc.into();

        assert_eq!(value_from_jsonc, value);
    }

    #[test]
    fn test_jsonc_vector() {
        let jsonc_vec = JsoncVector {
            data: vec![
                serde_json::from_str(r#"{"a":1.0,"b":[2.0,3.0],"c":{"d":4.0}}"#).unwrap(),
                serde_json::from_str(r#"{"e":5.0,"f":[6.0,7.0],"g":{"h":8.0}}"#).unwrap(),
            ],
        };
        jsonc_vec.flush("test_jsonc_vector.parquet");
        let mut loaded_jsonc_vec = JsoncVector::default();
        loaded_jsonc_vec.load("test_jsonc_vector.parquet");
        assert_eq!(loaded_jsonc_vec, jsonc_vec);

        std::fs::remove_file(format!("{}/test_jsonc_vector.parquet", PARQUET_DIR)).unwrap();
    }
}
