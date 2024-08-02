use crate::codec::JsonCodec;

use super::jsonb::JsonbVector;
use super::jsonc::JsoncVector;
use super::plain_json::PlainJsonVector;
use super::variant::VariantVector;

pub enum Format {
    Jsonb(JsonbVector),
    PlainJson(PlainJsonVector),
    Jsonc(JsoncVector),
    Variant(VariantVector),
}

impl Format {
    pub fn get_format(name: &str) -> Self {
        match name {
            "jsonb" => Format::Jsonb(JsonbVector::default()),
            "plain_json" => Format::PlainJson(PlainJsonVector::default()),
            "jsonc" => Format::Jsonc(JsoncVector::default()),
            "variant" => Format::Variant(VariantVector::default()),
            _ => panic!("Unsupported format: {}", name),
        }
    }

    pub fn encode(&mut self, json_strs: &[&[u8]]) {
        match self {
            Format::Jsonb(jsonb) => jsonb.encode(json_strs),
            Format::PlainJson(plain_json) => plain_json.encode(json_strs),
            Format::Jsonc(jsonc) => jsonc.encode(json_strs),
            Format::Variant(variant) => variant.encode(json_strs),
        }
    }

    pub fn decode(&self) -> Vec<String> {
        match self {
            Format::Jsonb(jsonb) => jsonb.decode(),
            Format::PlainJson(plain_json) => plain_json.decode(),
            Format::Jsonc(jsonc) => jsonc.decode(),
            Format::Variant(variant) => variant.decode(),
        }
    }

    pub fn flush(&self, path: &str) {
        match self {
            Format::Jsonb(jsonb) => jsonb.flush(path),
            Format::PlainJson(plain_json) => plain_json.flush(path),
            Format::Jsonc(jsonc) => jsonc.flush(path),
            Format::Variant(variant) => variant.flush(path),
        }
    }

    pub fn load(&mut self, path: &str) {
        match self {
            Format::Jsonb(jsonb) => jsonb.load(path),
            Format::PlainJson(plain_json) => plain_json.load(path),
            Format::Jsonc(jsonc) => jsonc.load(path),
            Format::Variant(variant) => variant.load(path),
        }
    }
}
