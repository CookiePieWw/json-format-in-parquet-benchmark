use crate::codec::JsonCodec;

use super::jsonb::JsonbVector;

pub enum Format {
    Jsonb(JsonbVector),
}

impl Format {
    pub fn get_format(name: &str) -> Self {
        match name {
            "jsonb" => Format::Jsonb(JsonbVector::default()),
            _ => panic!("Unsupported format: {}", name),
        }
    }

    pub fn encode(&mut self, json_strs: &[&[u8]]) {
        match self {
            Format::Jsonb(jsonb) => jsonb.encode(json_strs),
        }
    }

    pub fn decode(&self) -> Vec<String> {
        match self {
            Format::Jsonb(jsonb) => jsonb.decode(),
        }
    }

    pub fn flush(&self, path: &str) {
        match self {
            Format::Jsonb(jsonb) => jsonb.flush(path),
        }
    }

    pub fn load(&mut self, path: &str) {
        match self {
            Format::Jsonb(jsonb) => jsonb.load(path),
        }
    }
}
