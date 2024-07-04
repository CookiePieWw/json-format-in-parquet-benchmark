use std::error::Error;
use std::fs::File;
use std::io::BufRead;

use crate::consts::JSON_DIR;

/// Read a file of json strings
pub fn read(file: &str) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let path = format!("{}/{}", JSON_DIR, file);
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);

    let mut result: Vec<Vec<u8>> = Vec::new();
    for line in reader.lines() {
        result.push(line?.as_bytes().to_vec());
    }

    Ok(result)
}

pub trait JsonCodec {
    /// Encode JSON strings into internal representation
    fn encode(json_str: &[&[u8]]) -> Self;
    /// Decode internal representation into JSON strings
    fn decode(&self) -> Vec<String>;

    /// Flush the internal representation to a parquet file
    fn flush(&self, path: &str);
    /// Load a parquet file into the internal representation
    fn load(path: &str) -> Self;

    fn name() -> String;
}
