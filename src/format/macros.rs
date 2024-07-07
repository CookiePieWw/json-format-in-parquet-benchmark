/// Encode and decode function of JsonCodec with formats using serde_json
#[macro_export]
macro_rules! serde_ende {
    () => {
        fn encode(&mut self, json_str: &[&[u8]]) {
            self.data.clear();
            for json_str in json_str {
                let value: Value = serde_json::from_slice(json_str).unwrap();
                self.data.push(value);
            }
        }

        fn decode(&self) -> Vec<String> {
            self.data.iter().map(|v| v.to_string()).collect()
        }
    };
}
