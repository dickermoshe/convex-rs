use flutter_rust_bridge::frb;

/// Helper functions for encoding `Bytes`s as `String`s.
pub struct JsonBytes {}

impl JsonBytes {
    /// Encode a binary string as a string.
    #[frb(sync)]
    pub fn encode(bytes: &Vec<u8>) -> String {
        base64::encode(&bytes[..])
    }

    /// Decode a binary string from a string.
    #[frb(sync)]
    pub fn decode(s: String) -> anyhow::Result<Vec<u8>> {
        Ok(base64::decode(s.as_bytes())?)
    }
}
