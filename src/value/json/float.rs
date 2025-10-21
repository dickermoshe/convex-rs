use anyhow::anyhow;
use flutter_rust_bridge::frb;

/// Helper functions for encoding `f64`s as `String`s.
pub struct JsonFloat {}

impl JsonFloat {
    /// Encode an `f64` as a string.
    #[frb(ignore)]
    pub fn encode(n: f64) -> String {
        base64::encode(n.to_le_bytes())
    }

    /// Decode an `f64` from a string.
    #[frb(ignore)]
    pub fn decode(s: String) -> anyhow::Result<f64> {
        let bytes: [u8; 8] = base64::decode(s.as_bytes())?
            .try_into()
            .map_err(|_| anyhow!("Float64 must be exactly eight bytes"))?;
        Ok(f64::from_le_bytes(bytes))
    }
}
