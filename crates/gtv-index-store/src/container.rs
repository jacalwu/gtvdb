//! The `.gtvidx` container format.
//!
//! ```text
//! magic "GTVIDX\0\0" (8) | version u16 | flags u16 | manifest_len u32
//! | manifest JSON | index payload | checksum blake3 [u8; 32]
//! ```
//!
//! The trailing checksum covers every preceding byte, so a truncated or
//! tampered file is rejected on load.

use crate::{IndexStoreError, Result};

/// Container magic.
pub const CONTAINER_MAGIC: &[u8; 8] = b"GTVIDX\0\0";
/// Container format version.
pub const CONTAINER_VERSION: u16 = 1;
const HEADER_LEN: usize = 16;
const CHECKSUM_LEN: usize = 32;

/// Wrap `manifest_json` + `payload` into a `.gtvidx` byte blob.
pub fn encode(manifest_json: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + manifest_json.len() + payload.len() + CHECKSUM_LEN);
    out.extend_from_slice(CONTAINER_MAGIC);
    out.extend_from_slice(&CONTAINER_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&(manifest_json.len() as u32).to_le_bytes());
    out.extend_from_slice(manifest_json);
    out.extend_from_slice(payload);
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    out
}

/// Split a `.gtvidx` blob into `(manifest_json, payload)`, verifying length and
/// checksum.
pub fn decode(bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    if bytes.len() < HEADER_LEN + CHECKSUM_LEN {
        return Err(IndexStoreError::Corrupt("index container too short".into()));
    }
    if &bytes[0..8] != &CONTAINER_MAGIC[..] {
        return Err(IndexStoreError::Corrupt("bad index container magic".into()));
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    if version != CONTAINER_VERSION {
        return Err(IndexStoreError::Corrupt(format!(
            "unsupported container version {version}"
        )));
    }
    let manifest_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let manifest_end = HEADER_LEN
        .checked_add(manifest_len)
        .ok_or_else(|| IndexStoreError::Corrupt("manifest length overflow".into()))?;
    let payload_end = bytes.len() - CHECKSUM_LEN;
    if manifest_end > payload_end {
        return Err(IndexStoreError::Corrupt("manifest exceeds container".into()));
    }
    let (body, checksum) = bytes.split_at(payload_end);
    if blake3::hash(body).as_bytes() != checksum {
        return Err(IndexStoreError::Corrupt("index container checksum mismatch".into()));
    }
    Ok((
        bytes[HEADER_LEN..manifest_end].to_vec(),
        bytes[manifest_end..payload_end].to_vec(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let bytes = encode(b"{\"a\":1}", b"payload-bytes");
        let (m, p) = decode(&bytes).unwrap();
        assert_eq!(m, b"{\"a\":1}");
        assert_eq!(p, b"payload-bytes");
    }

    #[test]
    fn tamper_is_detected() {
        let mut bytes = encode(b"{}", b"data");
        let n = bytes.len();
        bytes[n - 40] ^= 0xff;
        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn truncation_is_detected() {
        let bytes = encode(b"{}", b"data");
        assert!(decode(&bytes[..bytes.len() - 4]).is_err());
    }
}
