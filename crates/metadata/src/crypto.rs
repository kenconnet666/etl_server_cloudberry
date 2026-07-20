//! Encryption for connection credentials stored in the control database.

use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use chacha20poly1305::{
    Key, XChaCha20Poly1305, XNonce,
    aead::{Aead, AeadCore, KeyInit, OsRng, Payload},
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop};

const MASTER_KEY_BYTES: usize = 32;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("master key is not valid base64")]
    InvalidBase64(#[source] base64::DecodeError),
    #[error("master key must decode to exactly 32 bytes")]
    InvalidKeyLength,
    #[error("credential encryption failed")]
    Encryption,
    #[error("credential decryption failed")]
    Decryption,
    #[error("encrypted credential has an invalid nonce")]
    InvalidNonce,
    #[error("credential is not valid UTF-8")]
    InvalidUtf8(#[source] std::string::FromUtf8Error),
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct MasterKey([u8; MASTER_KEY_BYTES]);

impl MasterKey {
    pub fn from_base64(value: &SecretString) -> Result<Self, CryptoError> {
        let mut decoded = STANDARD
            .decode(value.expose_secret())
            .map_err(CryptoError::InvalidBase64)?;
        if decoded.len() != MASTER_KEY_BYTES {
            decoded.zeroize();
            return Err(CryptoError::InvalidKeyLength);
        }
        let mut key = [0_u8; MASTER_KEY_BYTES];
        key.copy_from_slice(&decoded);
        decoded.zeroize();
        Ok(Self(key))
    }

    pub fn encrypt(
        &self,
        plaintext: &SecretString,
        associated_data: &[u8],
        key_version: u32,
    ) -> Result<EncryptedSecret, CryptoError> {
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.0));
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext.expose_secret().as_bytes(),
                    aad: associated_data,
                },
            )
            .map_err(|_| CryptoError::Encryption)?;
        Ok(EncryptedSecret {
            key_version,
            nonce: nonce.to_vec(),
            ciphertext,
        })
    }

    pub fn decrypt(
        &self,
        encrypted: &EncryptedSecret,
        associated_data: &[u8],
    ) -> Result<SecretString, CryptoError> {
        let nonce = XNonce::from_exact_iter(encrypted.nonce.iter().copied())
            .ok_or(CryptoError::InvalidNonce)?;
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.0));
        let mut plaintext = cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: &encrypted.ciphertext,
                    aad: associated_data,
                },
            )
            .map_err(|_| CryptoError::Decryption)?;
        let decoded = String::from_utf8(plaintext.clone()).map_err(CryptoError::InvalidUtf8);
        plaintext.zeroize();
        decoded.map(SecretString::from)
    }
}

impl fmt::Debug for MasterKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MasterKey([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedSecret {
    pub key_version: u32,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    fn test_key() -> MasterKey {
        let key = SecretString::from(STANDARD.encode([7_u8; MASTER_KEY_BYTES]));
        MasterKey::from_base64(&key).unwrap()
    }

    #[test]
    fn encrypts_with_bound_associated_data() {
        let key = test_key();
        let plaintext = SecretString::from("postgresql://user:password@source/db");
        let encrypted = key.encrypt(&plaintext, b"source:one", 1).unwrap();

        let decrypted = key.decrypt(&encrypted, b"source:one").unwrap();
        assert_eq!(decrypted.expose_secret(), plaintext.expose_secret());
        assert!(key.decrypt(&encrypted, b"source:two").is_err());
    }

    #[test]
    fn rejects_wrong_length_key() {
        let key = SecretString::from(STANDARD.encode([1_u8; 16]));
        assert!(MasterKey::from_base64(&key).is_err());
    }
}
