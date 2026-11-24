//! Secret encryption vault (AES-256-GCM) and secret redaction utility.
//!
//! Master keys live outside SQLite and outside the Git Workspace (either `DASH_MASTER_KEY`
//! or `<data_dir>/vault.key` with 0600 permissions). Plaintext secrets never appear in Git,
//! API responses, MCP outputs, widget messages, logs, or traces.

use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use rand::RngCore;
use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, RwLock};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VaultError {
    #[error("invalid master key format (expected 32-byte hex or base64)")]
    InvalidMasterKey,
    #[error("vault encryption failed")]
    EncryptionFailed,
    #[error("vault decryption failed (tampered ciphertext or wrong master key)")]
    DecryptionFailed,
    #[error("vault I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Upper bound on Administrator-set secret plaintexts held by the redactor (§20: no unbounded caches).
pub const MAX_REDACTED_SECRETS: usize = 4096;
/// Upper bound on server-generated credential tokens (sessions, Agent tokens) held by the redactor.
/// The oldest token is evicted first; tokens are high-entropy and never appear in content unless leaked.
pub const MAX_REDACTED_TOKENS: usize = 2048;
/// Minimum length of a server-generated token accepted by [`Redactor::register_token`].
pub const MIN_REDACTED_TOKEN_LEN: usize = 16;

#[derive(Default)]
struct RedactorInner {
    secrets: HashSet<String>,
    tokens: HashSet<String>,
    token_order: VecDeque<String>,
}

/// Thread-safe redactor storing known sensitive plaintext substrings to scrub from logs/traces/errors.
///
/// Only values the engine itself knows to be sensitive may be registered: decrypted vault secrets
/// (`register_secret`) and server-generated credentials after creation or successful verification
/// (`register_token`). Caller-supplied, unauthenticated input must never be registered, otherwise any
/// client could poison redaction of ordinary words (§5.2, §14.3).
#[derive(Clone, Default)]
pub struct Redactor {
    inner: Arc<RwLock<RedactorInner>>,
}

impl Redactor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a central secret plaintext (vault-managed). Bounded by [`MAX_REDACTED_SECRETS`].
    pub fn register_secret(&self, secret: &str) {
        if secret.len() >= 4 {
            if let Ok(mut guard) = self.inner.write() {
                if guard.secrets.contains(secret) {
                    return;
                }
                if guard.secrets.len() >= MAX_REDACTED_SECRETS {
                    tracing::warn!(
                        "redactor secret set is full ({MAX_REDACTED_SECRETS}); new secret not registered"
                    );
                    return;
                }
                guard.secrets.insert(secret.to_string());
            }
        }
    }

    /// Register a server-generated credential token (session or Agent token). Bounded with
    /// oldest-first eviction by [`MAX_REDACTED_TOKENS`]; short values are ignored.
    pub fn register_token(&self, token: &str) {
        if token.len() < MIN_REDACTED_TOKEN_LEN {
            return;
        }
        if let Ok(mut guard) = self.inner.write() {
            if guard.tokens.contains(token) {
                return;
            }
            while guard.tokens.len() >= MAX_REDACTED_TOKENS {
                let Some(old) = guard.token_order.pop_front() else {
                    break;
                };
                guard.tokens.remove(&old);
            }
            guard.tokens.insert(token.to_string());
            guard.token_order.push_back(token.to_string());
        }
    }

    pub fn unregister_secret(&self, secret: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.secrets.remove(secret);
            if guard.tokens.remove(secret) {
                guard.token_order.retain(|t| t != secret);
            }
        }
    }

    /// Number of registered sensitive values (secrets + tokens).
    pub fn registered_len(&self) -> usize {
        self.inner
            .read()
            .map(|g| g.secrets.len() + g.tokens.len())
            .unwrap_or(0)
    }

    pub fn is_registered(&self, value: &str) -> bool {
        self.inner
            .read()
            .map(|g| g.secrets.contains(value) || g.tokens.contains(value))
            .unwrap_or(false)
    }

    pub fn redact_str(&self, input: &str) -> String {
        let Ok(guard) = self.inner.read() else {
            return "[REDACTED_ERROR]".to_string();
        };
        let mut out = input.to_string();
        for s in guard.secrets.iter().chain(guard.tokens.iter()) {
            if out.contains(s.as_str()) {
                out = out.replace(s.as_str(), "[REDACTED]");
            }
        }
        out
    }

    pub fn redact_json(&self, val: &serde_json::Value) -> serde_json::Value {
        match val {
            serde_json::Value::String(s) => serde_json::Value::String(self.redact_str(s)),
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| self.redact_json(v)).collect())
            }
            serde_json::Value::Object(map) => {
                let mut next = serde_json::Map::new();
                for (k, v) in map {
                    let kl = k.to_ascii_lowercase();
                    if kl == "authorization"
                        || kl == "password"
                        || kl == "token_plaintext"
                        || kl == "secret_plaintext"
                        || kl == "api_key"
                    {
                        next.insert(k.clone(), serde_json::Value::String("[REDACTED]".into()));
                    } else {
                        next.insert(k.clone(), self.redact_json(v));
                    }
                }
                serde_json::Value::Object(next)
            }
            other => other.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SecretVault {
    key: [u8; 32],
    redactor: Redactor,
}

impl SecretVault {
    pub fn from_bytes(key: [u8; 32]) -> Self {
        Self {
            key,
            redactor: Redactor::new(),
        }
    }

    /// Generate a random 256-bit key in memory (useful for ephemeral Preview vaults).
    pub fn generate_ephemeral() -> Self {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        Self::from_bytes(key)
    }

    /// Load master key from `DASH_MASTER_KEY` or `<data_dir>/vault.key` (creating it with 0600 on first run).
    pub fn load_or_create(data_dir: &Path) -> Result<Self, VaultError> {
        if let Ok(raw) = std::env::var("DASH_MASTER_KEY") {
            let key = decode_key(raw.trim())?;
            return Ok(Self::from_bytes(key));
        }
        std::fs::create_dir_all(data_dir)?;
        let key_path = data_dir.join("vault.key");
        if key_path.exists() {
            let raw = std::fs::read_to_string(&key_path)?;
            let key = decode_key(raw.trim())?;
            Ok(Self::from_bytes(key))
        } else {
            let mut key = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut key);
            let encoded = hex::encode(key);
            std::fs::write(&key_path, &encoded)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
            }
            Ok(Self::from_bytes(key))
        }
    }

    pub fn redactor(&self) -> &Redactor {
        &self.redactor
    }

    pub fn encrypt(
        &self,
        secret_name: &str,
        key_version: i64,
        plaintext: &str,
    ) -> Result<(Vec<u8>, Vec<u8>), VaultError> {
        let cipher = Aes256Gcm::new_from_slice(&self.key).map_err(|_| VaultError::InvalidMasterKey)?;
        let mut nonce_bytes = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let aad = format!("{secret_name}:v{key_version}");
        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext.as_bytes(),
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| VaultError::EncryptionFailed)?;
        self.redactor.register_secret(plaintext);
        Ok((nonce_bytes.to_vec(), ciphertext))
    }

    pub fn decrypt(
        &self,
        secret_name: &str,
        key_version: i64,
        nonce_bytes: &[u8],
        ciphertext: &[u8],
    ) -> Result<String, VaultError> {
        if nonce_bytes.len() != 12 {
            return Err(VaultError::DecryptionFailed);
        }
        let cipher = Aes256Gcm::new_from_slice(&self.key).map_err(|_| VaultError::InvalidMasterKey)?;
        let nonce = Nonce::from_slice(nonce_bytes);
        let aad = format!("{secret_name}:v{key_version}");
        let plain = cipher
            .decrypt(
                nonce,
                Payload {
                    msg: ciphertext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| VaultError::DecryptionFailed)?;
        let s = String::from_utf8(plain).map_err(|_| VaultError::DecryptionFailed)?;
        self.redactor.register_secret(&s);
        Ok(s)
    }
}

fn decode_key(raw: &str) -> Result<[u8; 32], VaultError> {
    if raw.len() == 64 {
        let bytes = hex::decode(raw).map_err(|_| VaultError::InvalidMasterKey)?;
        return bytes.try_into().map_err(|_| VaultError::InvalidMasterKey);
    }
    if let Ok(bytes) = B64.decode(raw) {
        if bytes.len() == 32 {
            return bytes.try_into().map_err(|_| VaultError::InvalidMasterKey);
        }
    }
    Err(VaultError::InvalidMasterKey)
}
