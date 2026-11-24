//! Secret encryption vault (AES-256-GCM) and secret redaction utility.
//!
//! Master keys live outside SQLite and outside the Git Workspace. Key sources, in order of
//! precedence (§5.2, §19.5 — documented default in ADR-0004):
//!
//! 1. `DASH_MASTER_KEY` — Administrator-provided key (hex or base64), never written to disk.
//! 2. `DASH_MASTER_KEY_FILE` — key file at an Administrator-chosen path (ideally outside the
//!    data directory, e.g. a separately mounted secret volume); created `0600` if missing.
//! 3. `DASH_VAULT_PASSPHRASE` — `<data_dir>/vault.key` is stored *wrapped*: an Argon2id KEK
//!    derived from the passphrase encrypts the data key with AES-256-GCM, so a copy of the data
//!    directory alone cannot decrypt secrets. A legacy plaintext key file is re-wrapped in place.
//! 4. Fallback for unattended single-host use: plaintext `<data_dir>/vault.key` (`0600`,
//!    created atomically). This co-locates key and ciphertext, so the vault reports
//!    [`KeyProtection::UnprotectedColocated`] and `dash doctor` warns about it.
//!
//! Plaintext secrets never appear in Git, API responses, MCP outputs, widget messages, logs,
//! or traces.

use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use thiserror::Error;

const WRAPPED_KEY_PREFIX: &str = "dash-vault-key-v1:argon2id:";
const WRAPPED_KEY_AAD: &[u8] = b"dash-vault-key-v1";

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
    #[error("vault key file is passphrase-wrapped; set DASH_VAULT_PASSPHRASE to unlock it")]
    PassphraseRequired,
    #[error("vault passphrase is wrong or the wrapped key file is corrupted")]
    UnwrapFailed,
}

/// How the vault master key is protected at rest (reported by `dash doctor`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KeyProtection {
    /// Key supplied through `DASH_MASTER_KEY`; never persisted by dash.
    EnvMasterKey,
    /// Key read from an Administrator-chosen file outside the data directory.
    ExternalKeyFile { path: String },
    /// Key file in the data directory wrapped with an Argon2id passphrase-derived KEK.
    PassphraseWrapped { path: String },
    /// Plaintext key file next to the SQLite ciphertext (no additional protection boundary).
    UnprotectedColocated { path: String },
    /// Ephemeral in-memory key (Preview and tests).
    Ephemeral,
}

impl KeyProtection {
    /// A warning when ciphertext and its decryption key are stored together unprotected (§5.2).
    pub fn warning(&self) -> Option<String> {
        match self {
            Self::UnprotectedColocated { path } => Some(format!(
                "vault master key '{path}' is stored unwrapped next to the encrypted secrets; set \
                 DASH_VAULT_PASSPHRASE (wraps the key with Argon2id), DASH_MASTER_KEY, or \
                 DASH_MASTER_KEY_FILE pointing outside the data directory"
            )),
            _ => None,
        }
    }
}

/// Where the vault master key comes from (see module docs for precedence).
#[derive(Debug, Clone, Default)]
pub struct VaultKeySource {
    pub master_key: Option<String>,
    pub key_file: Option<PathBuf>,
    pub passphrase: Option<String>,
}

impl VaultKeySource {
    pub fn from_env() -> Self {
        let non_empty = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        Self {
            master_key: non_empty("DASH_MASTER_KEY"),
            key_file: non_empty("DASH_MASTER_KEY_FILE").map(PathBuf::from),
            passphrase: std::env::var("DASH_VAULT_PASSPHRASE").ok().filter(|v| !v.is_empty()),
        }
    }
}

/// Thread-safe redactor storing known sensitive plaintext substrings to scrub from logs/traces/errors.
#[derive(Clone, Default)]
pub struct Redactor {
    secrets: Arc<RwLock<HashSet<String>>>,
}

impl Redactor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_secret(&self, secret: &str) {
        if secret.len() >= 4 {
            if let Ok(mut guard) = self.secrets.write() {
                guard.insert(secret.to_string());
            }
        }
    }

    pub fn unregister_secret(&self, secret: &str) {
        if let Ok(mut guard) = self.secrets.write() {
            guard.remove(secret);
        }
    }

    pub fn redact_str(&self, input: &str) -> String {
        let Ok(guard) = self.secrets.read() else {
            return "[REDACTED_ERROR]".to_string();
        };
        let mut out = input.to_string();
        for s in guard.iter() {
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
    protection: KeyProtection,
}

impl SecretVault {
    pub fn from_bytes(key: [u8; 32]) -> Self {
        Self {
            key,
            redactor: Redactor::new(),
            protection: KeyProtection::EnvMasterKey,
        }
    }

    /// Generate a random 256-bit key in memory (useful for ephemeral Preview vaults).
    pub fn generate_ephemeral() -> Self {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        let mut v = Self::from_bytes(key);
        v.protection = KeyProtection::Ephemeral;
        v
    }

    /// Load the master key using the environment-selected source (see module docs).
    pub fn load_or_create(data_dir: &Path) -> Result<Self, VaultError> {
        Self::load_with(data_dir, &VaultKeySource::from_env())
    }

    /// Load (or create on first run) the master key from an explicit [`VaultKeySource`].
    pub fn load_with(data_dir: &Path, source: &VaultKeySource) -> Result<Self, VaultError> {
        if let Some(raw) = source.master_key.as_deref() {
            let key = decode_key(raw.trim())?;
            return Ok(Self::with_protection(key, KeyProtection::EnvMasterKey));
        }
        std::fs::create_dir_all(data_dir)?;
        let (key_path, external) = match source.key_file.as_ref() {
            Some(p) => (p.clone(), !path_is_within(p, data_dir)),
            None => (data_dir.join("vault.key"), false),
        };
        let passphrase = source.passphrase.as_deref();
        let path_str = key_path.to_string_lossy().to_string();

        let key = if key_path.exists() {
            let raw = std::fs::read_to_string(&key_path)?;
            let raw = raw.trim();
            if raw.starts_with(WRAPPED_KEY_PREFIX) {
                let pass = passphrase.ok_or(VaultError::PassphraseRequired)?;
                unwrap_key(raw, pass)?
            } else {
                let key = decode_key(raw)?;
                if let Some(pass) = passphrase {
                    // Upgrade a legacy plaintext key file in place (atomic replace, 0600).
                    write_key_file_atomic(&key_path, &wrap_key(&key, pass)?, true)?;
                }
                key
            }
        } else {
            let mut key = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut key);
            let encoded = match passphrase {
                Some(pass) => wrap_key(&key, pass)?,
                None => hex::encode(key),
            };
            write_key_file_atomic(&key_path, &encoded, false)?;
            key
        };

        let protection = if passphrase.is_some() {
            KeyProtection::PassphraseWrapped { path: path_str }
        } else if external {
            KeyProtection::ExternalKeyFile { path: path_str }
        } else {
            KeyProtection::UnprotectedColocated { path: path_str }
        };
        if let Some(w) = protection.warning() {
            tracing::warn!("{w}");
        }
        Ok(Self::with_protection(key, protection))
    }

    fn with_protection(key: [u8; 32], protection: KeyProtection) -> Self {
        Self {
            key,
            redactor: Redactor::new(),
            protection,
        }
    }

    /// How the master key is protected at rest.
    pub fn key_protection(&self) -> &KeyProtection {
        &self.protection
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

fn path_is_within(path: &Path, dir: &Path) -> bool {
    let canon = |p: &Path| {
        p.canonicalize().unwrap_or_else(|_| {
            // File may not exist yet: canonicalize its parent instead.
            match (p.parent(), p.file_name()) {
                (Some(parent), Some(name)) => parent
                    .canonicalize()
                    .map(|c| c.join(name))
                    .unwrap_or_else(|_| p.to_path_buf()),
                _ => p.to_path_buf(),
            }
        })
    };
    canon(path).starts_with(canon(dir))
}

/// Create (or atomically replace) a key file that is `0600` from its first byte on disk.
fn write_key_file_atomic(path: &Path, contents: &str, replace: bool) -> Result<(), VaultError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let target = if replace {
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(format!(".tmp-{}", std::process::id()));
        PathBuf::from(tmp)
    } else {
        path.to_path_buf()
    };
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&target)?;
    f.write_all(contents.as_bytes())?;
    f.sync_all()?;
    drop(f);
    if replace {
        std::fs::rename(&target, path)?;
    }
    Ok(())
}

fn derive_kek(passphrase: &str, salt: &[u8]) -> Result<[u8; 32], VaultError> {
    let mut kek = [0u8; 32];
    argon2::Argon2::default()
        .hash_password_into(passphrase.as_bytes(), salt, &mut kek)
        .map_err(|_| VaultError::EncryptionFailed)?;
    Ok(kek)
}

/// Wrap a data key: `dash-vault-key-v1:argon2id:<salt>:<nonce>:<ciphertext>` (base64 parts).
fn wrap_key(key: &[u8; 32], passphrase: &str) -> Result<String, VaultError> {
    let mut salt = [0u8; 16];
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut salt);
    rand::thread_rng().fill_bytes(&mut nonce);
    let kek = derive_kek(passphrase, &salt)?;
    let cipher = Aes256Gcm::new_from_slice(&kek).map_err(|_| VaultError::InvalidMasterKey)?;
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: key,
                aad: WRAPPED_KEY_AAD,
            },
        )
        .map_err(|_| VaultError::EncryptionFailed)?;
    Ok(format!(
        "{WRAPPED_KEY_PREFIX}{}:{}:{}",
        B64.encode(salt),
        B64.encode(nonce),
        B64.encode(ct)
    ))
}

fn unwrap_key(raw: &str, passphrase: &str) -> Result<[u8; 32], VaultError> {
    let rest = raw
        .strip_prefix(WRAPPED_KEY_PREFIX)
        .ok_or(VaultError::UnwrapFailed)?;
    let parts: Vec<&str> = rest.split(':').collect();
    if parts.len() != 3 {
        return Err(VaultError::UnwrapFailed);
    }
    let decode = |s: &str| B64.decode(s).map_err(|_| VaultError::UnwrapFailed);
    let (salt, nonce, ct) = (decode(parts[0])?, decode(parts[1])?, decode(parts[2])?);
    if nonce.len() != 12 {
        return Err(VaultError::UnwrapFailed);
    }
    let kek = derive_kek(passphrase, &salt)?;
    let cipher = Aes256Gcm::new_from_slice(&kek).map_err(|_| VaultError::InvalidMasterKey)?;
    let plain = cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &ct,
                aad: WRAPPED_KEY_AAD,
            },
        )
        .map_err(|_| VaultError::UnwrapFailed)?;
    plain.try_into().map_err(|_| VaultError::UnwrapFailed)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: &SecretVault) -> (Vec<u8>, Vec<u8>) {
        v.encrypt("s", 1, "plain-value").unwrap()
    }

    #[test]
    fn default_key_file_is_created_0600_and_reported_unprotected() {
        let tmp = tempfile::tempdir().unwrap();
        let v = SecretVault::load_with(tmp.path(), &VaultKeySource::default()).unwrap();
        assert!(matches!(v.key_protection(), KeyProtection::UnprotectedColocated { .. }));
        assert!(v.key_protection().warning().is_some());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(tmp.path().join("vault.key"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // Reload yields the same key.
        let (n, ct) = roundtrip(&v);
        let v2 = SecretVault::load_with(tmp.path(), &VaultKeySource::default()).unwrap();
        assert_eq!(v2.decrypt("s", 1, &n, &ct).unwrap(), "plain-value");
    }

    #[test]
    fn passphrase_wraps_key_and_data_dir_copy_alone_cannot_unlock() {
        let tmp = tempfile::tempdir().unwrap();
        let src = VaultKeySource {
            passphrase: Some("correct horse battery staple".into()),
            ..Default::default()
        };
        let v = SecretVault::load_with(tmp.path(), &src).unwrap();
        assert!(matches!(v.key_protection(), KeyProtection::PassphraseWrapped { .. }));
        assert!(v.key_protection().warning().is_none());
        let raw = std::fs::read_to_string(tmp.path().join("vault.key")).unwrap();
        assert!(raw.starts_with(WRAPPED_KEY_PREFIX));
        let (n, ct) = roundtrip(&v);

        // Without the passphrase the key file is useless.
        let err = SecretVault::load_with(tmp.path(), &VaultKeySource::default())
            .err()
            .unwrap();
        assert!(matches!(err, VaultError::PassphraseRequired));
        let wrong = VaultKeySource {
            passphrase: Some("wrong".into()),
            ..Default::default()
        };
        assert!(matches!(
            SecretVault::load_with(tmp.path(), &wrong).err().unwrap(),
            VaultError::UnwrapFailed
        ));
        let v2 = SecretVault::load_with(tmp.path(), &src).unwrap();
        assert_eq!(v2.decrypt("s", 1, &n, &ct).unwrap(), "plain-value");
    }

    #[test]
    fn legacy_plaintext_key_is_rewrapped_when_passphrase_is_set() {
        let tmp = tempfile::tempdir().unwrap();
        let legacy = SecretVault::load_with(tmp.path(), &VaultKeySource::default()).unwrap();
        let (n, ct) = roundtrip(&legacy);
        let src = VaultKeySource {
            passphrase: Some("pw".into()),
            ..Default::default()
        };
        let upgraded = SecretVault::load_with(tmp.path(), &src).unwrap();
        assert_eq!(upgraded.decrypt("s", 1, &n, &ct).unwrap(), "plain-value");
        let raw = std::fs::read_to_string(tmp.path().join("vault.key")).unwrap();
        assert!(raw.starts_with(WRAPPED_KEY_PREFIX));
        assert!(!raw.contains(&hex::encode(legacy.key)));
    }

    #[test]
    fn external_key_file_outside_data_dir_is_reported_as_separated() {
        let data = tempfile::tempdir().unwrap();
        let keys = tempfile::tempdir().unwrap();
        let src = VaultKeySource {
            key_file: Some(keys.path().join("dash-master.key")),
            ..Default::default()
        };
        let v = SecretVault::load_with(data.path(), &src).unwrap();
        assert!(matches!(v.key_protection(), KeyProtection::ExternalKeyFile { .. }));
        assert!(!data.path().join("vault.key").exists());
        assert!(keys.path().join("dash-master.key").exists());

        // A key file configured *inside* the data dir is still co-located.
        let inside = VaultKeySource {
            key_file: Some(data.path().join("other.key")),
            ..Default::default()
        };
        let v2 = SecretVault::load_with(data.path(), &inside).unwrap();
        assert!(matches!(v2.key_protection(), KeyProtection::UnprotectedColocated { .. }));
    }

    #[test]
    fn env_master_key_takes_precedence() {
        let tmp = tempfile::tempdir().unwrap();
        let src = VaultKeySource {
            master_key: Some(hex::encode([7u8; 32])),
            ..Default::default()
        };
        let v = SecretVault::load_with(tmp.path(), &src).unwrap();
        assert_eq!(v.key_protection(), &KeyProtection::EnvMasterKey);
        assert!(!tmp.path().join("vault.key").exists());
    }
}
