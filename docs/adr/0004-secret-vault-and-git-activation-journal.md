# Secret Vault Bootstrap, Git Activation Journal, and Lockfile Caching

## Decision

1. **Secret vault bootstrap:** Encrypted secrets live in SQLite (`encrypted_secret_blobs`) encrypted via AES-256-GCM with per-secret AAD (`<secret_name>:v<key_version>`). The 256-bit master key is read from `DASH_MASTER_KEY` or `<data_dir>/vault.key` (mode `0600`), never stored inside SQLite or the Git Workspace. Known secrets are registered with a thread-safe `Redactor` that scrubs plaintext from logs, trace spans, errors, and HTTP responses.
2. **Git activation journal & crash recovery:** Draft activation records transitions (`started` -> `committed` -> `activated` -> `pushed`) in SQLite `activation_journal`. On startup, `DraftManager::initialize` reconciles any incomplete journal against Workspace `HEAD` so crashed activations never leave a split-brain active Generation. Remote push failure marks `git_sync_state = unsynced` while keeping the locally committed and activated Generation operational for subsequent `dash git sync` retries.
3. **Offline lockfile reproducibility:** External Git packages declared in `packages.toml` are resolved transitively to exact Git commit SHAs and SHA-256 tree digests in `packages.lock`, with package trees cached under `<data_dir>/package_cache/<tree_digest>/` so restarts operate completely offline without re-fetching moving refs.
