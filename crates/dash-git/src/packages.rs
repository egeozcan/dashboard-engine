//! Git Package resolution, transitive lockfile (`packages.lock`), cycle/conflict detection,
//! and offline `<data_dir>/package_cache/<tree_digest>` caching (§4.4, §12.1).

use crate::git_cli::GitError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PackagesToml {
    #[serde(default)]
    pub local: LocalPackagesSection,
    #[serde(default)]
    pub dependencies: BTreeMap<String, PackageDependencySpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalPackagesSection {
    #[serde(default)]
    pub packages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PackageDependencySpec {
    pub git: String,
    #[serde(rename = "ref", default = "default_ref")]
    pub git_ref: String,
    #[serde(default)]
    pub subdir: Option<String>,
}

fn default_ref() -> String {
    "main".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackagesLock {
    #[serde(default = "default_lock_ver")]
    pub version: u32,
    #[serde(default)]
    pub packages: Vec<LockedPackage>,
}

impl Default for PackagesLock {
    fn default() -> Self {
        Self {
            version: 1,
            packages: Vec::new(),
        }
    }
}

fn default_lock_ver() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockedPackage {
    pub name: String,
    pub git: String,
    pub requested_ref: String,
    pub commit: String,
    pub tree_digest: String,
    #[serde(default)]
    pub subdir: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PackageManifestToml {
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub dependencies: BTreeMap<String, PackageDependencySpec>,
}

pub fn read_packages_toml(workspace_dir: &Path) -> Result<PackagesToml, GitError> {
    let path = workspace_dir.join("packages.toml");
    if !path.exists() {
        return Ok(PackagesToml::default());
    }
    let raw = std::fs::read_to_string(&path)?;
    toml::from_str(&raw).map_err(|e| GitError::Package(format!("invalid packages.toml: {e}")))
}

pub fn write_packages_toml(workspace_dir: &Path, cfg: &PackagesToml) -> Result<(), GitError> {
    let raw = toml::to_string_pretty(cfg)
        .map_err(|e| GitError::Package(format!("serialize packages.toml: {e}")))?;
    std::fs::write(workspace_dir.join("packages.toml"), raw)?;
    Ok(())
}

pub fn read_packages_lock(workspace_dir: &Path) -> Result<PackagesLock, GitError> {
    let path = workspace_dir.join("packages.lock");
    if !path.exists() {
        return Ok(PackagesLock::default());
    }
    let raw = std::fs::read_to_string(&path)?;
    // Support empty/comment-only lockfile cleanly
    if raw.trim().is_empty()
        || raw
            .lines()
            .all(|l| l.trim().is_empty() || l.trim_start().starts_with('#'))
    {
        return Ok(PackagesLock::default());
    }
    toml::from_str(&raw).map_err(|e| GitError::Package(format!("invalid packages.lock: {e}")))
}

pub fn write_packages_lock(workspace_dir: &Path, lock: &PackagesLock) -> Result<(), GitError> {
    let raw = toml::to_string_pretty(lock)
        .map_err(|e| GitError::Package(format!("serialize packages.lock: {e}")))?;
    std::fs::write(workspace_dir.join("packages.lock"), raw)?;
    Ok(())
}

/// Package name reserved for the engine's built-in Definitions (`dash.table`, ...).
pub const RESERVED_PACKAGE_NAME: &str = "dash";

/// `^[a-z][a-z0-9_]{0,63}$` — the shape of every Package name.
pub fn is_valid_package_name(name: &str) -> bool {
    dash_luau::declaration::is_valid_identifier(name)
}

/// Whether a dependency URL names a repository on the local host (`file://` or a filesystem path)
/// rather than a remote (`https://`, `ssh://` or scp-like `user@host:path`).
pub fn is_local_git_url(url: &str) -> bool {
    !(url.starts_with("https://") || url.starts_with("ssh://") || is_scp_like_url(url))
}

fn is_scp_like_url(url: &str) -> bool {
    // user@host:path — the user and host contain no '/' or ':'; the path follows the first ':'.
    let Some((user_host, path)) = url.split_once(':') else {
        return false;
    };
    let Some((user, host)) = user_host.split_once('@') else {
        return false;
    };
    !user.is_empty()
        && !host.is_empty()
        && !path.is_empty()
        && !user_host.contains('/')
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
}

/// Validate a dependency requirement before any Git I/O (§4.4: Git I/O is performed by the host,
/// so the host must never be steered into reading arbitrary local paths or passing options).
///
/// * `name` must be a valid, non-reserved Package name.
/// * `git` must be an `https://`, `ssh://` or scp-like URL; local paths and `file://` URLs are
///   accepted only when `allow_local` is true (Administrator-authored configuration). Values that
///   start with `-`, contain whitespace/control characters or use `<transport>::` helpers are
///   always rejected.
/// * `ref` must not start with `-` and must be a plausible Git ref name.
/// * `subdir` must be a plain relative path without `..` components.
pub fn validate_dependency_spec(
    name: &str,
    spec: &PackageDependencySpec,
    allow_local: bool,
) -> Result<(), GitError> {
    if !is_valid_package_name(name) {
        return Err(GitError::Package(format!(
            "invalid package name '{name}' (expected [a-z][a-z0-9_]*)"
        )));
    }
    if name == RESERVED_PACKAGE_NAME {
        return Err(GitError::Package(format!(
            "package name '{RESERVED_PACKAGE_NAME}' is reserved for built-in Definitions"
        )));
    }
    let url = spec.git.as_str();
    if url.is_empty()
        || url.len() > 2048
        || url.starts_with('-')
        || url.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(GitError::Package(format!(
            "package '{name}': invalid git URL '{url}'"
        )));
    }
    if url.contains("::") {
        return Err(GitError::Package(format!(
            "package '{name}': git remote helpers ('<transport>::') are not allowed: '{url}'"
        )));
    }
    if url.starts_with("http://") || url.starts_with("git://") {
        return Err(GitError::Package(format!(
            "package '{name}': unauthenticated transport not allowed, use https:// or ssh://: '{url}'"
        )));
    }
    if is_local_git_url(url) {
        let looks_like_path = url.starts_with("file://")
            || url.starts_with('/')
            || url.starts_with("./")
            || url.starts_with("../")
            || (url.len() > 2 && url.as_bytes()[1] == b':');
        if !looks_like_path {
            return Err(GitError::Package(format!(
                "package '{name}': unsupported git URL '{url}' (expected https://, ssh:// or user@host:path)"
            )));
        }
        if !allow_local {
            return Err(GitError::Package(format!(
                "package '{name}': local repository paths are not allowed for this principal: '{url}'"
            )));
        }
    }
    let r = spec.git_ref.as_str();
    if r.is_empty()
        || r.len() > 256
        || r.starts_with('-')
        || r.contains("..")
        || r.chars().any(|c| {
            c.is_whitespace()
                || c.is_control()
                || matches!(c, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
    {
        return Err(GitError::Package(format!(
            "package '{name}': invalid git ref '{r}'"
        )));
    }
    if let Some(sub) = &spec.subdir {
        if !dash_luau::declaration::is_safe_relative_path(sub) {
            return Err(GitError::Package(format!(
                "package '{name}': subdir '{sub}' must be a relative path inside the repository"
            )));
        }
    }
    Ok(())
}

fn git_command() -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new("git");
    cmd.env("GIT_TERMINAL_PROMPT", "0")
        // Only plain transports; never `ext::` or other remote helpers.
        .env("GIT_ALLOW_PROTOCOL", "https:ssh:file");
    cmd
}

async fn run_pkg_git(cwd: Option<&Path>, args: &[&str]) -> Result<String, GitError> {
    let mut cmd = git_command();
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let output = cmd.args(args).output().await?;
    if !output.status.success() {
        return Err(GitError::Command {
            cmd: format!("git {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Clone `url` into `dest` and check out `rev` (a branch, tag or commit) detached.
/// Returns the checked-out commit hash.
async fn clone_checkout(url: &str, rev: &str, dest: &Path) -> Result<String, GitError> {
    if rev.starts_with('-') {
        return Err(GitError::Package(format!("invalid revision '{rev}'")));
    }
    if dest.exists() {
        std::fs::remove_dir_all(dest)?;
    }
    let dest_s = dest.to_string_lossy().to_string();
    run_pkg_git(
        None,
        &["clone", "--no-checkout", "--quiet", "--", url, &dest_s],
    )
    .await?;
    // Resolve the revision to a commit: the name itself (tags, SHAs, the default branch), then the
    // remote-tracking branch of the same name.
    let mut commit = None;
    for candidate in [
        format!("{rev}^{{commit}}"),
        format!("origin/{rev}^{{commit}}"),
    ] {
        if let Ok(c) = run_pkg_git(
            Some(dest),
            &["rev-parse", "--verify", "--quiet", &candidate],
        )
        .await
        {
            if !c.is_empty() {
                commit = Some(c);
                break;
            }
        }
    }
    let commit = commit
        .ok_or_else(|| GitError::Package(format!("revision '{rev}' not found in '{url}'")))?;
    run_pkg_git(Some(dest), &["checkout", "--quiet", "--detach", &commit]).await?;
    Ok(commit)
}

/// Resolve all external Git dependencies declared in `packages.toml` (transitively),
/// detect cycles and version conflicts, cache fetched trees in `<data_dir>/package_cache/<tree_digest>`,
/// and write deterministic `packages.lock`.
///
/// Every dependency is re-resolved at its requested ref (an explicit "update all pins").
/// Use [`update_package_lock`] to change a single dependency without moving the others.
pub async fn resolve_and_lock_packages(
    workspace_dir: &Path,
    data_dir: &Path,
) -> Result<PackagesLock, GitError> {
    resolve_with_policy(workspace_dir, data_dir, None).await
}

/// Re-resolve only `target` (and any of its subdependencies that are new or whose requirement
/// changed); every other locked package keeps its pinned commit and tree digest (§4.4: the
/// runtime never silently updates external dependencies).
pub async fn update_package_lock(
    workspace_dir: &Path,
    data_dir: &Path,
    target: &str,
) -> Result<PackagesLock, GitError> {
    let existing = read_packages_lock(workspace_dir)?;
    resolve_with_policy(workspace_dir, data_dir, Some((&existing, target))).await
}

/// Like [`update_package_lock`], but resolves the in-memory `manifest` (a candidate
/// `packages.toml`) against the Workspace's current `packages.lock` and writes nothing: the
/// caller persists `packages.toml` and `packages.lock` only once resolution succeeded, so a
/// failed update never leaves an unresolvable requirement behind.
pub async fn resolve_package_update(
    workspace_dir: &Path,
    data_dir: &Path,
    manifest: &PackagesToml,
    target: &str,
) -> Result<PackagesLock, GitError> {
    let existing = read_packages_lock(workspace_dir)?;
    resolve_manifest(manifest, data_dir, Some((&existing, target))).await
}

async fn resolve_with_policy(
    workspace_dir: &Path,
    data_dir: &Path,
    reuse: Option<(&PackagesLock, &str)>,
) -> Result<PackagesLock, GitError> {
    let manifest = read_packages_toml(workspace_dir)?;
    let lock = resolve_manifest(&manifest, data_dir, reuse).await?;
    write_packages_lock(workspace_dir, &lock)?;
    Ok(lock)
}

/// Resolve `manifest` transitively into a lock (fetching into the content-addressed cache);
/// no Workspace file is written.
async fn resolve_manifest(
    manifest: &PackagesToml,
    data_dir: &Path,
    reuse: Option<(&PackagesLock, &str)>,
) -> Result<PackagesLock, GitError> {
    let mut ctx = ResolveCtx {
        data_dir,
        existing: reuse
            .map(|(lock, _)| {
                lock.packages
                    .iter()
                    .map(|p| (p.name.clone(), p.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        reuse_enabled: reuse.is_some(),
        update_target: reuse.map(|(_, t)| t.to_string()),
        visiting: BTreeSet::new(),
        stack: Vec::new(),
        resolved: BTreeMap::new(),
    };

    for (name, spec) in &manifest.dependencies {
        // Root requirements come from the Workspace's own packages.toml (Administrator or
        // service-validated configuration), so local paths are permitted here.
        resolve_recursive(name, spec, true, &mut ctx).await?;
    }

    Ok(PackagesLock {
        version: 1,
        packages: ctx.resolved.into_values().collect(),
    })
}

struct ResolveCtx<'a> {
    data_dir: &'a Path,
    existing: BTreeMap<String, LockedPackage>,
    reuse_enabled: bool,
    update_target: Option<String>,
    visiting: BTreeSet<String>,
    stack: Vec<String>,
    resolved: BTreeMap<String, LockedPackage>,
}

fn resolve_recursive<'a>(
    name: &'a str,
    spec: &'a PackageDependencySpec,
    allow_local: bool,
    ctx: &'a mut ResolveCtx<'_>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), GitError>> + Send + 'a>> {
    Box::pin(async move {
        validate_dependency_spec(name, spec, allow_local)?;
        if ctx.visiting.contains(name) {
            let mut cyc = ctx.stack.clone();
            cyc.push(name.to_string());
            return Err(GitError::Package(format!(
                "package dependency cycle detected: {}",
                cyc.join(" -> ")
            )));
        }
        if let Some(existing) = ctx.resolved.get(name) {
            if existing.git != spec.git
                || existing.requested_ref != spec.git_ref
                || existing.subdir != spec.subdir
            {
                return Err(GitError::Package(format!(
                    "conflicting dependency requirements for '{name}': ({}, {}, subdir {:?}) vs ({}, {}, subdir {:?})",
                    existing.git,
                    existing.requested_ref,
                    existing.subdir,
                    spec.git,
                    spec.git_ref,
                    spec.subdir
                )));
            }
            return Ok(());
        }

        ctx.visiting.insert(name.to_string());
        ctx.stack.push(name.to_string());

        // Reuse the existing pin when the requirement is unchanged and this package is not the
        // one being updated.
        let reusable = if ctx.reuse_enabled && ctx.update_target.as_deref() != Some(name) {
            ctx.existing.get(name).filter(|p| {
                p.git == spec.git && p.requested_ref == spec.git_ref && p.subdir == spec.subdir
            })
        } else {
            None
        }
        .cloned();

        let (commit, tree_digest, cache_dir) = match reusable {
            Some(locked) => {
                let dir = ensure_locked_package_cached(ctx.data_dir, &locked).await?;
                (locked.commit.clone(), locked.tree_digest.clone(), dir)
            }
            None => {
                let tmp_dir = tempfile::tempdir()?;
                let clone_dir = tmp_dir.path().join("repo");
                let commit = clone_checkout(&spec.git, &spec.git_ref, &clone_dir).await?;
                let src_root = package_source_root(name, &clone_dir, spec.subdir.as_deref())?;
                let tree_digest = compute_tree_digest(&src_root)?;
                let dir = install_into_cache(ctx.data_dir, &src_root, &tree_digest)?;
                (commit, tree_digest, dir)
            }
        };

        let sub_deps = read_cached_package_manifest(name, &cache_dir)?
            .map(|m| m.dependencies)
            .unwrap_or_default();

        // A package fetched from a remote cannot point the host at local repositories.
        let child_allow_local = allow_local && is_local_git_url(&spec.git);
        let dep_names: Vec<String> = sub_deps.keys().cloned().collect();
        for (dep_name, dep_spec) in &sub_deps {
            resolve_recursive(dep_name, dep_spec, child_allow_local, ctx).await?;
        }

        ctx.stack.pop();
        ctx.visiting.remove(name);

        ctx.resolved.insert(
            name.to_string(),
            LockedPackage {
                name: name.to_string(),
                git: spec.git.clone(),
                requested_ref: spec.git_ref.clone(),
                commit,
                tree_digest,
                subdir: spec.subdir.clone(),
                dependencies: dep_names,
            },
        );
        Ok(())
    })
}

/// Resolve `subdir` inside a fresh clone, refusing anything that escapes the clone root.
fn package_source_root(
    name: &str,
    clone_dir: &Path,
    subdir: Option<&str>,
) -> Result<PathBuf, GitError> {
    let root = match subdir {
        Some(sub) => {
            if !dash_luau::declaration::is_safe_relative_path(sub) {
                return Err(GitError::Package(format!(
                    "package '{name}': subdir '{sub}' must be a relative path inside the repository"
                )));
            }
            clone_dir.join(sub)
        }
        None => clone_dir.to_path_buf(),
    };
    if !root.exists() {
        return Err(GitError::Package(format!(
            "package '{name}' missing subdir '{}'",
            subdir.unwrap_or("")
        )));
    }
    let canon_root = std::fs::canonicalize(&root)?;
    let canon_clone = std::fs::canonicalize(clone_dir)?;
    if !canon_root.starts_with(&canon_clone) || !canon_root.is_dir() {
        return Err(GitError::Package(format!(
            "package '{name}': subdir '{}' escapes the repository",
            subdir.unwrap_or("")
        )));
    }
    Ok(canon_root)
}

/// Read `package.toml` of a cached Package tree, requiring its `name` to equal the lock key.
fn read_cached_package_manifest(
    name: &str,
    dir: &Path,
) -> Result<Option<PackageManifestToml>, GitError> {
    let pkg_toml_path = dir.join("package.toml");
    if !pkg_toml_path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&pkg_toml_path)?;
    let parsed: PackageManifestToml = toml::from_str(&raw)
        .map_err(|e| GitError::Package(format!("invalid package.toml in '{name}': {e}")))?;
    if parsed.name != name {
        return Err(GitError::Package(format!(
            "package '{name}': package.toml declares name '{}' which differs from the dependency name",
            parsed.name
        )));
    }
    Ok(Some(parsed))
}

pub fn package_cache_dir(data_dir: &Path, tree_digest: &str) -> PathBuf {
    let clean = tree_digest.strip_prefix("sha256:").unwrap_or(tree_digest);
    data_dir.join("package_cache").join(clean)
}

fn valid_tree_digest(d: &str) -> bool {
    match d.strip_prefix("sha256:") {
        Some(hex) => hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()),
        None => false,
    }
}

/// Copy `src_root` into the content-addressed cache atomically: the tree is written to a
/// temporary sibling directory, its digest re-verified, then renamed into place. An existing
/// cache entry is reused only if it still verifies; a corrupt one is replaced.
fn install_into_cache(
    data_dir: &Path,
    src_root: &Path,
    tree_digest: &str,
) -> Result<PathBuf, GitError> {
    if !valid_tree_digest(tree_digest) {
        return Err(GitError::Package(format!(
            "invalid tree digest '{tree_digest}'"
        )));
    }
    let final_dir = package_cache_dir(data_dir, tree_digest);
    if final_dir.exists() {
        if verify_cached_package(&final_dir, tree_digest).is_ok() {
            return Ok(final_dir);
        }
        std::fs::remove_dir_all(&final_dir)?;
    }
    let cache_root = data_dir.join("package_cache");
    std::fs::create_dir_all(&cache_root)?;
    let tmp = cache_root.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    let result = (|| -> Result<(), GitError> {
        copy_tree_excluding_git(src_root, &tmp)?;
        verify_cached_package(&tmp, tree_digest)?;
        match std::fs::rename(&tmp, &final_dir) {
            Ok(()) => Ok(()),
            // A concurrent writer installed the same content first.
            Err(_) if final_dir.exists() => {
                let _ = std::fs::remove_dir_all(&tmp);
                verify_cached_package(&final_dir, tree_digest)
            }
            Err(e) => Err(e.into()),
        }
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&tmp);
    }
    result.map(|_| final_dir)
}

/// Recompute the tree digest of a cached Package and compare it with the locked digest.
pub fn verify_cached_package(dir: &Path, expected_digest: &str) -> Result<(), GitError> {
    let actual = compute_tree_digest(dir)?;
    if actual != expected_digest {
        return Err(GitError::Package(format!(
            "package cache integrity check failed for {}: expected {expected_digest}, found {actual}",
            dir.display()
        )));
    }
    Ok(())
}

/// Validate the fields of a lock entry that are fed to Git and to the filesystem.
fn validate_locked_entry(pkg: &LockedPackage) -> Result<(), GitError> {
    validate_dependency_spec(
        &pkg.name,
        &PackageDependencySpec {
            git: pkg.git.clone(),
            git_ref: pkg.requested_ref.clone(),
            subdir: pkg.subdir.clone(),
        },
        true,
    )?;
    if pkg.commit.len() < 7 || !pkg.commit.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(GitError::Package(format!(
            "locked package '{}': invalid commit '{}'",
            pkg.name, pkg.commit
        )));
    }
    if !valid_tree_digest(&pkg.tree_digest) {
        return Err(GitError::Package(format!(
            "locked package '{}': invalid tree digest '{}'",
            pkg.name, pkg.tree_digest
        )));
    }
    Ok(())
}

/// Make sure a locked Package is present (and verified) in the cache, fetching it at its pinned
/// commit — never at its moving ref — when absent or corrupt.
async fn ensure_locked_package_cached(
    data_dir: &Path,
    pkg: &LockedPackage,
) -> Result<PathBuf, GitError> {
    validate_locked_entry(pkg)?;
    let dir = package_cache_dir(data_dir, &pkg.tree_digest);
    if dir.exists() && verify_cached_package(&dir, &pkg.tree_digest).is_ok() {
        return Ok(dir);
    }
    let tmp_dir = tempfile::tempdir()?;
    let clone_dir = tmp_dir.path().join("repo");
    let commit = clone_checkout(&pkg.git, &pkg.commit, &clone_dir).await?;
    if !commit.starts_with(&pkg.commit) && !pkg.commit.starts_with(&commit) {
        return Err(GitError::Package(format!(
            "locked package '{}': fetched commit {commit} differs from pinned {}",
            pkg.name, pkg.commit
        )));
    }
    let src_root = package_source_root(&pkg.name, &clone_dir, pkg.subdir.as_deref())?;
    let digest = compute_tree_digest(&src_root)?;
    if digest != pkg.tree_digest {
        return Err(GitError::Package(format!(
            "locked package '{}' at commit {}: fetched tree digest {digest} differs from locked {}",
            pkg.name, pkg.commit, pkg.tree_digest
        )));
    }
    install_into_cache(data_dir, &src_root, &pkg.tree_digest)
}

/// Locked packages whose cache entry is missing (cheap existence check; integrity is verified
/// by [`cached_package_roots`] and [`fetch_locked_packages`]).
pub fn missing_locked_packages<'a>(
    data_dir: &Path,
    lock: &'a PackagesLock,
) -> Vec<&'a LockedPackage> {
    lock.packages
        .iter()
        .filter(|p| !package_cache_dir(data_dir, &p.tree_digest).exists())
        .collect()
}

/// Materialize every locked Package into `<data_dir>/package_cache` at its pinned commit,
/// verifying each fetched tree against the locked digest. The lockfile is never modified.
/// Returns the names of the Packages that had to be fetched.
pub async fn fetch_locked_packages(
    data_dir: &Path,
    lock: &PackagesLock,
) -> Result<Vec<String>, GitError> {
    let mut fetched = Vec::new();
    for pkg in &lock.packages {
        validate_locked_entry(pkg)?;
        let dir = package_cache_dir(data_dir, &pkg.tree_digest);
        if dir.exists() && verify_cached_package(&dir, &pkg.tree_digest).is_ok() {
            continue;
        }
        ensure_locked_package_cached(data_dir, pkg).await?;
        fetched.push(pkg.name.clone());
    }
    Ok(fetched)
}

/// Ensure all locked packages are available locally from cache (failing if uncached offline,
/// NEVER silently fetching a moving branch without an explicit lockfile update). Each cached tree
/// is re-verified against its locked digest and its `package.toml` name against the lock key.
pub fn cached_package_roots(
    data_dir: &Path,
    lock: &PackagesLock,
) -> Result<Vec<(String, PathBuf)>, GitError> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for pkg in &lock.packages {
        validate_locked_entry(pkg)?;
        if !seen.insert(pkg.name.clone()) {
            return Err(GitError::Package(format!(
                "packages.lock lists package '{}' more than once",
                pkg.name
            )));
        }
        let dir = package_cache_dir(data_dir, &pkg.tree_digest);
        if !dir.exists() {
            return Err(GitError::Package(format!(
                "locked package '{}' (commit {}, tree {}) not present in offline cache {}",
                pkg.name,
                pkg.commit,
                pkg.tree_digest,
                dir.display()
            )));
        }
        verify_cached_package(&dir, &pkg.tree_digest)?;
        read_cached_package_manifest(&pkg.name, &dir)?;
        out.push((pkg.name.clone(), dir));
    }
    Ok(out)
}

fn symlink_error(path: &Path) -> GitError {
    GitError::Package(format!(
        "symbolic links are not allowed in packages or workspaces: {}",
        path.display()
    ))
}

pub fn compute_tree_digest(root: &Path) -> Result<String, GitError> {
    let mut files = BTreeMap::new();
    collect_files_rel(root, root, &mut files)?;
    let mut hasher = Sha256::new();
    for (rel, bytes) in files {
        hasher.update(rel.as_bytes());
        hasher.update(b"\0");
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
        hasher.update(b"\0");
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

fn collect_files_rel(
    base: &Path,
    cur: &Path,
    out: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), GitError> {
    for entry in std::fs::read_dir(cur)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let path = entry.path();
        // `file_type()` does not follow symbolic links.
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            return Err(symlink_error(&path));
        }
        if ft.is_dir() {
            collect_files_rel(base, &path, out)?;
        } else if ft.is_file() {
            let rel = path
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            out.insert(rel, std::fs::read(&path)?);
        }
    }
    Ok(())
}

/// Recursively copy a tree, skipping `.git`. Symbolic links are refused rather than followed,
/// so a Package (or Workspace) can never pull host files into the cache or a draft worktree.
pub fn copy_tree_excluding_git(src: &Path, dst: &Path) -> Result<(), GitError> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let src_p = entry.path();
        let dst_p = dst.join(&name);
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            return Err(symlink_error(&src_p));
        }
        if ft.is_dir() {
            copy_tree_excluding_git(&src_p, &dst_p)?;
        } else if ft.is_file() {
            std::fs::copy(&src_p, &dst_p)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git_cli::{ensure_git_repo, git_commit_all};
    use tempfile::TempDir;

    fn spec(git: &str, r: &str, subdir: Option<&str>) -> PackageDependencySpec {
        PackageDependencySpec {
            git: git.into(),
            git_ref: r.into(),
            subdir: subdir.map(String::from),
        }
    }

    /// Create a one-Definition Package repository and return its first commit.
    async fn make_pkg_repo(dir: &Path, name: &str, marker: &str) -> String {
        std::fs::create_dir_all(dir.join("hello")).unwrap();
        std::fs::write(
            dir.join("package.toml"),
            format!("name = \"{name}\"\nversion = \"1.0.0\"\n"),
        )
        .unwrap();
        std::fs::write(
            dir.join("hello/definition.luau"),
            format!(
                "local dash = require(\"dash\"); return dash.data_provider {{ id = \"{name}.hello\" }}"
            ),
        )
        .unwrap();
        write_marker(dir, marker);
        ensure_git_repo(dir, "main").await.unwrap();
        git_commit_all(dir, "v1", "dev", None).await.unwrap()
    }

    fn write_marker(dir: &Path, marker: &str) {
        std::fs::write(
            dir.join("hello/runtime.luau"),
            format!(
                "local R = {{}}; function R.query(ctx) return {{ v = \"{marker}\" }} end; return R"
            ),
        )
        .unwrap();
    }

    fn write_ws_deps(ws: &Path, deps: &[(&str, &Path)]) {
        std::fs::create_dir_all(ws).unwrap();
        let mut raw = String::new();
        for (name, repo) in deps {
            raw.push_str(&format!(
                "[dependencies.{name}]\ngit = \"{}\"\nref = \"main\"\n",
                repo.display()
            ));
        }
        std::fs::write(ws.join("packages.toml"), raw).unwrap();
    }

    #[test]
    fn dependency_specs_are_validated() {
        assert!(validate_dependency_spec(
            "ext",
            &spec("https://example.com/r.git", "v1.2.0", None),
            false
        )
        .is_ok());
        assert!(validate_dependency_spec(
            "ext",
            &spec("git@github.com:org/repo.git", "main", None),
            false
        )
        .is_ok());
        assert!(validate_dependency_spec(
            "ext",
            &spec("ssh://git@host/r.git", "main", Some("pkgs/ext")),
            false
        )
        .is_ok());
        // Local repositories only for Administrator-authored configuration.
        assert!(validate_dependency_spec(
            "ext",
            &spec("/home/u/private-repo", "main", None),
            false
        )
        .is_err());
        assert!(validate_dependency_spec(
            "ext",
            &spec("file:///home/u/private-repo", "main", None),
            false
        )
        .is_err());
        assert!(
            validate_dependency_spec("ext", &spec("/home/u/private-repo", "main", None), true)
                .is_ok()
        );
        // Option injection, remote helpers and insecure transports.
        assert!(validate_dependency_spec(
            "ext",
            &spec("--upload-pack=touch /tmp/x", "main", None),
            true
        )
        .is_err());
        assert!(validate_dependency_spec(
            "ext",
            &spec("ext::sh -c touch% /tmp/x", "main", None),
            true
        )
        .is_err());
        assert!(validate_dependency_spec(
            "ext",
            &spec("http://example.com/r.git", "main", None),
            true
        )
        .is_err());
        assert!(validate_dependency_spec(
            "ext",
            &spec("https://example.com/r.git", "--orphan", None),
            true
        )
        .is_err());
        // Subdirectories must stay inside the clone.
        for sub in ["../..", "/etc", "a/../../b", "a\\b", ""] {
            assert!(
                validate_dependency_spec(
                    "ext",
                    &spec("https://example.com/r.git", "main", Some(sub)),
                    true
                )
                .is_err(),
                "subdir {sub:?}"
            );
        }
        // Package names.
        assert!(validate_dependency_spec(
            "dash",
            &spec("https://example.com/r.git", "main", None),
            true
        )
        .is_err());
        assert!(validate_dependency_spec(
            "../x",
            &spec("https://example.com/r.git", "main", None),
            true
        )
        .is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_in_fetched_packages_are_rejected() {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        make_pkg_repo(&repo, "ext", "v1").await;
        let secret = tmp.path().join("vault.key");
        std::fs::write(&secret, b"secret").unwrap();
        std::fs::create_dir_all(repo.join("hello/assets")).unwrap();
        std::os::unix::fs::symlink(&secret, repo.join("hello/assets/k.txt")).unwrap();
        git_commit_all(&repo, "add link", "dev", None)
            .await
            .unwrap();

        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        write_ws_deps(&ws, &[("ext", &repo)]);
        let err = resolve_and_lock_packages(&ws, &data)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("symbolic links"), "{err}");
        let cache = data.join("package_cache");
        if cache.exists() {
            for e in std::fs::read_dir(&cache).unwrap() {
                let p = e.unwrap().path();
                assert!(compute_tree_digest(&p).is_ok());
            }
        }
    }

    #[tokio::test]
    async fn fresh_installation_fetches_locked_commit_not_moving_ref() {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        let pinned = make_pkg_repo(&repo, "ext", "pinned").await;
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        write_ws_deps(&ws, &[("ext", &repo)]);
        let lock = resolve_and_lock_packages(&ws, &data).await.unwrap();
        assert_eq!(lock.packages[0].commit, pinned);
        let lock_raw = std::fs::read_to_string(ws.join("packages.lock")).unwrap();

        // Upstream `main` moves on; the cache is lost (fresh Installation).
        write_marker(&repo, "moved");
        git_commit_all(&repo, "v2", "dev", None).await.unwrap();
        std::fs::remove_dir_all(data.join("package_cache")).unwrap();
        assert_eq!(missing_locked_packages(&data, &lock).len(), 1);
        assert!(cached_package_roots(&data, &lock).is_err());

        let fetched = fetch_locked_packages(&data, &lock).await.unwrap();
        assert_eq!(fetched, vec!["ext".to_string()]);
        let roots = cached_package_roots(&data, &lock).unwrap();
        let rt = std::fs::read_to_string(roots[0].1.join("hello/runtime.luau")).unwrap();
        assert!(
            rt.contains("pinned"),
            "must materialize the pinned commit, got {rt}"
        );
        assert_eq!(
            std::fs::read_to_string(ws.join("packages.lock")).unwrap(),
            lock_raw
        );
        // Nothing left to fetch.
        assert!(fetch_locked_packages(&data, &lock)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn draft_manager_startup_materializes_committed_lock() {
        use dash_core::Principal;
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        make_pkg_repo(&repo, "ext", "pinned").await;
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        write_ws_deps(&ws, &[("ext", &repo)]);
        std::fs::create_dir_all(ws.join("instances")).unwrap();
        std::fs::write(
            ws.join("instances/data.yaml"),
            "data:\n  hello_inst:\n    definition: ext.hello\n",
        )
        .unwrap();
        resolve_and_lock_packages(&ws, &data).await.unwrap();
        std::fs::remove_dir_all(data.join("package_cache")).unwrap();

        let store = dash_store::SqliteStore::open(&data).unwrap();
        let mgr =
            crate::DraftManager::initialize(ws, data, store, dash_luau::ResourceBudget::default())
                .await
                .unwrap();
        let res = mgr
            .engine()
            .queries
            .query(
                "hello_inst",
                serde_json::json!({}),
                &Principal::administrator(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(res.value.unwrap()["v"], "pinned");
    }

    #[tokio::test]
    async fn tampered_cache_fails_integrity_check_and_is_repaired_by_fetch() {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        make_pkg_repo(&repo, "ext", "pinned").await;
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        write_ws_deps(&ws, &[("ext", &repo)]);
        let lock = resolve_and_lock_packages(&ws, &data).await.unwrap();
        let dir = package_cache_dir(&data, &lock.packages[0].tree_digest);
        std::fs::write(dir.join("hello/runtime.luau"), "return { evil = true }").unwrap();
        let err = cached_package_roots(&data, &lock).unwrap_err().to_string();
        assert!(err.contains("integrity"), "{err}");

        fetch_locked_packages(&data, &lock).await.unwrap();
        cached_package_roots(&data, &lock).unwrap();
        // No temporary directories are left behind in the cache.
        for e in std::fs::read_dir(data.join("package_cache")).unwrap() {
            let name = e.unwrap().file_name().to_string_lossy().to_string();
            assert!(!name.starts_with(".tmp-"), "{name}");
        }
    }

    #[tokio::test]
    async fn updating_one_dependency_keeps_other_pins() {
        let tmp = TempDir::new().unwrap();
        let repo_a = tmp.path().join("a");
        let repo_b = tmp.path().join("b");
        let a1 = make_pkg_repo(&repo_a, "alpha", "a1").await;
        let b1 = make_pkg_repo(&repo_b, "beta", "b1").await;
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        write_ws_deps(&ws, &[("alpha", &repo_a), ("beta", &repo_b)]);
        resolve_and_lock_packages(&ws, &data).await.unwrap();

        write_marker(&repo_a, "a2");
        let a2 = git_commit_all(&repo_a, "a2", "dev", None).await.unwrap();
        write_marker(&repo_b, "b2");
        git_commit_all(&repo_b, "b2", "dev", None).await.unwrap();

        let lock = update_package_lock(&ws, &data, "alpha").await.unwrap();
        let commit_of = |n: &str| {
            lock.packages
                .iter()
                .find(|p| p.name == n)
                .unwrap()
                .commit
                .clone()
        };
        assert_eq!(commit_of("alpha"), a2);
        assert_ne!(commit_of("alpha"), a1);
        assert_eq!(commit_of("beta"), b1, "beta must stay pinned");
    }

    #[tokio::test]
    async fn package_name_must_match_dependency_key() {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        make_pkg_repo(&repo, "other", "v1").await;
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        write_ws_deps(&ws, &[("ext", &repo)]);
        let err = resolve_and_lock_packages(&ws, &data)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("differs from the dependency name"), "{err}");
    }

    #[tokio::test]
    async fn conflicting_subdir_requirements_fail() {
        let tmp = TempDir::new().unwrap();
        // A monorepo holding two variants of Package `alpha`.
        let mono = tmp.path().join("mono");
        for sub in ["v1", "v2"] {
            std::fs::create_dir_all(mono.join(sub)).unwrap();
            std::fs::write(mono.join(sub).join("package.toml"), "name = \"alpha\"\n").unwrap();
        }
        ensure_git_repo(&mono, "main").await.unwrap();
        git_commit_all(&mono, "init", "dev", None).await.unwrap();
        // Package `beta` requires alpha from subdir v2.
        let beta = tmp.path().join("beta");
        std::fs::create_dir_all(&beta).unwrap();
        std::fs::write(
            beta.join("package.toml"),
            format!(
                "name = \"beta\"\n[dependencies.alpha]\ngit = \"{}\"\nref = \"main\"\nsubdir = \"v2\"\n",
                mono.display()
            ),
        )
        .unwrap();
        ensure_git_repo(&beta, "main").await.unwrap();
        git_commit_all(&beta, "init", "dev", None).await.unwrap();

        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(
            ws.join("packages.toml"),
            format!(
                "[dependencies.alpha]\ngit = \"{}\"\nref = \"main\"\nsubdir = \"v1\"\n\n[dependencies.beta]\ngit = \"{}\"\nref = \"main\"\n",
                mono.display(),
                beta.display()
            ),
        )
        .unwrap();
        let err = resolve_and_lock_packages(&ws, &data)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("conflicting dependency requirements"), "{err}");
    }

    /// Workspace with Package `ext` locked at `pinned`, one Instance of `ext.hello`, and an
    /// initialized (committed) DraftManager that is dropped again. Returns (tmp, ws, data, repo).
    async fn committed_locked_workspace() -> (TempDir, PathBuf, PathBuf, PathBuf) {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        make_pkg_repo(&repo, "ext", "pinned").await;
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        write_ws_deps(&ws, &[("ext", &repo)]);
        std::fs::create_dir_all(ws.join("instances")).unwrap();
        std::fs::write(
            ws.join("instances/data.yaml"),
            "data:\n  hello_inst:\n    definition: ext.hello\n",
        )
        .unwrap();
        resolve_and_lock_packages(&ws, &data).await.unwrap();
        let store = dash_store::SqliteStore::open(&data).unwrap();
        crate::DraftManager::initialize(
            ws.clone(),
            data.clone(),
            store,
            dash_luau::ResourceBudget::default(),
        )
        .await
        .unwrap();
        (tmp, ws, data, repo)
    }

    async fn start_and_query_hello(ws: &Path, data: &Path) -> serde_json::Value {
        let store = dash_store::SqliteStore::open(data).unwrap();
        let mgr = crate::DraftManager::initialize(
            ws.to_path_buf(),
            data.to_path_buf(),
            store,
            dash_luau::ResourceBudget::default(),
        )
        .await
        .expect("startup materializes the active Revision's locked Packages");
        mgr.engine()
            .queries
            .query(
                "hello_inst",
                serde_json::json!({}),
                &dash_core::Principal::administrator(),
                true,
            )
            .await
            .unwrap()
            .value
            .unwrap()
    }

    /// R3:git-startup-lock-materialization (a): the lock that is materialized is the one of the
    /// Revision being loaded (HEAD), not an uncommitted working-tree edit of packages.lock.
    #[tokio::test]
    async fn startup_materializes_the_lock_of_head_not_the_dirty_working_tree() {
        let (_tmp, ws, data, _repo) = committed_locked_workspace().await;
        std::fs::remove_dir_all(data.join("package_cache")).unwrap();
        // Uncommitted edit: the working-tree lock no longer pins `ext`.
        write_packages_lock(&ws, &PackagesLock::default()).unwrap();
        let v = start_and_query_hello(&ws, &data).await;
        assert_eq!(v["v"], "pinned");
    }

    /// R3:git-startup-lock-materialization (b): a present but corrupt cache entry is repaired at
    /// startup instead of failing the integrity check.
    #[tokio::test]
    async fn startup_repairs_a_corrupt_cache_entry() {
        let (_tmp, ws, data, _repo) = committed_locked_workspace().await;
        let lock = read_packages_lock(&ws).unwrap();
        let dir = package_cache_dir(&data, &lock.packages[0].tree_digest);
        std::fs::write(dir.join("hello/runtime.luau"), "return { evil = true }").unwrap();
        let v = start_and_query_hello(&ws, &data).await;
        assert_eq!(v["v"], "pinned");
        cached_package_roots(&data, &lock).unwrap();
    }

    /// R3:git-dependency-update-partial-write: a failed `dependency_update` leaves the draft's
    /// packages.toml / packages.lock untouched, so later updates are not poisoned.
    #[tokio::test]
    async fn failed_dependency_update_leaves_the_draft_untouched() {
        let (tmp, ws, data, repo) = committed_locked_workspace().await;
        let repo_b = tmp.path().join("b");
        make_pkg_repo(&repo_b, "beta", "b1").await;
        let store = dash_store::SqliteStore::open(&data).unwrap();
        let mgr =
            crate::DraftManager::initialize(ws, data, store, dash_luau::ResourceBudget::default())
                .await
                .unwrap();
        let d = mgr
            .draft_begin(&dash_core::Principal::administrator())
            .await
            .unwrap();
        let wt = PathBuf::from(&d.worktree_path);
        let toml_before = std::fs::read(wt.join("packages.toml")).unwrap();
        let lock_before = std::fs::read(wt.join("packages.lock")).unwrap();

        let err = mgr
            .draft_update_dependency(
                &d.draft_id,
                "ext",
                spec(&repo.to_string_lossy(), "no-such-tag", None),
            )
            .await
            .expect_err("unknown ref must fail");
        assert!(!err.to_string().is_empty());
        assert_eq!(
            std::fs::read(wt.join("packages.toml")).unwrap(),
            toml_before
        );
        assert_eq!(
            std::fs::read(wt.join("packages.lock")).unwrap(),
            lock_before
        );
        assert!(mgr.draft_status(&d.draft_id).unwrap().changes.is_empty());

        // A later, valid update of another dependency is not poisoned by the failed one.
        let (_rec, lock) = mgr
            .draft_update_dependency(
                &d.draft_id,
                "beta",
                spec(&repo_b.to_string_lossy(), "main", None),
            )
            .await
            .expect("valid update after a failed one");
        let names: Vec<&str> = lock.packages.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["beta", "ext"]);
        let cfg = read_packages_toml(&wt).unwrap();
        assert_eq!(cfg.dependencies["ext"].git_ref, "main");
        assert_eq!(read_packages_lock(&wt).unwrap().packages, lock.packages);
        let report = mgr.draft_validate(&d.draft_id).await.unwrap();
        assert!(
            !report.issues.iter().any(|i| i.code == "LOCK_MISMATCH"),
            "{:?}",
            report.issues
        );
    }
}
