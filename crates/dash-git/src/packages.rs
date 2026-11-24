//! Git Package resolution, transitive lockfile (`packages.lock`), cycle/conflict detection,
//! and offline `<data_dir>/package_cache/<tree_digest>` caching (§4.4, §12.1).

use crate::git_cli::{clone_at_ref, GitError};
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

/// Resolve all external Git dependencies declared in `packages.toml` (transitively),
/// detect cycles and version conflicts, cache fetched trees in `<data_dir>/package_cache/<tree_digest>`,
/// and write deterministic `packages.lock`.
pub async fn resolve_and_lock_packages(
    workspace_dir: &Path,
    data_dir: &Path,
) -> Result<PackagesLock, GitError> {
    let manifest = read_packages_toml(workspace_dir)?;
    let mut resolved: BTreeMap<String, LockedPackage> = BTreeMap::new();
    let mut visiting = BTreeSet::new();
    let mut stack = Vec::new();

    for (name, spec) in &manifest.dependencies {
        resolve_recursive(
            name,
            spec,
            data_dir,
            &mut visiting,
            &mut stack,
            &mut resolved,
        )
        .await?;
    }

    let lock = PackagesLock {
        version: 1,
        packages: resolved.into_values().collect(),
    };
    write_packages_lock(workspace_dir, &lock)?;
    Ok(lock)
}

fn resolve_recursive<'a>(
    name: &'a str,
    spec: &'a PackageDependencySpec,
    data_dir: &'a Path,
    visiting: &'a mut BTreeSet<String>,
    stack: &'a mut Vec<String>,
    resolved: &'a mut BTreeMap<String, LockedPackage>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), GitError>> + Send + 'a>> {
    Box::pin(async move {
        if visiting.contains(name) {
            let mut cyc = stack.clone();
            cyc.push(name.to_string());
            return Err(GitError::Package(format!(
                "package dependency cycle detected: {}",
                cyc.join(" -> ")
            )));
        }
        if let Some(existing) = resolved.get(name) {
            if existing.git != spec.git || existing.requested_ref != spec.git_ref {
                return Err(GitError::Package(format!(
                    "conflicting dependency requirements for '{name}': ({}, {}) vs ({}, {})",
                    existing.git, existing.requested_ref, spec.git, spec.git_ref
                )));
            }
            return Ok(());
        }

        visiting.insert(name.to_string());
        stack.push(name.to_string());

        let tmp_dir = tempfile::tempdir()?;
        let commit = clone_at_ref(&spec.git, &spec.git_ref, tmp_dir.path()).await?;

        let src_root = match &spec.subdir {
            Some(sub) => tmp_dir.path().join(sub),
            None => tmp_dir.path().to_path_buf(),
        };
        if !src_root.exists() {
            return Err(GitError::Package(format!(
                "package '{name}' missing subdir '{}'",
                src_root.display()
            )));
        }

        let tree_digest = compute_tree_digest(&src_root)?;
        let cache_dir = package_cache_dir(data_dir, &tree_digest);
        if !cache_dir.exists() {
            copy_tree_excluding_git(&src_root, &cache_dir)?;
        }

        let pkg_toml_path = cache_dir.join("package.toml");
        let sub_deps = if pkg_toml_path.exists() {
            let raw = std::fs::read_to_string(&pkg_toml_path)?;
            let parsed: PackageManifestToml = toml::from_str(&raw)
                .map_err(|e| GitError::Package(format!("invalid package.toml in '{name}': {e}")))?;
            parsed.dependencies
        } else {
            BTreeMap::new()
        };

        let dep_names: Vec<String> = sub_deps.keys().cloned().collect();
        for (dep_name, dep_spec) in &sub_deps {
            resolve_recursive(dep_name, dep_spec, data_dir, visiting, stack, resolved).await?;
        }

        stack.pop();
        visiting.remove(name);

        resolved.insert(
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

pub fn package_cache_dir(data_dir: &Path, tree_digest: &str) -> PathBuf {
    let clean = tree_digest.strip_prefix("sha256:").unwrap_or(tree_digest);
    data_dir.join("package_cache").join(clean)
}

/// Ensure all locked packages are available locally from cache (failing if uncached offline,
/// NEVER silently fetching a moving branch without an explicit lockfile update).
pub fn cached_package_roots(
    data_dir: &Path,
    lock: &PackagesLock,
) -> Result<Vec<(String, PathBuf)>, GitError> {
    let mut out = Vec::new();
    for pkg in &lock.packages {
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
        out.push((pkg.name.clone(), dir));
    }
    Ok(out)
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
        if path.is_dir() {
            collect_files_rel(base, &path, out)?;
        } else if path.is_file() {
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
        if src_p.is_dir() {
            copy_tree_excluding_git(&src_p, &dst_p)?;
        } else if src_p.is_file() {
            std::fs::copy(&src_p, &dst_p)?;
        }
    }
    Ok(())
}
