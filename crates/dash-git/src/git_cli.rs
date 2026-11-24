//! Host-mediated Git CLI wrapper (`GIT_TERMINAL_PROMPT=0`), worktrees, 3-way merge, commit, and push.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::process::Command;

pub fn absolutize_path(path: &Path) -> Result<PathBuf, GitError> {
    if path.exists() {
        return Ok(std::fs::canonicalize(path)?);
    }
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut existing = abs.as_path();
    let mut tail = Vec::new();
    while !existing.exists() {
        if let Some(name) = existing.file_name() {
            tail.push(name.to_os_string());
        } else {
            return Ok(abs);
        }
        match existing.parent() {
            Some(p) => existing = p,
            None => return Ok(abs),
        }
    }
    let mut resolved = std::fs::canonicalize(existing)?;
    for comp in tail.into_iter().rev() {
        resolved.push(comp);
    }
    Ok(resolved)
}

#[derive(Debug, Error)]
pub enum GitError {
    #[error("git command '{cmd}' failed: {stderr}")]
    Command { cmd: String, stderr: String },
    #[error("git I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("rebase conflict on files: {0:?}")]
    MergeConflict(Vec<String>),
    #[error("package dependency error: {0}")]
    Package(String),
    #[error("workspace parse error: {0}")]
    Workspace(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitCommitInfo {
    pub hash: String,
    pub author: String,
    pub timestamp: String,
    pub subject: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStatusReport {
    pub head_commit: Option<String>,
    pub branch: String,
    pub dirty: bool,
    pub changed_files: Vec<String>,
    pub remote_url: Option<String>,
    pub sync_status: String, // synced | unsynced | no_remote
    pub last_error: Option<String>,
    /// Revision of the Generation that is actually running (§12.1). Differs from
    /// `head_commit` when the Workspace HEAD was moved outside a Dash activation.
    #[serde(default)]
    pub active_generation_revision: Option<String>,
    /// `true` when Workspace HEAD is not the running Generation's Revision (external Git
    /// divergence that needs an explicit draft proposal / rebase, §12.3).
    #[serde(default)]
    pub head_diverged: bool,
}

pub async fn run_git(cwd: &Path, args: &[&str]) -> Result<String, GitError> {
    let output = Command::new("git")
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(args)
        .output()
        .await?;
    if !output.status.success() {
        return Err(GitError::Command {
            cmd: format!("git {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub async fn run_git_allow_exit_1(cwd: &Path, args: &[&str]) -> Result<(i32, String), GitError> {
    let output = Command::new("git")
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(args)
        .output()
        .await?;
    let code = output.status.code().unwrap_or(-1);
    Ok((code, String::from_utf8_lossy(&output.stdout).to_string()))
}

pub async fn ensure_git_repo(dir: &Path, branch: &str) -> Result<String, GitError> {
    std::fs::create_dir_all(dir)?;
    if !dir.join(".git").exists() {
        run_git(dir, &["init", "-b", branch]).await?;
        run_git(dir, &["config", "user.name", "dash-engine"]).await?;
        run_git(dir, &["config", "user.email", "dash@localhost"]).await?;
    } else {
        // Ensure local user identity exists for commits
        if run_git(dir, &["config", "user.name"]).await.is_err() {
            let _ = run_git(dir, &["config", "user.name", "dash-engine"]).await;
        }
        if run_git(dir, &["config", "user.email"]).await.is_err() {
            let _ = run_git(dir, &["config", "user.email", "dash@localhost"]).await;
        }
    }

    if let Ok(head) = git_head(dir).await {
        return Ok(head);
    }
    // Initial empty or seed commit if files exist
    run_git(dir, &["add", "-A"]).await?;
    run_git(
        dir,
        &[
            "commit",
            "--allow-empty",
            "-m",
            "chore: initialize dash workspace",
        ],
    )
    .await?;
    git_head(dir).await
}

pub async fn git_head(dir: &Path) -> Result<String, GitError> {
    run_git(dir, &["rev-parse", "HEAD"]).await
}

pub async fn git_commit_all(
    dir: &Path,
    message: &str,
    principal_id: &str,
    draft_id: Option<&str>,
) -> Result<String, GitError> {
    run_git(dir, &["add", "-A"]).await?;
    let status = run_git(dir, &["status", "--porcelain"]).await?;
    if status.trim().is_empty() {
        return git_head(dir).await;
    }
    let mut full_msg = format!("{message}\n\nDash-Principal: {principal_id}");
    if let Some(d) = draft_id {
        full_msg.push_str(&format!("\nDash-Draft: {d}"));
    }
    run_git(dir, &["commit", "-m", &full_msg]).await?;
    git_head(dir).await
}

pub async fn git_history(dir: &Path, limit: usize) -> Result<Vec<GitCommitInfo>, GitError> {
    let lim = format!("-n{limit}");
    let out = run_git(dir, &["log", &lim, "--pretty=format:%H\t%an\t%aI\t%s"]).await?;
    let mut commits = Vec::new();
    for line in out.lines() {
        let parts: Vec<&str> = line.splitn(4, '\t').collect();
        if parts.len() == 4 {
            commits.push(GitCommitInfo {
                hash: parts[0].into(),
                author: parts[1].into(),
                timestamp: parts[2].into(),
                subject: parts[3].into(),
            });
        }
    }
    Ok(commits)
}

/// URL of `remote` with any embedded credentials removed (see [`sanitize_remote_url`]). The
/// result is safe to return from APIs and to persist.
pub async fn git_remote_url(dir: &Path, remote: &str) -> Option<String> {
    git_remote_url_raw(dir, remote)
        .await
        .map(|u| sanitize_remote_url(&u).url)
}

/// URL of `remote` exactly as configured, possibly including credentials. Never return or
/// persist it; use [`git_remote_url`] or [`sanitize_remote_url`].
pub async fn git_remote_url_raw(dir: &Path, remote: &str) -> Option<String> {
    run_git(dir, &["remote", "get-url", remote]).await.ok()
}

/// A remote URL stripped of credentials, plus the credential strings that were removed (so a
/// caller can register them with a redactor and mask Git error output that echoes them).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SanitizedRemoteUrl {
    pub url: String,
    pub removed_secrets: Vec<String>,
}

/// Remove credentials from a Git remote URL (§5.2, §12.1: remote credentials are host secrets
/// and never appear in API responses).
///
/// - `scheme://user:password@host/path?query#fragment` becomes `scheme://host/path`: the whole
///   userinfo is dropped (a lone user component of an HTTPS URL is commonly a token), and so are
///   the query string and fragment, which can carry tokens too.
/// - scp-like `user@host:path` is kept as is: the user there is an SSH login, not a secret.
/// - local paths are returned unchanged.
pub fn sanitize_remote_url(raw: &str) -> SanitizedRemoteUrl {
    let raw = raw.trim();
    let Some(scheme_end) = raw.find("://") else {
        return SanitizedRemoteUrl {
            url: raw.to_string(),
            removed_secrets: vec![],
        };
    };
    let (scheme, rest) = (&raw[..scheme_end], &raw[scheme_end + 3..]);
    let mut removed = Vec::new();
    let (rest, tail) = match rest.find(['?', '#']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    for part in tail.split(['?', '&', '#']).filter(|p| !p.is_empty()) {
        let value = part.split_once('=').map(|(_, v)| v).unwrap_or(part);
        removed.push(value.to_string());
    }
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = (&rest[..authority_end], &rest[authority_end..]);
    let host = match authority.rfind('@') {
        Some(at) => {
            match authority[..at].split_once(':') {
                // `user:password@`: the password is the secret
                Some((_, password)) => removed.push(password.to_string()),
                // `token@`: a lone user component is commonly an access token
                None => removed.push(authority[..at].to_string()),
            }
            &authority[at + 1..]
        }
        None => authority,
    };
    // Git messages can show either the percent-encoded or the decoded form; mask both.
    let decoded: Vec<String> = removed
        .iter()
        .filter_map(|s| percent_decode(s).filter(|d| d != s))
        .collect();
    removed.extend(decoded);
    // Very short fragments are not credentials, and redacting them would mangle other output.
    removed.retain(|s| s.len() >= 4);
    removed.sort();
    removed.dedup();
    SanitizedRemoteUrl {
        url: format!("{scheme}://{host}{path}"),
        removed_secrets: removed,
    }
}

fn percent_decode(s: &str) -> Option<String> {
    if !s.contains('%') {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Default upper bound for one `git push`. A push failure leaves the Revision active and
/// `unsynced` (§12.2 step 8); a hung remote must never stall the daemon indefinitely.
pub const DEFAULT_PUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Push `HEAD` to `<remote>/<branch>` (bounded by [`DEFAULT_PUSH_TIMEOUT`]).
pub async fn git_push(dir: &Path, remote: &str, branch: &str) -> Result<(), GitError> {
    git_push_commit(dir, remote, "HEAD", branch, DEFAULT_PUSH_TIMEOUT).await
}

/// Push exactly `commit` to `refs/heads/<branch>` on `remote`, non-interactively and within
/// `timeout`. SSH runs in batch mode with a connect timeout (unless the operator configured
/// an SSH command), stalled HTTP transfers abort, and the git process is killed when the
/// timeout expires or the caller's future is dropped.
pub async fn git_push_commit(
    dir: &Path,
    remote: &str,
    commit: &str,
    branch: &str,
    timeout: std::time::Duration,
) -> Result<(), GitError> {
    if remote.starts_with('-') || commit.starts_with('-') || branch.starts_with('-') {
        return Err(GitError::Workspace(format!(
            "refusing option-like push arguments '{remote}' '{commit}' '{branch}'"
        )));
    }
    let refspec = format!("{commit}:refs/heads/{branch}");
    let ssh_configured = std::env::var_os("GIT_SSH_COMMAND").is_some()
        || std::env::var_os("GIT_SSH").is_some()
        || run_git(dir, &["config", "--get", "core.sshCommand"])
            .await
            .is_ok_and(|v| !v.is_empty());
    let mut cmd = Command::new("git");
    cmd.current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .args([
            "-c",
            "http.lowSpeedLimit=1000",
            "-c",
            "http.lowSpeedTime=30",
            "push",
            remote,
            &refspec,
        ])
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    if !ssh_configured {
        cmd.env(
            "GIT_SSH_COMMAND",
            "ssh -o BatchMode=yes -o ConnectTimeout=15",
        );
    }
    let cmd_label = format!("git push {remote} {refspec}");
    let output = match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(out) => out?,
        Err(_) => {
            return Err(GitError::Command {
                cmd: cmd_label,
                stderr: format!("push timed out after {}s", timeout.as_secs()),
            })
        }
    };
    if !output.status.success() {
        return Err(GitError::Command {
            cmd: cmd_label,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(())
}

/// Paths changed between two revisions (NUL-separated, so paths are never C-quoted).
pub async fn git_changed_files_between(
    dir: &Path,
    from_rev: &str,
    to_rev: &str,
) -> Result<Vec<String>, GitError> {
    let range = format!("{from_rev}..{to_rev}");
    let out = run_git_raw(dir, &["diff", "--name-only", "-z", &range]).await?;
    out.split(|b| *b == 0)
        .filter(|r| !r.is_empty())
        .map(utf8_path)
        .collect()
}

/// Run git and return its raw, untrimmed stdout, failing on a non-zero exit.
pub async fn run_git_raw(cwd: &Path, args: &[&str]) -> Result<Vec<u8>, GitError> {
    let output = Command::new("git")
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(args)
        .output()
        .await?;
    if !output.status.success() {
        return Err(GitError::Command {
            cmd: format!("git {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(output.stdout)
}

fn utf8_path(raw: &[u8]) -> Result<String, GitError> {
    String::from_utf8(raw.to_vec()).map_err(|_| {
        GitError::Workspace(format!(
            "Workspace path {:?} is not valid UTF-8; rename it so Dash can address it",
            String::from_utf8_lossy(raw)
        ))
    })
}

pub async fn git_show_file(dir: &Path, rev: &str, rel_path: &str) -> Option<Vec<u8>> {
    let spec = format!("{rev}:{rel_path}");
    let output = Command::new("git")
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(["show", &spec])
        .output()
        .await
        .ok()?;
    if output.status.success() {
        Some(output.stdout)
    } else {
        None
    }
}

pub async fn create_worktree(
    repo_dir: &Path,
    worktree_dir: &Path,
    base_commit: &str,
) -> Result<(), GitError> {
    let abs_repo = absolutize_path(repo_dir)?;
    let abs_wt = absolutize_path(worktree_dir)?;
    if let Some(parent) = abs_wt.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let wt_str = abs_wt.to_string_lossy().to_string();
    run_git(
        &abs_repo,
        &["worktree", "add", "--detach", &wt_str, base_commit],
    )
    .await?;
    if !abs_wt.join(".git").exists() {
        return Err(GitError::Workspace(format!(
            "git worktree creation did not populate {}",
            abs_wt.display()
        )));
    }
    Ok(())
}

pub async fn remove_worktree(repo_dir: &Path, worktree_dir: &Path) {
    let abs_repo = absolutize_path(repo_dir).unwrap_or_else(|_| repo_dir.to_path_buf());
    let abs_wt = absolutize_path(worktree_dir).unwrap_or_else(|_| worktree_dir.to_path_buf());
    if abs_wt.exists() {
        let wt_str = abs_wt.to_string_lossy().to_string();
        let _ = run_git(&abs_repo, &["worktree", "remove", "--force", &wt_str]).await;
        let _ = std::fs::remove_dir_all(&abs_wt);
    }
}

pub async fn clone_at_ref(
    source_url: &str,
    git_ref: &str,
    dest_dir: &Path,
) -> Result<String, GitError> {
    if dest_dir.exists() {
        std::fs::remove_dir_all(dest_dir)?;
    }
    if let Some(parent) = dest_dir.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if source_url.starts_with('-') || git_ref.starts_with('-') {
        return Err(GitError::Package(format!(
            "refusing option-like git URL or ref: '{source_url}' '{git_ref}'"
        )));
    }
    let output = Command::new("git")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ALLOW_PROTOCOL", "https:ssh:file")
        .args(["clone", "--", source_url, &dest_dir.to_string_lossy()])
        .output()
        .await?;
    if !output.status.success() {
        return Err(GitError::Command {
            cmd: format!("git clone {source_url}"),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    run_git(dest_dir, &["checkout", git_ref, "--"]).await?;
    git_head(dest_dir).await
}

// ---- Plumbing helpers for transactional activation (§12.2, §12.3) ----

/// Run git with extra environment variables, returning trimmed stdout on success.
pub async fn run_git_env(
    cwd: &Path,
    args: &[&str],
    envs: &[(&str, &str)],
) -> Result<String, GitError> {
    let mut cmd = Command::new("git");
    cmd.current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let output = cmd.output().await?;
    if !output.status.success() {
        return Err(GitError::Command {
            cmd: format!("git {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Run git and return `(exit_code, raw stdout bytes)` without failing on a non-zero exit.
pub async fn run_git_bytes(cwd: &Path, args: &[&str]) -> Result<(i32, Vec<u8>), GitError> {
    let output = Command::new("git")
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(args)
        .output()
        .await?;
    Ok((output.status.code().unwrap_or(-1), output.stdout))
}

/// Tree id of a revision (`<rev>^{tree}`).
pub async fn git_tree_of(dir: &Path, rev: &str) -> Result<String, GitError> {
    run_git(dir, &["rev-parse", &format!("{rev}^{{tree}}")]).await
}

/// Resolve a revision expression to a full commit id (fails if it is not a commit).
pub async fn git_resolve_commit(dir: &Path, rev: &str) -> Result<String, GitError> {
    run_git(
        dir,
        &["rev-parse", "--verify", &format!("{rev}^{{commit}}")],
    )
    .await
}

/// Stage every change of a draft worktree into its own (draft-private) index and return the
/// resulting tree id. The tree id is a content fingerprint of exactly what would be committed,
/// including deletions, and excluding gitignored files.
pub async fn git_stage_all_tree(worktree: &Path) -> Result<String, GitError> {
    run_git(worktree, &["add", "-A"]).await?;
    run_git(worktree, &["write-tree"]).await
}

/// Snapshot the working tree of `dir` as a tree object using a throw-away index, leaving the
/// repository's real index untouched. Deleted tracked files are absent from the snapshot and
/// gitignored files are excluded.
pub async fn git_snapshot_worktree_tree(dir: &Path) -> Result<String, GitError> {
    let tmp = tempfile::tempdir()?;
    let idx = tmp.path().join("index");
    let idx_s = idx.to_string_lossy().to_string();
    let envs = [("GIT_INDEX_FILE", idx_s.as_str())];
    if git_head(dir).await.is_ok() {
        run_git_env(dir, &["read-tree", "HEAD"], &envs).await?;
    }
    run_git_env(dir, &["add", "-A"], &envs).await?;
    run_git_env(dir, &["write-tree"], &envs).await
}

/// Materialize the content of `rev` into `dest` (a plain directory, not a git worktree) using
/// a throw-away index. Used to load a Generation exactly from its Revision.
pub async fn git_export_tree(repo: &Path, rev: &str, dest: &Path) -> Result<(), GitError> {
    std::fs::create_dir_all(dest)?;
    let tmp = tempfile::tempdir()?;
    let idx = tmp.path().join("index");
    let idx_s = idx.to_string_lossy().to_string();
    let dest_abs = absolutize_path(dest)?;
    let dest_s = dest_abs.to_string_lossy().to_string();
    run_git_env(
        repo,
        &["--work-tree", &dest_s, "read-tree", "--reset", "-u", rev],
        &[("GIT_INDEX_FILE", idx_s.as_str())],
    )
    .await?;
    Ok(())
}

/// Tracked-file modifications (staged or unstaged) in `dir`, excluding untracked files.
pub async fn git_tracked_changes(dir: &Path) -> Result<Vec<String>, GitError> {
    let out = run_git(dir, &["status", "--porcelain", "--untracked-files=no"]).await?;
    Ok(out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|s| s.trim().to_string())
        .collect())
}

/// Create a commit object for `tree` with parent `parent` without moving any ref.
/// `author` / `committer` are `(name, email)` pairs recorded for attribution.
pub async fn git_commit_tree(
    dir: &Path,
    tree: &str,
    parent: &str,
    message: &str,
    author: (&str, &str),
    committer: (&str, &str),
) -> Result<String, GitError> {
    run_git_env(
        dir,
        &["commit-tree", tree, "-p", parent, "-m", message],
        &[
            ("GIT_AUTHOR_NAME", author.0),
            ("GIT_AUTHOR_EMAIL", author.1),
            ("GIT_COMMITTER_NAME", committer.0),
            ("GIT_COMMITTER_EMAIL", committer.1),
        ],
    )
    .await
}

/// Atomically move `HEAD` (and the branch it points to) from `expected_old` to `new`.
/// Fails without changing anything if HEAD is no longer `expected_old`.
pub async fn git_update_head(
    dir: &Path,
    new: &str,
    expected_old: &str,
    reason: &str,
) -> Result<(), GitError> {
    run_git(
        dir,
        &["update-ref", "-m", reason, "HEAD", new, expected_old],
    )
    .await?;
    Ok(())
}

/// Two-tree fast-forward of index + working tree from `from` to `to`. Refuses (and changes
/// nothing) when local modifications or untracked files would be overwritten.
pub async fn git_read_tree_fast_forward(dir: &Path, from: &str, to: &str) -> Result<(), GitError> {
    // Refresh stat info first so files that were merely touched are not reported "not uptodate"
    let _ = run_git_allow_exit_1(dir, &["update-index", "-q", "--refresh"]).await;
    run_git(dir, &["read-tree", "-m", "-u", from, to]).await?;
    Ok(())
}

/// Replace only the index with `rev` (working tree untouched, like `git reset --mixed`).
pub async fn git_read_tree_index_only(dir: &Path, rev: &str) -> Result<(), GitError> {
    run_git(dir, &["read-tree", rev]).await?;
    Ok(())
}

/// Force index + working tree of tracked paths to `rev` (used only for crash repair of a
/// working-tree switch Dash itself was performing).
pub async fn git_read_tree_reset(dir: &Path, rev: &str) -> Result<(), GitError> {
    run_git(dir, &["read-tree", "--reset", "-u", rev]).await?;
    Ok(())
}

/// Tree id of the current index.
pub async fn git_index_tree(dir: &Path) -> Result<String, GitError> {
    run_git(dir, &["write-tree"]).await
}

/// Name/status diff between two tree-ish objects: `(status letter, path)`.
pub async fn git_diff_name_status(
    dir: &Path,
    from: &str,
    to: &str,
) -> Result<Vec<(char, String)>, GitError> {
    // `-z`: records are `<status>NUL<path>NUL` with paths verbatim. Without it Git C-quotes
    // paths with non-ASCII bytes, quotes, backslashes or control characters (core.quotePath),
    // and the quoted form would then be used as if it were the real file name.
    let out = run_git_raw(
        dir,
        &["diff", "--no-renames", "--name-status", "-z", from, to],
    )
    .await?;
    let mut fields = out.split(|b| *b == 0);
    let mut entries = Vec::new();
    while let Some(status) = fields.next() {
        let Some(&st) = status.first() else {
            continue;
        };
        let path = fields.next().filter(|p| !p.is_empty()).ok_or_else(|| {
            GitError::Workspace(format!(
                "malformed `git diff --name-status -z` output: status {:?} without a path",
                String::from_utf8_lossy(status)
            ))
        })?;
        entries.push((st as char, utf8_path(path)?));
    }
    Ok(entries)
}

/// Full commit message of `rev`.
pub async fn git_commit_message(dir: &Path, rev: &str) -> Result<String, GitError> {
    run_git(dir, &["log", "-1", "--format=%B", rev]).await
}

/// Value of the first `key: value` trailer line in a commit message.
pub fn commit_trailer(message: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}: ");
    message
        .lines()
        .find_map(|l| l.strip_prefix(&prefix).map(|v| v.trim().to_string()))
}

/// `true` when `ancestor` is reachable from (or equal to) `descendant`.
pub async fn git_is_ancestor(dir: &Path, ancestor: &str, descendant: &str) -> bool {
    run_git(dir, &["merge-base", "--is-ancestor", ancestor, descendant])
        .await
        .is_ok()
}

/// Last known remote-tracking commit for `<remote>/<branch>`, if any (no network access).
pub async fn git_remote_tracking_commit(dir: &Path, remote: &str, branch: &str) -> Option<String> {
    run_git(
        dir,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("refs/remotes/{remote}/{branch}^{{commit}}"),
        ],
    )
    .await
    .ok()
    .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod plumbing_tests {
    use super::*;

    async fn repo_with_commit(dir: &Path) -> String {
        std::fs::write(dir.join("a.txt"), "a1\n").unwrap();
        std::fs::write(dir.join("b.txt"), "b1\n").unwrap();
        ensure_git_repo(dir, "main").await.unwrap()
    }

    #[tokio::test]
    async fn snapshot_tree_includes_deletions_and_keeps_real_index() {
        let tmp = tempfile::tempdir().unwrap();
        let head = repo_with_commit(tmp.path()).await;
        let head_tree = git_tree_of(tmp.path(), &head).await.unwrap();
        assert_eq!(
            git_snapshot_worktree_tree(tmp.path()).await.unwrap(),
            head_tree
        );

        std::fs::remove_file(tmp.path().join("b.txt")).unwrap();
        let snap = git_snapshot_worktree_tree(tmp.path()).await.unwrap();
        assert_ne!(snap, head_tree);
        let diff = git_diff_name_status(tmp.path(), &head_tree, &snap)
            .await
            .unwrap();
        assert_eq!(diff, vec![('D', "b.txt".to_string())]);
        // The real index still matches HEAD (nothing was staged by the snapshot)
        assert_eq!(git_index_tree(tmp.path()).await.unwrap(), head_tree);
    }

    #[tokio::test]
    async fn commit_tree_update_head_and_fast_forward_keep_ignored_files() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        std::fs::create_dir_all(&ws).unwrap();
        let base = repo_with_commit(&ws).await;
        std::fs::write(ws.join(".gitignore"), "*.local\n").unwrap();
        run_git(&ws, &["add", ".gitignore"]).await.unwrap();
        run_git(&ws, &["commit", "-m", "ignore"]).await.unwrap();
        std::fs::write(ws.join("ignored.local"), "keep me").unwrap();
        let base2 = git_head(&ws).await.unwrap();
        assert_ne!(base, base2);

        let wt = tmp.path().join("wt");
        create_worktree(&ws, &wt, &base2).await.unwrap();
        std::fs::write(wt.join("a.txt"), "a2\n").unwrap();
        std::fs::remove_file(wt.join("b.txt")).unwrap();
        let tree = git_stage_all_tree(&wt).await.unwrap();
        let c = git_commit_tree(
            &wt,
            &tree,
            &base2,
            "msg\n\nDash-Author: agt_1",
            ("agt_1", "agt_1@dash.local"),
            ("admin", "admin@dash.local"),
        )
        .await
        .unwrap();
        // A stale expected-old value is refused without moving HEAD
        assert!(git_update_head(&ws, &c, &base, "x").await.is_err());
        assert_eq!(git_head(&ws).await.unwrap(), base2);
        git_update_head(&ws, &c, &base2, "activate").await.unwrap();
        git_read_tree_fast_forward(&ws, &base2, &c).await.unwrap();
        assert_eq!(std::fs::read_to_string(ws.join("a.txt")).unwrap(), "a2\n");
        assert!(!ws.join("b.txt").exists());
        assert_eq!(
            std::fs::read_to_string(ws.join("ignored.local")).unwrap(),
            "keep me"
        );
        assert!(git_tracked_changes(&ws).await.unwrap().is_empty());
        let msg = git_commit_message(&ws, "HEAD").await.unwrap();
        assert_eq!(
            commit_trailer(&msg, "Dash-Author").as_deref(),
            Some("agt_1")
        );
        let hist = git_history(&ws, 1).await.unwrap();
        assert_eq!(hist[0].author, "agt_1");
    }

    #[tokio::test]
    async fn diffs_report_real_paths_for_names_git_would_quote() {
        let tmp = tempfile::tempdir().unwrap();
        let head = repo_with_commit(tmp.path()).await;
        let names = [
            "übersicht.yaml",
            "tab\there.txt",
            "quote\"d.txt",
            "back\\slash.txt",
        ];
        for n in names {
            std::fs::write(tmp.path().join(n), "x\n").unwrap();
        }
        let snap = git_snapshot_worktree_tree(tmp.path()).await.unwrap();
        let head_tree = git_tree_of(tmp.path(), &head).await.unwrap();
        let mut diff = git_diff_name_status(tmp.path(), &head_tree, &snap)
            .await
            .unwrap();
        diff.sort();
        let mut expected: Vec<(char, String)> =
            names.iter().map(|n| ('A', n.to_string())).collect();
        expected.sort();
        assert_eq!(diff, expected);
        run_git(tmp.path(), &["add", "-A"]).await.unwrap();
        run_git(tmp.path(), &["commit", "-m", "names"])
            .await
            .unwrap();
        let mut changed = git_changed_files_between(tmp.path(), &head, "HEAD")
            .await
            .unwrap();
        changed.sort();
        let mut expected: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        expected.sort();
        assert_eq!(changed, expected);
        assert_eq!(
            git_show_file(tmp.path(), "HEAD", "übersicht.yaml").await,
            Some(b"x\n".to_vec())
        );
    }

    #[test]
    fn remote_urls_are_stripped_of_credentials() {
        let s = sanitize_remote_url("https://x-access-token:ghp_abc123XYZ@github.com/acme/ws.git");
        assert_eq!(s.url, "https://github.com/acme/ws.git");
        assert_eq!(s.removed_secrets, vec!["ghp_abc123XYZ".to_string()]);

        let s = sanitize_remote_url("https://ghp_tokenonly@github.com/acme/ws.git");
        assert_eq!(s.url, "https://github.com/acme/ws.git");
        assert_eq!(s.removed_secrets, vec!["ghp_tokenonly".to_string()]);

        let s = sanitize_remote_url(
            "https://user:p%40ss%2Fword@host:8443/r.git?private_token=tok_123#frag",
        );
        assert_eq!(s.url, "https://host:8443/r.git");
        assert!(s.removed_secrets.contains(&"p%40ss%2Fword".to_string()));
        assert!(s.removed_secrets.contains(&"p@ss/word".to_string()));
        assert!(s.removed_secrets.contains(&"tok_123".to_string()));

        for kept in [
            "git@github.com:acme/ws.git",
            "ssh://github.com/acme/ws.git",
            "/srv/git/ws.git",
            "file:///srv/git/ws.git",
        ] {
            let s = sanitize_remote_url(kept);
            assert_eq!(s.url, kept);
            assert!(s.removed_secrets.is_empty());
        }
    }

    #[tokio::test]
    async fn push_is_bounded_by_its_timeout() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        std::fs::create_dir_all(&ws).unwrap();
        repo_with_commit(&ws).await;
        let hang = tmp.path().join("hang.sh");
        std::fs::write(&hang, "#!/bin/sh\nsleep 30\n").unwrap();
        std::process::Command::new("chmod")
            .args(["+x", &hang.to_string_lossy()])
            .status()
            .unwrap();
        run_git(
            &ws,
            &["remote", "add", "origin", "ssh://git@example.invalid/r.git"],
        )
        .await
        .unwrap();
        run_git(&ws, &["config", "core.sshCommand", &hang.to_string_lossy()])
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let err = git_push_commit(
            &ws,
            "origin",
            "HEAD",
            "main",
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("timed out"), "{err}");
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        assert!(
            git_push_commit(&ws, "--upload-pack=x", "HEAD", "main", DEFAULT_PUSH_TIMEOUT)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn export_tree_materializes_revision_without_touching_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        std::fs::create_dir_all(&ws).unwrap();
        let head = repo_with_commit(&ws).await;
        std::fs::write(ws.join("a.txt"), "dirty\n").unwrap();
        let out = tmp.path().join("out");
        git_export_tree(&ws, &head, &out).await.unwrap();
        assert_eq!(std::fs::read_to_string(out.join("a.txt")).unwrap(), "a1\n");
        assert!(!out.join(".git").exists());
        assert_eq!(
            std::fs::read_to_string(ws.join("a.txt")).unwrap(),
            "dirty\n"
        );
        assert_eq!(git_tracked_changes(&ws).await.unwrap().len(), 1);
    }
}
