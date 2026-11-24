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
        &["commit", "--allow-empty", "-m", "chore: initialize dash workspace"],
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
    let out = run_git(
        dir,
        &["log", &lim, "--pretty=format:%H\t%an\t%aI\t%s"],
    )
    .await?;
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

pub async fn git_remote_url(dir: &Path, remote: &str) -> Option<String> {
    run_git(dir, &["remote", "get-url", remote]).await.ok()
}

pub async fn git_push(dir: &Path, remote: &str, branch: &str) -> Result<(), GitError> {
    run_git(dir, &["push", remote, &format!("HEAD:{branch}")]).await?;
    Ok(())
}

pub async fn git_changed_files_between(
    dir: &Path,
    from_rev: &str,
    to_rev: &str,
) -> Result<Vec<String>, GitError> {
    let range = format!("{from_rev}..{to_rev}");
    let out = run_git(dir, &["diff", "--name-only", &range]).await?;
    Ok(out
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|s| s.to_string())
        .collect())
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
