//! Per-session workspace allocation. Default is one git worktree per session,
//! branched off the fleet's `base_branch`.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::OrchestratorError;

/// Allocates and cleans up a workspace per session.
pub trait WorktreeManager: Send + Sync {
    /// Create (or reuse) the workspace for `session`, returning its path.
    fn create(&self, session: &str, base_branch: &str) -> Result<PathBuf, OrchestratorError>;
    /// Remove the workspace for `session`. Idempotent.
    fn cleanup(&self, session: &str) -> Result<(), OrchestratorError>;
}

/// Real implementation: one git worktree at `<root>/.cap/<session>` on branch `cap/<session>`.
#[derive(Debug, Clone)]
pub struct GitWorktreeManager {
    repo: PathBuf,
}

impl GitWorktreeManager {
    pub fn new(repo: impl AsRef<Path>) -> Self {
        Self {
            repo: repo.as_ref().to_path_buf(),
        }
    }

    fn dir_for(&self, session: &str) -> PathBuf {
        self.repo.join(".cap").join(session)
    }

    fn git(&self, args: &[&str]) -> Result<(), OrchestratorError> {
        let out = Command::new("git")
            .args(args)
            .current_dir(&self.repo)
            .output()
            .map_err(|e| OrchestratorError::Worktree(format!("spawning git failed: {e}")))?;
        if !out.status.success() {
            return Err(OrchestratorError::Worktree(format!(
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(())
    }
}

impl WorktreeManager for GitWorktreeManager {
    fn create(&self, session: &str, base_branch: &str) -> Result<PathBuf, OrchestratorError> {
        if !crate::config::valid_session_id(session) {
            return Err(OrchestratorError::Config(format!(
                "invalid session name '{session}' — rejected by worktree manager"
            )));
        }
        // Auto-init if not a git repo
        if !self.repo.join(".git").exists() {
            self.git(&["init", "-q", "-b", base_branch])?;
            let gitignore = self.repo.join(".gitignore");
            if !gitignore.exists() {
                std::fs::write(&gitignore, b".cap/\n").ok();
            }
            self.git(&["add", "."])?;
            self.git(&["commit", "-qm", "init"])?;
        }
        let dir = self.dir_for(session);
        if dir.join(".git").exists() {
            return Ok(dir);
        }
        let dir_str = dir.to_string_lossy().to_string();
        let branch = format!("cap/{session}");
        let branch_ref = format!("refs/heads/{branch}");
        if self.git(&["rev-parse", "--verify", &branch_ref]).is_ok() {
            self.git(&["worktree", "add", &dir_str, &branch])?;
        } else {
            self.git(&["worktree", "add", "-b", &branch, &dir_str, base_branch])?;
        }
        Ok(dir)
    }

    fn cleanup(&self, session: &str) -> Result<(), OrchestratorError> {
        let dir = self.dir_for(session);
        let dir_str = dir.to_string_lossy().to_string();
        let _ = self.git(&["worktree", "remove", "--force", &dir_str]);
        Ok(())
    }
}

/// Test/dev implementation: a throwaway temp dir per session, no git.
#[derive(Debug)]
pub struct NoopWorktreeManager {
    root: tempfile::TempDir,
}

impl NoopWorktreeManager {
    pub fn new() -> Self {
        Self {
            root: tempfile::tempdir().expect("create temp dir"),
        }
    }
}

impl Default for NoopWorktreeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WorktreeManager for NoopWorktreeManager {
    fn create(&self, session: &str, _base_branch: &str) -> Result<PathBuf, OrchestratorError> {
        let dir = self.root.path().join(session);
        std::fs::create_dir_all(&dir).map_err(|e| OrchestratorError::Worktree(e.to_string()))?;
        Ok(dir)
    }

    fn cleanup(&self, _session: &str) -> Result<(), OrchestratorError> {
        Ok(()) // TempDir cleans itself on drop.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_repo() -> tempfile::TempDir {
        let repo = tempfile::tempdir().unwrap();
        run_git(repo.path(), &["init", "-q", "-b", "main"]);
        run_git(repo.path(), &["config", "user.email", "t@t"]);
        run_git(repo.path(), &["config", "user.name", "t"]);
        std::fs::write(repo.path().join("f.txt"), "x").unwrap();
        run_git(repo.path(), &["add", "."]);
        run_git(repo.path(), &["commit", "-qm", "init"]);
        repo
    }

    fn run_git(cwd: &Path, args: &[&str]) {
        let ok = std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .status()
            .unwrap()
            .success();
        assert!(ok, "git {args:?} failed");
    }

    #[test]
    fn noop_returns_distinct_dirs_per_session() {
        let wt = NoopWorktreeManager::new();
        let a = wt.create("a", "main").unwrap();
        let b = wt.create("b", "main").unwrap();
        assert!(a.exists());
        assert!(b.exists());
        assert_ne!(a, b);
        wt.cleanup("a").unwrap();
    }

    #[test]
    fn git_creates_a_worktree_off_base_branch() {
        let repo = init_repo();

        let wt = GitWorktreeManager::new(repo.path());
        let dir = wt.create("worker", "main").unwrap();
        assert!(
            dir.join("f.txt").exists(),
            "worktree should contain repo files"
        );
        wt.cleanup("worker").unwrap();
        assert!(!dir.exists(), "cleanup should remove the worktree dir");
    }

    #[test]
    fn git_reuses_session_branch_after_cleanup() {
        let repo = init_repo();

        let wt = GitWorktreeManager::new(repo.path());
        let first = wt.create("worker", "main").unwrap();
        wt.cleanup("worker").unwrap();

        let second = wt.create("worker", "main").unwrap();

        assert_eq!(first, second);
        assert!(second.join("f.txt").exists());
    }

    #[test]
    fn git_reuses_live_session_worktree() {
        let repo = init_repo();

        let wt = GitWorktreeManager::new(repo.path());
        let first = wt.create("worker", "main").unwrap();

        let second = wt.create("worker", "main").unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn git_reuses_existing_session_branch_without_deleting_commits() {
        let repo = init_repo();

        let wt = GitWorktreeManager::new(repo.path());
        let first = wt.create("worker", "main").unwrap();
        std::fs::write(first.join("state.txt"), "preserved").unwrap();
        run_git(&first, &["add", "state.txt"]);
        run_git(&first, &["commit", "-qm", "preserve worker state"]);
        wt.cleanup("worker").unwrap();

        let second = wt.create("worker", "main").unwrap();

        assert_eq!(
            std::fs::read_to_string(second.join("state.txt")).unwrap(),
            "preserved"
        );
    }
}
