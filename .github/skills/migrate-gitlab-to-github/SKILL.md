---
name: migrate-gitlab-to-github
description: Migrate a Git repository from GitLab to GitHub, handling remote URL changes, merge conflicts, and common pitfalls. Use when: moving projects from GitLab to GitHub, resolving push rejections, or handling unrelated histories.
---

# Migrate GitLab to GitHub Skill

This skill automates the process of migrating a Git repository from GitLab to GitHub, including handling common issues like push rejections and merge conflicts.

## Workflow Steps

1. **Check current remote**: Run `git remote -v` to confirm current remote URL.
2. **Get GitHub repo URL**: Ask user for the new GitHub repository URL.
3. **Change remote URL**: Run `git remote set-url origin <github_url>`.
4. **Check current branch**: Run `git branch` to identify the branch to push.
5. **Attempt push**: Run `git push -u origin <branch>`.
6. **Handle push failure**: If rejected due to remote content:
   - Run `git pull origin <branch> --allow-unrelated-histories`.
   - Resolve any merge conflicts.
7. **Resolve conflicts**: For each conflicted file:
   - Choose version: `git checkout --ours <file>` (keep local) or `git checkout --theirs <file>` (keep remote).
   - Add resolved files: `git add <file>`.
8. **Commit merge**: Run `git commit -m "Merge from GitLab to GitHub"`.
9. **Final push**: Run `git push origin <branch>`.

## Common Pitfalls and Solutions

### Pitfall 1: Push Rejected - Remote Contains Work
**Error**: `! [rejected] main -> main (fetch first)`
**Cause**: GitHub repo has initial files (README, .gitignore) that local doesn't have.
**Solution**: Pull with `--allow-unrelated-histories` before pushing.

### Pitfall 2: Merge Conflicts
**Error**: `Automatic merge failed; fix conflicts`
**Cause**: Same files modified in both histories (e.g., README.md).
**Solution**: 
- Choose version using `checkout --ours` or `--theirs`.
- Edit manually if needed.
- Add and commit.

### Pitfall 3: Unrelated Histories
**Error**: `fatal: refusing to merge unrelated histories`
**Cause**: GitLab and GitHub repos have no common commits.
**Solution**: Use `--allow-unrelated-histories` flag in pull.

### Pitfall 4: Authentication Issues
**Cause**: GitHub requires different credentials than GitLab.
**Solution**: Ensure SSH keys or personal access tokens are configured for GitHub.

### Pitfall 5: Branch Name Mismatch
**Cause**: GitLab uses `master`, GitHub uses `main`.
**Solution**: Check branch name with `git branch` before pushing.

## Post-Migration Checklist

- ✅ Remote URL updated to GitHub
- ✅ All commits pushed
- ✅ No merge conflicts remaining
- ✅ Branch tracking set up (`-u` flag)
- ✅ Test clone from GitHub to verify

## Usage Notes

- Always backup the repository before migration.
- If the GitHub repo is not empty, expect conflicts.
- For large repositories, consider using GitHub's import tool instead.
- Update any CI/CD pipelines to point to the new GitHub URL.