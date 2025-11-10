# Push to GitHub Instructions

Your code has been committed locally. To push to GitHub:

## Step 1: Create the repository on GitHub

1. Go to https://github.com/new
2. Repository name: `berkeleyPingPong`
3. Description: "Berkeley Table Tennis Round Robin Tournament Statistics"
4. Choose Public or Private
5. **Do NOT** check "Initialize with README"
6. Click "Create repository"

## Step 2: Push your code

After creating the repository, GitHub will show you commands. Use these:

```bash
git remote add origin https://github.com/YOUR_USERNAME/berkeleyPingPong.git
git branch -M main
git push -u origin main
```

Replace `YOUR_USERNAME` with your GitHub username.

## Alternative: Use the automated script

If you have a GitHub personal access token:

1. Get a token from https://github.com/settings/tokens (needs 'repo' permission)
2. Run:
   ```bash
   export GITHUB_TOKEN=your_token_here
   ./create_github_repo.sh
   ```

