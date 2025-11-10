#!/bin/bash
# Script to create a GitHub repository and push the code

REPO_NAME="berkeleyPingPong"
DESCRIPTION="Berkeley Table Tennis Round Robin Tournament Statistics"

# Check if GitHub token is set
if [ -z "$GITHUB_TOKEN" ]; then
    echo "Error: GITHUB_TOKEN environment variable is not set."
    echo ""
    echo "To create a GitHub repository, you need a personal access token."
    echo "1. Go to https://github.com/settings/tokens"
    echo "2. Generate a new token with 'repo' permissions"
    echo "3. Run: export GITHUB_TOKEN=your_token_here"
    echo "4. Then run this script again"
    exit 1
fi

# Get GitHub username from token or API
USERNAME=$(curl -s -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/user | grep -o '"login":"[^"]*' | cut -d'"' -f4)

if [ -z "$USERNAME" ]; then
    echo "Error: Could not authenticate with GitHub. Please check your token."
    exit 1
fi

echo "Creating repository: $USERNAME/$REPO_NAME"

# Create the repository
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Authorization: token $GITHUB_TOKEN" \
    -H "Accept: application/vnd.github.v3+json" \
    https://api.github.com/user/repos \
    -d "{\"name\":\"$REPO_NAME\",\"description\":\"$DESCRIPTION\",\"private\":false}")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" != "201" ]; then
    if echo "$BODY" | grep -q "already exists"; then
        echo "Repository already exists. Adding remote and pushing..."
    else
        echo "Error creating repository:"
        echo "$BODY"
        exit 1
    fi
else
    echo "Repository created successfully!"
fi

# Add remote and push
echo "Adding remote origin..."
git remote remove origin 2>/dev/null
git remote add origin "https://github.com/$USERNAME/$REPO_NAME.git"

echo "Pushing to GitHub..."
git branch -M main
git push -u origin main

echo ""
echo "Repository is now available at: https://github.com/$USERNAME/$REPO_NAME"

