curl -L \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $TOKEN"\
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/RealistAI/UC4_SQL/pulls \
  -d '{"title": "Pull request for the specific branch", "body": "Adding the validated sql files to the UC4 Repo", "head": "'"$BRANCH"'", "base": "master"}';
