cp package.json _package.json
preset=`conventional-commits-detector` && echo $preset
bump=`conventional-recommended-bump -p $preset` && echo $bump
npm --no-git-tag-version version $bump &>/dev/null
conventional-changelog -i CHANGELOG.md -s -p $preset
npm run validate
# npm run doc <-- todo: fix esdoc gen (markdown comment issues)
git add CHANGELOG.md && version=$(json -f package.json version)
git commit -m "docs(CHANGELOG): $version"
mv -f _package.json package.json
npm version $bump -m "chore(release): %s"
git push --follow-tags
conventional-github-releaser -p $preset
npm run lerna:publish