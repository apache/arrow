# R CRAN Release

Guide the R package maintainer through the CRAN release process for the Apache Arrow R package. Only use this skill when explicitly doing a CRAN release.

Use exactly this sequence of steps. Use a checklist as you go. Do not skip ahead, and ask the user to confirm step completion.

## 1. Create GitHub Tracking Issue

Ask the user for the release version number.

```bash
gh issue create --repo apache/arrow \
  --title "[R] CRAN packaging checklist for version <VERSION>" \
  --body "$(cat r/PACKAGING.md | sed -n '/^- \[ \]/,$p')"
```

Track the issue number - update checkboxes as you complete each step.

## 2. Create CRAN Release Branch

Ask the user which RC number to use (e.g., rc1, rc2), then confirm before creating and pushing.

```bash
git fetch upstream
git checkout apache-arrow-<VERSION>-rc<N>
git checkout -b maint-<VERSION>-r
git push upstream maint-<VERSION>-r
```

All subsequent steps should be done on this branch.

## 3. Remove Badges from README

In `r/README.md`, delete everything between `<!-- badges: start -->` and `<!-- badges: end -->` (inclusive):

```bash
sed -i '/<!-- badges: start -->/,/<!-- badges: end -->/d' r/README.md
```

## 4. Review Deprecated Functions

Find functions using `.Deprecated()` that may need to advance (deprecated -> defunct/removed):

```bash
grep -rn "\.Deprecated" r/R/*.R
```

Review each match and decide if the deprecation should advance for this release (e.g., remove the function entirely or change to `.Defunct()`).

## 5. Evaluate Nightly Build Status

First, find the release candidate date:

```bash
gh api repos/apache/arrow/releases --jq '.[] | select(.tag_name | contains("<VERSION>-rc")) | {tag_name, created_at}'
```

Then check R nightly builds were passing at that time:

1. Go to https://github.com/ursacomputing/crossbow/actions
2. Click the "Logs" tab
3. Filter by the RC date
4. Filter by task status: failure
5. Look for any failing R jobs (jobs starting with `test-r-` or `r-`)

All R jobs should have been passing at RC time. If any were failing, investigate whether they would cause CRAN rejection.

For failed job logs, search: https://github.com/ursacomputing/crossbow/branches/all?query=<job-name>

## 6. Check Current CRAN Check Results

Fetch https://cran.r-project.org/web/checks/check_results_arrow.html and extract the check results table showing platform, version, and status. Also check for any "Additional issues" section.

All platforms should show OK or NOTE status. NOTEs about package size (e.g., "installed size is 130+ Mb") are expected due to bundled Arrow C++ and can be ignored. Other NOTEs or any ERROR/WARN should be investigated.

## 7. Ensure README is Accurate

Read `r/README.md` and verify:
- Installation instructions are current
- Feature descriptions match current functionality
- Version-specific notes (e.g., C++ version requirements) are correct
- No outdated information

Report any issues found.

## 8. Run URL Checker

Confirm on the `maint-<VERSION>-r` branch:

```bash
git branch --show-current
```

Then run:

```bash
cd r && Rscript -e 'urlchecker::url_check()'
```

All URLs should pass (badges were already removed). Fix any broken links.

## 9. Polish NEWS

Review `r/NEWS.md` and polish following tidyverse style:

- Use present tense ("X now does Y", not "X did Y")
- Name contributors with `@username` if they're not a listed package author
- Use categories: "New features", "Minor improvements and fixes", "Installation" (if relevant)
- Keep entries concise - match the style of previous releases
- Only include user-facing changes - no CI updates or internal refactoring

Find the previous version from NEWS.md:

```bash
grep "^# arrow" r/NEWS.md | head -5
```

Then find R commits since that version:

```bash
git log --oneline apache-arrow-<PREVIOUS_VERSION>..HEAD | grep "\[R\]"
```

Do NOT update version numbers - this is done automatically later.

Open a GitHub issue for the NEWS updates, submit a PR to main, then cherry-pick into the `maint-<VERSION>-r` branch later.

## 10. Cherry-pick Necessary Changes

Check if there are any fixes that need to be cherry-picked into the `maint-<VERSION>-r` branch:

1. Check the comments on the release tracking issue for any noted cherry-picks
2. Ask if there are any other fixes merged to main after the RC

Common reasons to cherry-pick:
- Fixes for CRAN check failures identified in earlier steps
- NEWS updates (from step 9)
- Critical bug fixes

For each PR noted, get the merge commit SHA:

```bash
gh pr view <PR_NUMBER> --repo apache/arrow --json mergeCommit,title --jq '{sha: .mergeCommit.oid, title: .title}'
```

Present the list of commits and ask for confirmation before cherry-picking.

For each confirmed commit:

```bash
git cherry-pick <commit-sha>
```

Ask before pushing:

```bash
git push upstream maint-<VERSION>-r
```

## 11. Create Crossbow Verification PR

Create a PR to run all R crossbow jobs against the CRAN release branch:

```bash
gh pr create --repo apache/arrow \
  --base maint-<VERSION> \
  --head maint-<VERSION>-r \
  --title "WIP: [R] Verify CRAN release <VERSION>" \
  --body "Do not merge: Running R crossbow jobs against the CRAN release branch." \
  --draft
```

Then add a comment to trigger crossbow:

```bash
gh pr comment <PR_NUMBER> --repo apache/arrow --body "@github-actions crossbow submit --group r"
```

Add a link to this PR in the tracking issue so progress can be monitored.

Before proceeding to CRAN submission, verify all crossbow jobs pass.

## 12. Build and Check Package Locally

Ensure on the `maint-<VERSION>-r` branch with a clean working directory:

```bash
git fetch upstream
git checkout maint-<VERSION>-r
git clean -f -d
```

Check if `ARROW_HOME` is set:

```bash
echo "ARROW_HOME=${ARROW_HOME:-<not set>}"
```

If set, unset it so the build uses the vendored C++ version:

```bash
unset ARROW_HOME
```

Run the build (this takes a while):

```bash
cd r && make build
```

After the build, check for any generated doc changes that need to be committed:

```bash
git status
```

If there are modified `.Rd` files or other doc changes, commit them:

```bash
git add r/man/ r/inst/NOTICE.txt
git commit -m "[R] Update generated documentation"
```

Ask before pushing.

Then run the check:

```r
devtools::check_built("arrow_<VERSION>.tar.gz")
```

Fix any issues before proceeding.

## 13. Wait for Release Vote

Check if the Apache Arrow release vote has passed before continuing.

## 14. Update Checksums

Download checksums for pre-compiled binaries from ASF artifactory. Ask for the libarrow version (usually matches the release version):

```bash
cd r && Rscript tools/update-checksums.R <LIBARROW_VERSION>
```

Then commit the checksums:

```bash
git add -f tools/checksums/
git commit -m "[CRAN] Add checksums"
```

Ask before pushing.

## 15. Rebuild Package

Rebuild the tarball with checksums added:

```bash
cd r && make build
```

Check for any new doc changes after rebuild:

```bash
git status
```

Commit any changes and ask before pushing.

## 16. Check Binary Distributions

Test that the package works with pre-compiled Arrow C++ binaries on different platforms.

### Windows (win-builder)

Upload `r/arrow_<VERSION>.tar.gz` to https://win-builder.r-project.org/upload.aspx (r-devel only).

Results are emailed to Jon (the package maintainer). Ping Jon and wait for confirmation that the check is clean - no ERRORs, WARNINGs, or unexpected NOTEs. NOTEs about package size are expected.

### macOS Builder

Upload `r/arrow_<VERSION>.tar.gz` to https://mac.r-project.org/macbuilder/submit.html

Check the results link for any issues.

### Ubuntu Binary Installation

Test on Ubuntu that hosted binaries are used:

```r
install.packages("r/arrow_<VERSION>.tar.gz", repos = NULL)
```

The installation should download pre-compiled binaries rather than building from source.

### Final Local Check

Run one final local check:

```r
devtools::check_built("r/arrow_<VERSION>.tar.gz")
```

## 17. Submit to CRAN

Upload `r/arrow_<VERSION>.tar.gz` to https://xmpalantir.wu.ac.at/cransubmit/

Ping Jon to confirm the submission email when it arrives.

## 18. Tag for r-universe

After CRAN accepts the package:

```bash
git tag -f r-universe-release maint-<VERSION>-r
git push upstream r-universe-release --force
```

## 19. Update Backwards Compatibility Matrix

Add a new line to `dev/tasks/r/github.linux.arrow.version.back.compat.yml` with the new version.

Create a PR to main for this change.

## 20. Wait for CRAN Binaries

Monitor https://cran.r-project.org/package=arrow until CRAN-hosted binaries reflect the new version.

## 21. Prepare Social Media Content (Major Releases)

For major releases, prepare content for social media highlighting new features.

Format (thread of toots):
1. Opening: "We're excited to announce the release of {arrow} <VERSION>. Here's a roundup of the new features and changes. Full details can be found at https://arrow.apache.org/docs/r/news/ #rstats"
2. Feature highlights: One toot per major user-facing feature, with code screenshots from https://carbon.now.sh/ where appropriate. Skip minor fixes and CI/internal changes.
3. Contributor stats: Total contributors, C++ only, R only, both, first-timers.

To generate contributor stats, run from the arrow repo root:

```r
source("r/tools/contributor_stats.R")
release_contributor_stats("apache-arrow-<PREVIOUS_VERSION>", "apache-arrow-<VERSION>")
```

## 22. Patch Releases Only: Update Version Numbers

For patch releases (e.g., X.Y.1), update the version in:
- `ci/scripts/PKGBUILD`
- `r/DESCRIPTION`
- `r/NEWS.md`

## 23. CRAN-Only Releases: Update Documentation

For CRAN-only releases (no corresponding Arrow release):

Rebuild news page:

```r
pkgdown::build_news()
```

Submit a PR to the `asf-site` branch of https://github.com/apache/arrow-site with contents of `arrow/r/docs/news/index.html` replacing `arrow-site/docs/r/news/index.html`.

Bump the version in `r/pkgdown/assets/versions.json` and update on the `asf-site` branch too.

## 24. Check C++ Updates

Review C++ changes in this release and create GitHub issues for any items that need R bindings:

```bash
git log --oneline apache-arrow-<PREVIOUS_VERSION>..apache-arrow-<VERSION> -- cpp/ | head -50
```

## 25. Review Checklist

Review this packaging checklist and update as needed based on any issues encountered during this release.
