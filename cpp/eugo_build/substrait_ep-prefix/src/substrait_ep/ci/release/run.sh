#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail

npx --yes \
  -p semantic-release \
  -p "@semantic-release/commit-analyzer" \
  -p "@semantic-release/release-notes-generator" \
  -p "@semantic-release/changelog" \
  -p "@semantic-release/github" \
  -p "@semantic-release/exec" \
  -p "@semantic-release/git" \
  -p "conventional-changelog-conventionalcommits" \
  semantic-release --ci
