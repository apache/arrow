#!/bin/bash

set -ueo pipefail

git diff --name-only $(git merge-base $1 $2)
