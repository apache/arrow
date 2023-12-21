#!/bin/bash
set -eux

bazel clean
bazel build --compilation_mode=dbg -- //:all
bazel test  --compilation_mode=dbg --test_output=errors -- //:all \
  -//:dfa_test \
  -//:exhaustive1_test \
  -//:exhaustive2_test \
  -//:exhaustive3_test \
  -//:exhaustive_test \
  -//:random_test

bazel clean
bazel build --compilation_mode=opt -- //:all
bazel test  --compilation_mode=opt --test_output=errors -- //:all \
  -//:dfa_test \
  -//:exhaustive1_test \
  -//:exhaustive2_test \
  -//:exhaustive3_test \
  -//:exhaustive_test \
  -//:random_test

exit 0
