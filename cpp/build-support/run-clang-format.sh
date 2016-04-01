#!/bin/bash
#
# Runs clang format in the given directory
# Arguments:
#   $1 - Path to the source tree
#   $2 - Path to the clang format binary
#   $3 - Apply fixes (will raise an error if false and not there where changes)
#   $ARGN - Files to run clang format on
#
SOURCE_DIR=$1
shift
CLANG_FORMAT=$1
shift
APPLY_FIXES=$1
shift

# clang format will only find its configuration if we are in 
# the source tree or in a path relative to the source tree
pushd $SOURCE_DIR
if [ "$APPLY_FIXES" == "1" ]; then
  $CLANG_FORMAT -i $@
else

  NUM_CORRECTIONS=`$CLANG_FORMAT -output-replacements-xml  $@ | grep offset | wc -l`
  if [ "$NUM_CORRECTIONS" -gt "0" ]; then
    echo "clang-format suggested changes, please run 'make format'!!!!"
    exit 1
  fi
fi 
popd
