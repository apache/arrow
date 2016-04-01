#!/bin/bash
#
# Runs clang format in the given directory
# Arguments:
#   $1 - Path to the clang tidy binary
#   $2 - Path to the compile_commands.json to use
#   $3 - Apply fixes (will raise an error if false and not there where changes)
#   $ARGN - Files to run clang format on
#
CLANG_TIDY=$1
shift
COMPILE_COMMANDS=$1
shift
APPLY_FIXES=$1
shift

# clang format will only find its configuration if we are in 
# the source tree or in a path relative to the source tree
if [ "$APPLY_FIXES" == "1" ]; then
  $CLANG_TIDY -p $COMPILE_COMMANDS -fix  $@
else
  NUM_CORRECTIONS=`$CLANG_TIDY -p $COMPILE_COMMANDS $@ 2>&1 | grep -v Skipping | grep "warnings* generated" | wc -l`
  if [ "$NUM_CORRECTIONS" -gt "0" ]; then
    echo "clang-tidy had suggested fixes.  Please fix these!!!"
    exit 1
  fi
fi 
