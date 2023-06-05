#!/usr/bin/env bash
# Copyright 2014 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Script which wraps running a test and redirects its output to a
# test log directory.
#
# Arguments:
#    $1 - Base path for logs/artifacts.
#    $2 - type of test (e.g. test or benchmark)
#    $3 - path to executable
#    $ARGN - arguments for executable
#

OUTPUT_ROOT=$1
shift
ROOT=$(cd $(dirname $BASH_SOURCE)/..; pwd)

TEST_LOGDIR=$OUTPUT_ROOT/build/$1-logs
mkdir -p $TEST_LOGDIR

RUN_TYPE=$1
shift
TEST_DEBUGDIR=$OUTPUT_ROOT/build/$RUN_TYPE-debug
mkdir -p $TEST_DEBUGDIR

TEST_DIRNAME=$(cd $(dirname $1); pwd)
TEST_FILENAME=$(basename $1)
shift
TEST_EXECUTABLE="$TEST_DIRNAME/$TEST_FILENAME"
TEST_NAME=$(echo $TEST_FILENAME | sed -E -e 's/\..+$//') # Remove path and extension (if any).

# We run each test in its own subdir to avoid core file related races.
TEST_WORKDIR=$OUTPUT_ROOT/build/test-work/$TEST_NAME
mkdir -p $TEST_WORKDIR
pushd $TEST_WORKDIR >/dev/null || exit 1
rm -f *

set -o pipefail

LOGFILE=$TEST_LOGDIR/$TEST_NAME.txt
XMLFILE=$TEST_LOGDIR/$TEST_NAME.xml

TEST_EXECUTION_ATTEMPTS=1

# Remove both the uncompressed output, so the developer doesn't accidentally get confused
# and read output from a prior test run.
rm -f $LOGFILE $LOGFILE.gz

pipe_cmd=cat

function setup_sanitizers() {
  # Sets environment variables for different sanitizers (it configures how) the run_tests. Function works.

  # Configure TSAN (ignored if this isn't a TSAN build).
  #
  TSAN_OPTIONS="$TSAN_OPTIONS suppressions=$ROOT/build-support/tsan-suppressions.txt"
  TSAN_OPTIONS="$TSAN_OPTIONS history_size=7"
  # Some tests deliberately fail allocating memory
  TSAN_OPTIONS="$TSAN_OPTIONS allocator_may_return_null=1"
  export TSAN_OPTIONS

  UBSAN_OPTIONS="$UBSAN_OPTIONS print_stacktrace=1"
  UBSAN_OPTIONS="$UBSAN_OPTIONS suppressions=$ROOT/build-support/ubsan-suppressions.txt"
  export UBSAN_OPTIONS

  # Enable leak detection even under LLVM 3.4, where it was disabled by default.
  # This flag only takes effect when running an ASAN build.
  # ASAN_OPTIONS="$ASAN_OPTIONS detect_leaks=1"
  # export ASAN_OPTIONS

  # Set up suppressions for LeakSanitizer
  LSAN_OPTIONS="$LSAN_OPTIONS suppressions=$ROOT/build-support/lsan-suppressions.txt"
  export LSAN_OPTIONS
}

function run_test() {
  # Run gtest style tests with sanitizers if they are setup appropriately.

  # gtest won't overwrite old junit test files, resulting in a build failure
  # even when retries are successful.
  rm -f $XMLFILE

  $TEST_EXECUTABLE "$@" > $LOGFILE.raw 2>&1
  STATUS=$?
  cat $LOGFILE.raw \
    | ${PYTHON:-python} $ROOT/build-support/asan_symbolize.py \
    | ${CXXFILT:-c++filt} \
    | $pipe_cmd 2>&1 | tee $LOGFILE
  rm -f $LOGFILE.raw

  # TSAN doesn't always exit with a non-zero exit code due to a bug:
  # mutex errors don't get reported through the normal error reporting infrastructure.
  # So we make sure to detect this and exit 1.
  #
  # Additionally, certain types of failures won't show up in the standard JUnit
  # XML output from gtest. We assume that gtest knows better than us and our
  # regexes in most cases, but for certain errors we delete the resulting xml
  # file and let our own post-processing step regenerate it.
  if grep -E -q "ThreadSanitizer|Leak check.*detected leaks" $LOGFILE ; then
    echo ThreadSanitizer or leak check failures in $LOGFILE
    STATUS=1
    rm -f $XMLFILE
  fi
}

function print_coredumps() {
  # The script expects core files relative to the build directory with unique
  # names per test executable because of the parallel running. So the corefile
  # patterns must be set with prefix `core.{test-executable}*`:
  #
  # In case of macOS:
  #   sudo sysctl -w kern.corefile=core.%N.%P
  # On Linux:
  #   sudo sysctl -w kernel.core_pattern=core.%e.%p
  #
  # and the ulimit must be increased:
  #   ulimit -c unlimited

  # filename is truncated to the first 15 characters in case of linux, so limit
  # the pattern for the first 15 characters
  FILENAME=$(basename "${TEST_EXECUTABLE}")
  FILENAME=$(echo ${FILENAME} | cut -c-15)
  PATTERN="^core\.${FILENAME}"

  COREFILES=$(ls | grep $PATTERN)
  if [ -n "$COREFILES" ]; then
    echo "Found core dump, printing backtrace:"

    for COREFILE in $COREFILES; do
      # Print backtrace
      if [ "$(uname)" == "Darwin" ]; then
        lldb -c "${COREFILE}" --batch --one-line "thread backtrace all -e true"
      else
        gdb -c "${COREFILE}" $TEST_EXECUTABLE -ex "thread apply all bt" -ex "set pagination 0" -batch
      fi
      # Remove the coredump, regenerate it via running the test case directly
      rm "${COREFILE}"
    done
  fi
}

function post_process_tests() {
  # If we have a LeakSanitizer report, and XML reporting is configured, add a new test
  # case result to the XML file for the leak report. Otherwise Jenkins won't show
  # us which tests had LSAN errors.
  if grep -E -q "ERROR: LeakSanitizer: detected memory leaks" $LOGFILE ; then
    echo Test had memory leaks. Editing XML
    sed -i.bak -e '/<\/testsuite>/ i\
  <testcase name="LeakSanitizer" status="run" classname="LSAN">\
    <failure message="LeakSanitizer failed" type="">\
      See txt log file for details\
    </failure>\
  </testcase>' \
      $XMLFILE
    mv $XMLFILE.bak $XMLFILE
  fi
}

function run_other() {
  # Generic run function for test like executables that aren't actually gtest
  $TEST_EXECUTABLE "$@" 2>&1 | $pipe_cmd > $LOGFILE
  STATUS=$?
}

if [ $RUN_TYPE = "test" ]; then
  setup_sanitizers
fi

# Run the actual test.
for ATTEMPT_NUMBER in $(seq 1 $TEST_EXECUTION_ATTEMPTS) ; do
  if [ $ATTEMPT_NUMBER -lt $TEST_EXECUTION_ATTEMPTS ]; then
    # If the test fails, the test output may or may not be left behind,
    # depending on whether the test cleaned up or exited immediately. Either
    # way we need to clean it up. We do this by comparing the data directory
    # contents before and after the test runs, and deleting anything new.
    #
    # The comm program requires that its two inputs be sorted.
    TEST_TMPDIR_BEFORE=$(find $TEST_TMPDIR -maxdepth 1 -type d | sort)
  fi

  if [ $ATTEMPT_NUMBER -lt $TEST_EXECUTION_ATTEMPTS ]; then
    # Now delete any new test output.
    TEST_TMPDIR_AFTER=$(find $TEST_TMPDIR -maxdepth 1 -type d | sort)
    DIFF=$(comm -13 <(echo "$TEST_TMPDIR_BEFORE") \
                    <(echo "$TEST_TMPDIR_AFTER"))
    for DIR in $DIFF; do
      # Multiple tests may be running concurrently. To avoid deleting the
      # wrong directories, constrain to only directories beginning with the
      # test name.
      #
      # This may delete old test directories belonging to this test, but
      # that's not typically a concern when rerunning flaky tests.
      if [[ $DIR =~ ^$TEST_TMPDIR/$TEST_NAME ]]; then
        echo Deleting leftover flaky test directory "$DIR"
        rm -Rf "$DIR"
      fi
    done
  fi
  echo "Running $TEST_NAME, redirecting output into $LOGFILE" \
    "(attempt ${ATTEMPT_NUMBER}/$TEST_EXECUTION_ATTEMPTS)"
  if [ $RUN_TYPE = "test" ]; then
    run_test $*
  else
    run_other $*
  fi
  if [ "$STATUS" -eq "0" ]; then
    break
  elif [ "$ATTEMPT_NUMBER" -lt "$TEST_EXECUTION_ATTEMPTS" ]; then
    echo Test failed attempt number $ATTEMPT_NUMBER
    echo Will retry...
  fi
done

if [ $RUN_TYPE = "test" ]; then
  post_process_tests
fi

print_coredumps

popd
rm -Rf $TEST_WORKDIR

exit $STATUS
