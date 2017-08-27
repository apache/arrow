# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#
# This is an awk script to process output from the include-what-you-use (IWYU)
# tool. As of now, IWYU is of alpha quality and it gives many incorrect
# recommendations -- obviously invalid or leading to compilation breakage.
# Most of those can be silenced using appropriate IWYU pragmas, but it's not
# the case for the auto-generated files.
#
# Also, it's possible to address invalid recommendation using mappings:
#   https://github.com/include-what-you-use/include-what-you-use/blob/master/docs/IWYUMappings.md
#
# Usage:
#  1. Run the CMake with -DCMAKE_CXX_INCLUDE_WHAT_YOU_USE=<iwyu_cmd_line>
#
#     The path to the IWYU binary should be absolute. The path to the binary
#     and the command-line options should be separated by semicolon
#     (that's for feeding it into CMake list variables).
#
#     E.g., from the build directory (line breaks are just for readability):
#
#     CC=../../thirdparty/clang-toolchain/bin/clang
#     CXX=../../thirdparty/clang-toolchain/bin/clang++
#     IWYU="`pwd`../../thirdparty/clang-toolchain/bin/include-what-you-use;\
#       -Xiwyu;--mapping_file=`pwd`../../build-support/iwyu/mappings/map.imp"
#
#     ../../build-support/enable_devtoolset.sh \
#       env CC=$CC CXX=$CXX \
#       ../../thirdparty/installed/common/bin/cmake \
#       -DCMAKE_CXX_INCLUDE_WHAT_YOU_USE=\"$IWYU\" \
#       ../..
#
#     NOTE:
#       Since the arrow code has some 'ifdef NDEBUG' directives, it's possible
#       that IWYU would produce different results if run against release, not
#       debug build. However, we plan to use the tool only with debug builds.
#
#  2. Run make, separating the output from the IWYU tool into a separate file
#     (it's possible to use piping the output from the tool to the script
#      but having a file is good for future reference, if necessary):
#
#     make -j$(nproc) 2>/tmp/iwyu.log
#
#  3. Process the output from the IWYU tool using the script:
#
#     awk -f ../../build-support/iwyu/iwyu-filter.awk /tmp/iwyu.log
#

BEGIN {
  # This is the list of the files for which the suggestions from IWYU are
  # ignored. Eventually, this list should become empty as soon as all the valid
  # suggestions are addressed and invalid ones are taken care either by proper
  # IWYU pragmas or adding special mappings (e.g. like boost mappings).
  # muted["relative/path/to/file"]
}

# mute all suggestions for the auto-generated files
/.*\.(pb|proxy|service)\.(cc|h) should (add|remove) these lines:/, /^$/ {
  next
}

# mute suggestions for the explicitly specified files
/.* should (add|remove) these lines:/ {
  do_print = 1
  for (path in muted) {
    if (index($0, path)) {
      do_print = 0
      break
    }
  }
}
/^$/ {
  if (do_print) print
  do_print = 0
}
{ if (do_print) print }
