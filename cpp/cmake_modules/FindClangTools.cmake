#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tries to find the clang-tidy and clang-format modules
#
# Usage of this module as follows:
#
#  find_package(ClangTools)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  ClangToolsBin_HOME -
#   When set, this path is inspected instead of standard library binary locations
#   to find clang-tidy and clang-format
#
# This module defines
#  CLANG_TIDY_BIN, The  path to the clang tidy binary
#  CLANG_TIDY_FOUND, Whether clang tidy was found
#  CLANG_FORMAT_BIN, The path to the clang format binary 
#  CLANG_TIDY_FOUND, Whether clang format was found

find_program(CLANG_TIDY_BIN 
  NAMES clang-tidy-3.8 clang-tidy-3.7 clang-tidy-3.6  clang-tidy
  PATHS ${ClangTools_PATH} $ENV{CLANG_TOOLS_PATH} /usr/local/bin /usr/bin 
        NO_DEFAULT_PATH
)

if ( "${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND" ) 
  set(CLANG_TIDY_FOUND 0)
  message("clang-tidy not found")
else()
  set(CLANG_TIDY_FOUND 1)
  message("clang-tidy found at ${CLANG_TIDY_BIN}")
endif()

find_program(CLANG_FORMAT_BIN 
  NAMES clang-format-3.8 clang-format-3.7 clang-format-3.6  clang-format
  PATHS ${ClangTools_PATH} $ENV{CLANG_TOOLS_PATH} /usr/local/bin /usr/bin 
        NO_DEFAULT_PATH
)

if ( "${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND" ) 
  set(CLANG_FORMAT_FOUND 0)
  message("clang-format not found")
else()
  set(CLANG_FORMAT_FOUND 1)
  message("clang-format found at ${CLANG_FORMAT_BIN}")
endif()

