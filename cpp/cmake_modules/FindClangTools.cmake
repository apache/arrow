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
# Variables used by this module which can change the default behaviour and need
# to be set before calling find_package:
#
#  CLANG_FORMAT_VERSION -
#   The version of clang-format to find. If this is not specified, clang-format
#   will not be searched for.
#
#  ClangTools_PATH -
#   When set, this path is inspected in addition to standard library binary locations
#   to find clang-tidy and clang-format
#
# This module defines
#  CLANG_TIDY_BIN, The  path to the clang tidy binary
#  CLANG_TIDY_FOUND, Whether clang tidy was found
#  CLANG_FORMAT_BIN, The path to the clang format binary
#  CLANG_FORMAT_FOUND, Whether clang format was found

if (DEFINED ENV{HOMEBREW_PREFIX})
  set(HOMEBREW_PREFIX "$ENV{HOMEBREW_PREFIX}")
else()
  find_program(BREW_BIN brew)
  if ((NOT ("${BREW_BIN}" STREQUAL "BREW_BIN-NOTFOUND")) AND APPLE)
    execute_process(
      COMMAND ${BREW_BIN} --prefix
      OUTPUT_VARIABLE HOMEBREW_PREFIX
      OUTPUT_STRIP_TRAILING_WHITESPACE
    )
  else()
    set(HOMEBREW_PREFIX "/usr/local")
  endif()
endif()

set(CLANG_TOOLS_SEARCH_PATHS
  ${ClangTools_PATH}
  $ENV{CLANG_TOOLS_PATH}
  /usr/local/bin /usr/bin
  "C:/Program Files/LLVM/bin"
  "${HOMEBREW_PREFIX}/bin")

find_program(CLANG_TIDY_BIN
  NAMES clang-tidy-${ARROW_LLVM_VERSION}
  clang-tidy-${ARROW_LLVM_MAJOR_VERSION}
  PATHS ${CLANG_TOOLS_SEARCH_PATHS} NO_DEFAULT_PATH
)

if ( "${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND" )
  set(CLANG_TIDY_FOUND 0)
  message(STATUS "clang-tidy not found")
else()
  set(CLANG_TIDY_FOUND 1)
  message(STATUS "clang-tidy found at ${CLANG_TIDY_BIN}")
endif()

if (ARROW_LLVM_VERSION)
    find_program(CLANG_FORMAT_BIN
      NAMES clang-format-${ARROW_LLVM_VERSION}
      clang-format-${ARROW_LLVM_MAJOR_VERSION}
      PATHS ${CLANG_TOOLS_SEARCH_PATHS} NO_DEFAULT_PATH
    )

    # If not found yet, search alternative locations
    if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
      if (APPLE)
        # Homebrew ships older LLVM versions in /usr/local/opt/llvm@version/
        if ("${ARROW_LLVM_MINOR_VERSION}" STREQUAL "0")
            find_program(CLANG_FORMAT_BIN
              NAMES clang-format
              PATHS "${HOMEBREW_PREFIX}/opt/llvm@${ARROW_LLVM_MAJOR_VERSION}/bin"
                    NO_DEFAULT_PATH
            )
        else()
            find_program(CLANG_FORMAT_BIN
              NAMES clang-format
              PATHS "${HOMEBREW_PREFIX}/opt/llvm@${ARROW_LLVM_VERSION}/bin"
                    NO_DEFAULT_PATH
            )
        endif()

        if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
          # binary was still not found, look into Cellar
          file(GLOB CLANG_FORMAT_PATH "${HOMEBREW_PREFIX}/Cellar/llvm/${ARROW_LLVM_VERSION}.*")
          find_program(CLANG_FORMAT_BIN
            NAMES clang-format
            PATHS "${CLANG_FORMAT_PATH}/bin"
                  NO_DEFAULT_PATH
          )
        endif()
      else()
        # try searching for "clang-format" and check the version
        find_program(CLANG_FORMAT_BIN
          NAMES clang-format
          PATHS ${CLANG_TOOLS_SEARCH_PATHS} NO_DEFAULT_PATH
        )
        if (NOT ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND"))
          execute_process(COMMAND ${CLANG_FORMAT_BIN} "-version"
            OUTPUT_VARIABLE CLANG_FORMAT_FOUND_VERSION_MESSAGE
            OUTPUT_STRIP_TRAILING_WHITESPACE)
          if (NOT ("${CLANG_FORMAT_FOUND_VERSION_MESSAGE}" MATCHES "^clang-format version ${ARROW_LLVM_MAJOR_VERSION}\\.${ARROW_LLVM_MINOR_VERSION}.*"))
            set(CLANG_FORMAT_BIN "CLANG_FORMAT_BIN-NOTFOUND")
          endif()
        endif()
      endif()
    endif()

endif()

if ( "${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND" )
  set(CLANG_FORMAT_FOUND 0)
  message(STATUS "clang-format not found")
else()
  set(CLANG_FORMAT_FOUND 1)
  message(STATUS "clang-format found at ${CLANG_FORMAT_BIN}")
endif()
