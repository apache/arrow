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
#  ClangTools_PATH -
#   When set, this path is inspected instead of standard library binary locations
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

find_program(CLANG_TIDY_BIN
  NAMES clang-tidy-4.0
  clang-tidy-3.9
  clang-tidy-3.8
  clang-tidy-3.7
  clang-tidy-3.6
  clang-tidy
  PATHS ${ClangTools_PATH} $ENV{CLANG_TOOLS_PATH} /usr/local/bin /usr/bin "${HOMEBREW_PREFIX}/bin"
        NO_DEFAULT_PATH
)

if ( "${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND" )
  set(CLANG_TIDY_FOUND 0)
  message("clang-tidy not found")
else()
  set(CLANG_TIDY_FOUND 1)
  message("clang-tidy found at ${CLANG_TIDY_BIN}")
endif()

if (CLANG_FORMAT_VERSION)
    find_program(CLANG_FORMAT_BIN
      NAMES clang-format-${CLANG_FORMAT_VERSION}
      PATHS
            ${ClangTools_PATH}
            $ENV{CLANG_TOOLS_PATH}
            /usr/local/bin /usr/bin "${HOMEBREW_PREFIX}/bin"
            NO_DEFAULT_PATH
    )

    # If not found yet, search alternative locations
    if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
      STRING(REGEX REPLACE "^([0-9]+)\\.[0-9]+" "\\1" CLANG_FORMAT_MAJOR_VERSION "${CLANG_FORMAT_VERSION}")
      STRING(REGEX REPLACE "^[0-9]+\\.([0-9]+)" "\\1" CLANG_FORMAT_MINOR_VERSION "${CLANG_FORMAT_VERSION}")
      if (APPLE)
        # Homebrew ships older LLVM versions in /usr/local/opt/llvm@version/
        if ("${CLANG_FORMAT_MINOR_VERSION}" STREQUAL "0")
            find_program(CLANG_FORMAT_BIN
              NAMES clang-format
              PATHS "${HOMEBREW_PREFIX}/opt/llvm@${CLANG_FORMAT_MAJOR_VERSION}/bin"
                    NO_DEFAULT_PATH
            )
        else()
            find_program(CLANG_FORMAT_BIN
              NAMES clang-format
              PATHS "${HOMEBREW_PREFIX}/opt/llvm@${CLANG_FORMAT_VERSION}/bin"
                    NO_DEFAULT_PATH
            )
        endif()

        if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
          # binary was still not found, look into Cellar
          file(GLOB CLANG_FORMAT_PATH "${HOMEBREW_PREFIX}/Cellar/llvm/${CLANG_FORMAT_VERSION}.*")
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
          PATHS
                ${ClangTools_PATH}
                $ENV{CLANG_TOOLS_PATH}
                /usr/local/bin /usr/bin
                NO_DEFAULT_PATH
        )
        if (NOT ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND"))
          execute_process(COMMAND ${CLANG_FORMAT_BIN} "-version"
            OUTPUT_VARIABLE CLANG_FORMAT_FOUND_VERSION_MESSAGE
            OUTPUT_STRIP_TRAILING_WHITESPACE)
          if (NOT ("${CLANG_FORMAT_FOUND_VERSION_MESSAGE}" MATCHES "^clang-format version ${CLANG_FORMAT_MAJOR_VERSION}\\.${CLANG_FORMAT_MINOR_VERSION}.*"))
            set(CLANG_FORMAT_BIN "CLANG_FORMAT_BIN-NOTFOUND")
          endif()
        endif()
      endif()
    endif()

else()
    find_program(CLANG_FORMAT_BIN
      NAMES clang-format-4.0
      clang-format-3.9
      clang-format-3.8
      clang-format-3.7
      clang-format-3.6
      clang-format
      PATHS ${ClangTools_PATH} $ENV{CLANG_TOOLS_PATH} /usr/local/bin /usr/bin "${HOMEBREW_PREFIX}/bin"
            NO_DEFAULT_PATH
    )
endif()

if ( "${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND" )
  set(CLANG_FORMAT_FOUND 0)
  message("clang-format not found")
else()
  set(CLANG_FORMAT_FOUND 1)
  message("clang-format found at ${CLANG_FORMAT_BIN}")
endif()
