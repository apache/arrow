# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file LICENSE.rst or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/substrait_ep")
  file(MAKE_DIRECTORY "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/substrait_ep")
endif()
file(MAKE_DIRECTORY
  "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/substrait_ep-build"
  "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix"
  "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/tmp"
  "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/substrait_ep-stamp"
  "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src"
  "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/substrait_ep-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/substrait_ep-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/substrait_ep-stamp${cfgdir}") # cfgdir has leading slash
endif()
