# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-prefix/src/re2_ep"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-prefix/src/re2_ep-build"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-install"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-prefix/tmp"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-prefix/src/re2_ep-stamp"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-prefix/src"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-prefix/src/re2_ep-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-prefix/src/re2_ep-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/re2_ep-prefix/src/re2_ep-stamp${cfgdir}") # cfgdir has leading slash
endif()
