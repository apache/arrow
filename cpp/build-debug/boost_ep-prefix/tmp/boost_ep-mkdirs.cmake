# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix/src/boost_ep"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix/src/boost_ep-build"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix/tmp"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix/src/boost_ep-stamp"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix/src"
  "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix/src/boost_ep-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix/src/boost_ep-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/boost_ep-prefix/src/boost_ep-stamp${cfgdir}") # cfgdir has leading slash
endif()
