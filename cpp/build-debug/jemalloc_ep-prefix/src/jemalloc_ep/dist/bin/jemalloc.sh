#!/bin/sh

prefix=/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/jemalloc_ep-prefix/src/jemalloc_ep/dist
exec_prefix=/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/jemalloc_ep-prefix/src/jemalloc_ep/dist
libdir=/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/jemalloc_ep-prefix/src/jemalloc_ep/dist//lib

DYLD_INSERT_LIBRARIES=${libdir}/libjemalloc.2.dylib
export DYLD_INSERT_LIBRARIES
exec "$@"
