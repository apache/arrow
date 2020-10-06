#!/bin/bash

set -exuo pipefail

# Patch Rcpp
# https://github.com/RcppCore/Rcpp/pull/1069
sed -i -e 's; abs; std::abs;g' $BUILD_PREFIX/Lib/R/library/Rcpp/include/Rcpp/DataFrame.h

# Rename arrow.dll to lib_arrow.dll to avoid conflicts with the arrow-cpp arrow.dll
sed -i -e 's/R_init_arrow/R_init_lib_arrow/g' r/src/arrowExports.cpp
sed -i -e 's/useDynLib(arrow/useDynLib(lib_arrow/g' r/NAMESPACE

# Make exported symbols visible
# This needs to be patched in https://github.com/wch/r-source/blob/trunk/src/include/R_ext/Visibility.h
sed -i -e 's/attribute_visible/__declspec(dllexport)/g' $BUILD_PREFIX/Lib/R/library/Rcpp/include/RcppCommon.h
