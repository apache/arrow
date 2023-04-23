#!/bin/bash
set -ex

export DISABLE_AUTOBREW=1

# arrow uses C++17
export ARROW_R_CXXFLAGS="${ARROW_R_CXXFLAGS} -std=c++17"
export LIBARROW_BUILD=false

if [[ "${target_platform}" == osx-* ]]; then
    # See https://conda-forge.org/docs/maintainer/knowledge_base.html#newer-c-features-with-old-sdk
    export ARROW_R_CXXFLAGS="${ARROW_R_CXXFLAGS} -D_LIBCPP_DISABLE_AVAILABILITY"
fi

# ${R_ARGS} necessary to support cross-compilation
${R} CMD INSTALL --build r/. ${R_ARGS}
# Ensure that features are enabled in the R build (feel free to add others)
${R} -s -e 'library(arrow); stopifnot(arrow_with_dataset(), arrow_with_parquet(), arrow_with_s3())'
