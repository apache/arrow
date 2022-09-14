#!/usr/bin/env bash

set -ex

export DISABLE_AUTOBREW=1
# See https://conda-forge.org/docs/maintainer/knowledge_base.html#newer-c-features-with-old-sdk
export ARROW_R_CXXFLAGS="${ARROW_R_CXXFLAGS} -D_LIBCPP_DISABLE_AVAILABILITY"

$R CMD INSTALL --build r/.
