#!/bin/bash

set -exuo pipefail

# Rename arrow.dll to lib_arrow.dll to avoid conflicts with the arrow-cpp arrow.dll
sed -i -e 's/void R_init_arrow/__declspec(dllexport) void R_init_lib_arrow/g' r/src/arrowExports.cpp
sed -i -e 's/useDynLib(arrow/useDynLib(lib_arrow/g' r/NAMESPACE
