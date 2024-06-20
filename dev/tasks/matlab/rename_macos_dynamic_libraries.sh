#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -ex

# Issue Number 1:
#
# MATLAB's programmatic packaging interface does not properly package symbolic links. If the
# toolboxFolder argumented provided to the constructor of matlab.addons.toolbox.ToolboxOptions
# contains a symbolic link, the ToolboxOptions will resolve the symbolic link and include
# the symbolic link's target as one of the files to package instead of the link itself.
#
# Example:
#
# Suppose you had this folder structure
#
#        $ tree /tmp/example
#        /tmp/example
#        |-- regular_file.txt
#        |-- symbolic_link -> regular_file.txt
#
# Passing "/tmp/example" as the toolboxFolder argument to ToolboxOptions' constructor in MATLAB
# returns the following object:
#
#       >> opts = matlab.addons.toolbox.ToolboxOptions("/tmp/example", "dummy-identifier");
#       >> opts.ToolboxFiles
#
#       ans =
#
#            "/private/tmp/example/regular_file.txt"
#
# Note that the ToolboxFiles property - the list of files to package - does not include the
# symbolic link.
#
# This is a known limitation with matlab.addons.toolbox.ToolboxOptions.
#
# Why is this a problem?
#
# On macOS, building the Arrow C++ bindings generates the following files:
#
#        $ tree arrow/matlab/install/arrow_matlab/+libmexclass/+proxy/
#        .
#        |-- libarrow.1700.0.0.dylib
#        |-- libarrow.1700.dylib -> libarrow.1700.0.0.dylib
#        |-- libarrow.dylib -> libarrow.1700.dylib
#
# When "arrow/matlab/install/arrow_matlab" is suppplied as the toolboxFolder argument
# to the constructor of ToolboxOptions, only the libarrow.1700.0.0.dylib is included as a file
# to package. This is a problem because building the MATLAB interface to Arrow creates a shared
# library called libarrowproxy.dylib, which is linked against libarrow.1700.dylib - not
# libarrow.1700.0.0.dyblib.
#
# This can be seen by calling otool with the -L flag on libarrowproxy.dylib:
#
#        $ otool -L libarrowproxy.dylib | grep -E '@rpath/libarrow\.'
#	            @rpath/libarrow.1700.dylib
#
# To prevent a run-time linker error, we need to update the name of libarrowproxy.dylib's
# dependent shared library from @rpath/libarrow.1700.dylib to @rpath/libarrow.1700.0.0.dylib
# because only libarrow.1700.0.0.dylib is packaged.
#
# ==============================================================================================
#
# Issue Number 2:
#
# The platforms that the MATLAB Interface to Arrow supports Windows, Linux,
# Intel/AMD-based macOS and ARM-based macOS. Currently, we create one "monolithic" MLTBX file
# to package the interface that includes all shared libraries for all supported platforms.
# We do this because the File Exchange <-> GitHub Release integration does not support
# platform-specific MLTBX files.
#
# The problem with creating one MLTBX to package the interface for all platforms is that
# the names of the shared libraries built by the interface for Intel/AMD-based macOS
# and ARM-based macOS are identical.For example, building the interface generates the shared
# library libarrow.1700.0.0.dylib on both platforms. To avoid this duplicate name problem,
# we need to uniquify the names of the shared libraries generated for Intel/AMD-based
# macOS and ARM-based macOS.

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <dylib-dir> <arch>"
  exit 1
fi

DYLIB_DIR=${1}
ARCH=${2}

if [ "$ARCH" == "arm64" ]; then
  IS_ARM64=1
elif [ "$ARCH" == "x64" ]; then
  IS_ARM64=0
else
  echo "<arch> must be arm64 or x64"
  exit 1
fi

pushd ${DYLIB_DIR}

LIBARROW_DYLIB="$(find . -name 'libarrow.dylib' | xargs basename)"
LIBARROW_MAJOR_DYLIB="$(find . -name 'libarrow.*.dylib' -type l | xargs basename)"
LIBARROW_MAJOR_MINOR_PATCH_DYLIB="$(echo libarrow.*.*.dylib)"
LIBMEXCLASS_DYLIB="$(find . -name 'libmexclass.dylib' | xargs basename)"
LIBARROWPROXY_DYLIB="$(find . -name 'libarrowproxy.dylib' | xargs basename)"
if [ $IS_ARM64 -eq 1 ]; then
  MEX_GATEWAY="$(find . -name 'gateway.mexmaca64' | xargs basename)"
else
  MEX_GATEWAY="$(find . -name 'gateway.mexmaci64' | xargs basename)"
fi

MAJOR_MINOR_PATCH_VERSION=${LIBARROW_MAJOR_MINOR_PATCH_DYLIB#*.}
MAJOR_MINOR_PATCH_VERSION=${MAJOR_MINOR_PATCH_VERSION%.*}

LIBARROW_ARCH_MAJOR_MINOR_PATCH_DYLIB="libarrow_${ARCH}.${MAJOR_MINOR_PATCH_VERSION}.dylib"
LIBARROWPROXY_ARCH_DYLIB="libarrowproxy_${ARCH}.dylib"
LIBMEXCLASS_ARCH_DYLIB="libmexclass_${ARCH}.dylib"

# Delete the symbolic links. These files are not included in the packaged MLTBX file. 
rm ${LIBARROW_MAJOR_DYLIB}
rm ${LIBARROW_DYLIB}

# Rename libarrow.*.*.*.dylib to libarrow_(arm64|x64).*.*.*.dylib (e.g. libarrow.1700.0.0.dylib -> libarrow_(arm64|x64).1700.0.0.dylib)
mv ${LIBARROW_MAJOR_MINOR_PATCH_DYLIB} ${LIBARROW_ARCH_MAJOR_MINOR_PATCH_DYLIB}

# Rename libarrowproxy.dylib to libarrowproxy_(arm64|x64).dylib
mv ${LIBARROWPROXY_DYLIB} ${LIBARROWPROXY_ARCH_DYLIB}

# Rename libmexclass.dylib to libmexclass_(arm64|x64).dylib
mv ${LIBMEXCLASS_DYLIB} ${LIBMEXCLASS_ARCH_DYLIB}

# Update the identificaton names of the renamed dynamic libraries
install_name_tool -id @rpath/${LIBMEXCLASS_ARCH_DYLIB} ${LIBMEXCLASS_ARCH_DYLIB}
install_name_tool -id @rpath/${LIBARROWPROXY_ARCH_DYLIB} ${LIBARROWPROXY_ARCH_DYLIB}
install_name_tool -id @rpath/${LIBARROW_ARCH_MAJOR_MINOR_PATCH_DYLIB} ${LIBARROW_ARCH_MAJOR_MINOR_PATCH_DYLIB}

# Change install name of dependent shared library libarrow.*.*.*.dylib to libarrow_arm64.*.*.*.dylib in libarrowproxy_(arm64|x64).dylib
install_name_tool -change @rpath/${LIBARROW_MAJOR_DYLIB} @rpath/${LIBARROW_ARCH_MAJOR_MINOR_PATCH_DYLIB} ${LIBARROWPROXY_ARCH_DYLIB}

# Change install name of dependent shared library libmexclass.dylib to libmexclass_(arm64|x64).*.*.*.dylib libarrowproxy_(arm64|x64).dylib
install_name_tool -change @rpath/${LIBMEXCLASS_DYLIB} @rpath/${LIBMEXCLASS_ARCH_DYLIB} ${LIBARROWPROXY_ARCH_DYLIB}

# Change install name of dependent shared library libmexclass.dylib to libmexclass_(arm64|x64).dylib in gateway.(mexmaca64|mexmaci64)
install_name_tool -change @rpath/${LIBMEXCLASS_DYLIB} @rpath/${LIBMEXCLASS_ARCH_DYLIB} ${MEX_GATEWAY}

# Change install name of dependent shared library libarrowproxy.dylib to libarrowproxy_(arm64|x64).dylib in gateway.(mexmaca64|mexmaci64)
install_name_tool -change @rpath/${LIBARROWPROXY_DYLIB} @rpath/${LIBARROWPROXY_ARCH_DYLIB} ${MEX_GATEWAY}

popd