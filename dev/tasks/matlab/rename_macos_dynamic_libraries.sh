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
# MATLAB's programmatic packaging interface does not properly handle symbolic link files. Instead
# of adding a symbolic link entry to the archive, the interface follows the link to
# copy the link's target file contents AND uses the target file name as the entry name.
#
# Example:
# 
# Suppose you had this folder structure:   
#    
#        $ tree /tmp/example
#        /tmp/example
#        |-- regular_file.txt
#        |-- symbolic_link -> regular_file.txt
#  
# In MATLAB, if the symbolic link and its target file are in the same folder, then the symbolic link
# is not included as one of the files to be packaged:
#
#       >> opts = matlab.addons.toolbox.ToolboxOptions("/tmp/example", "dummy-identifier");
#       >> opts.ToolboxFiles
#
#       ans = 
#
#            "/private/tmp/example/regular_file.txt"
#
# This is a bug. 
#
# Why is this a problem? On macOS, building the Arrow C++ bindings generates the following files:
#     
#        $ tree arrow/matlab/install/arrow_matlab/+libmexclass/+proxy/ 
#        . 
#        |-- libarrow.1700.0.0.dylib
#        |-- libarrow.1700.dylib -> libarrow.1700.0.0.dylib
#        |-- libarrow.dylib -> libarrow.1700.dylib
#
# When arrow/matlab/install/arrow_matlab is packaged into an MLTBX file, only the "regular file"
# libarrow.1700.0.0.dylib is included. This is problematic because building the MATLAB creates
# a shared library named libarrowproxy.dylib, which links against libarrow.1700.dylib 
# - not libarrow.1700.0.0.dylib:
#
#        $ otool -L libarrowproxy.dylib | grep -E '@rpath/libarrow\.'
#	            @rpath/libarrow.1700.dylib
#
# To avoid a run-time linker issue, we need to update the name of libarrowproxy.dylib's 
# dependent shared library from @rpath/libarrow.1700.dylib to @rpath/libarrow.1700.0.0.dylib.
#
# ==============================================================================================
#
# Issue Number 2:
#
# We currently create one MLTBX file to package the MATLAB Arrow interface for win64, glnxa64,
# maci64, and maca64. We do this because the MATLAB File Exchange <-> GitHub Releases integration 
# does not support platform-specific MLTBX files as of this moment. This mostly works, except rename
# either the maci64 shared libraries or the maca64 shared libraries to avoid duplicate filenames
# in the MLTBX file because maci64 and maca64 shared libraries have the same extension: dylib.
# For example, the shared library libarrow.1700.0.0.dylib is produced when building Arrow on
# macOS AND Intel-based macOS.
#
# To workaround this issue, we have decided to append the suffixes arm64 and x64 to the shared 
# libraries for ARM-based macOS and Intel-based macOS, respectively.

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <dylib-dir> <arch>"
  exit
fi

DYLIB_DIR=${1}
ARCH=${2}

if ["$ARCH" == "arm64"]; then
  IS_ARM64=1
elif ["$ARCH" == "x64"]; then
  IS_ARM64=0
else
  echo "<arch> must be arm64 or x64"
  exit
fi

ORIG_DIR=$(pwd)

cd ${DYLIB_DIR}

ORIG_LIBARROW_DYLIB="$(find . -name 'libarrow.dylib' | xargs basename)"
ORIG_LIBARROW_MAJOR_DYLIB="$(find . -name 'libarrow.*.dylib' -type l | xargs basename)"
ORIG_LIBARROW_MAJOR_MINOR_PATCH_DYLIB="$(echo libarrow.*.*.dylib)"
ORIG_LIBMEXCLASS_DYLIB="$(find . -name 'libmexclass.dylib' | xargs basename)"
ORIG_LIBARROWPROXY_DYLIB="$(find . -name 'libarrowproxy.dylib' | xargs basename)"
if [$IS_ARM64 -eq 1]; then
  MEX_GATEWAY="$(find . -name 'gateway.mexmaca64' | xargs basename)"
else
  MEX_GATEWAY="$(find . -name 'gateway.mexmaci64' | xargs basename)"
fi

MAJOR_MINOR_PATCH_VERSION=${ORIG_LIBARROW_MAJOR_MINOR_PATCH_DYLIB#*.}
MAJOR_MINOR_PATCH_VERSION=${MAJOR_MINOR_PATCH_VERSION%.*}

NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB="libarrow_${ARCH}.${MAJOR_MINOR_PATCH_VERSION}.dylib"
NEW_LIBARROWPROXY_DYLIB="libarrowproxy_${ARCH}.dylib"
NEW_LIBMEXCLASS_DYLIB="libmexclass_${ARCH}.dylib"

# Delete the symbolic links. These files are not included in the packaged MLTBX file. 
rm ${ORIG_LIBARROW_MAJOR_DYLIB}
rm ${ORIG_LIBARROW_DYLIB}

# Rename libarrow.*.*.*.dylib to libarrow_(arm64|x64).*.*.*.dylib (e.g. libarrow.1700.0.0.dylib -> libarrow_(arm64|x64).1700.0.0.dylib)
mv ${ORIG_LIBARROW_MAJOR_MINOR_PATCH_DYLIB} ${NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB}
# Rename libarrowproxy.dylib to libarrowproxy_(arm64|x64).dylib
mv ${ORIG_LIBARROWPROXY_DYLIB} ${NEW_LIBARROWPROXY_DYLIB}
# Rename libmexclass.dylib to libmexclass_(arm64|x64).dylib
mv ${ORIG_LIBMEXCLASS_DYLIB} ${NEW_LIBMEXCLASS_DYLIB}

# Update the identificaton names of the renamed dynamic libraries
install_name_tool -id @rpath/${NEW_LIBMEXCLASS_DYLIB} ${NEW_LIBMEXCLASS_DYLIB}
install_name_tool -id @rpath/${NEW_LIBARROWPROXY_DYLIB} ${NEW_LIBARROWPROXY_DYLIB}
install_name_tool -id @rpath/${NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB} ${NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB}

# Change install name of dependent shared library libarrow.*.*.*.dylib to libarrow_arm64.*.*.*.dylib in libarrowproxy_(arm64|x64).dylib
install_name_tool -change @rpath/${ORIG_LIBARROW_MAJOR_DYLIB} @rpath/${NEW_LIBARROW_MAJOR_MINOR_PATCH_DYLIB} ${NEW_LIBARROWPROXY_DYLIB}
# Change install name of dependent shared library libmexclass.dylib to libmexclass_(arm64|x64).*.*.*.dylib libarrowproxy_(arm64|x64).dylib
install_name_tool -change @rpath/${ORIG_LIBMEXCLASS_DYLIB} @rpath/${NEW_LIBMEXCLASS_DYLIB} ${NEW_LIBARROWPROXY_DYLIB}

# Change install name of dependent shared library libmexclass.dylib to libmexclass_(arm64|x64).dylib in gateway.mexmaca64
install_name_tool -change @rpath/${ORIG_LIBMEXCLASS_DYLIB} @rpath/${NEW_LIBMEXCLASS_DYLIB} ${MEX_GATEWAY}
# Change install name of dependent shared library libarrowproxy.dylib to libarrowproxy_(arm64|x64).dylib in gateway.mexmaca64
install_name_tool -change @rpath/${ORIG_LIBARROWPROXY_DYLIB} @rpath/${NEW_LIBARROWPROXY_DYLIB} ${MEX_GATEWAY}

cd ${ORIG_DIR}