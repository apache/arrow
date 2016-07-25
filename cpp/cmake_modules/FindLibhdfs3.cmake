#   Copyright 2011-2015 Quickstep Technologies LLC.
#   Copyright 2015 Pivotal Software, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# Module to find the Pivotal libhdfs3.

find_path(LIBHDFS3_INCLUDE_DIR hdfs/hdfs.h PATHS ${LIBHDFS3_ROOT} NO_DEFAULT_PATH PATH_SUFFIXES "include")

find_library(LIBHDFS3_LIBRARY NAMES hdfs3 libhdfs3 PATHS ${LIBHDFS3_ROOT} NO_DEFAULT_PATH PATH_SUFFIXES "lib")

# Linking against libhdfs3 also requires linking against gsasl and kerberos.
find_package(GSasl REQUIRED)
find_package(Kerberos REQUIRED)

set(LIBHDFS3_LIBRARIES ${LIBHDFS3_LIBRARY}
                       ${GSASL_LIBRARY}
                       ${KERBEROS_LIBRARY})
set(LIBHDFS3_INCLUDE_DIRS ${LIBHDFS3_INCLUDE_DIR}
                          ${GSASL_INCLUDE_DIR}
                          ${KERBEROS_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Libhdfs3 DEFAULT_MSG
                                  LIBHDFS3_LIBRARY LIBHDFS3_INCLUDE_DIR)

mark_as_advanced(LIBHDFS3_INCLUDE_DIR LIBHDFS3_LIBRARY)
