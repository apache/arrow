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

# - Try to find the GNU sasl library (gsasl)
#
# Once done this will define
#
#  GSASL_FOUND - System has gnutls
#  GSASL_INCLUDE_DIR - The gnutls include directory
#  GSASL_LIBRARY - The libraries needed to use gnutls


IF (GSASL_INCLUDE_DIR AND GSASL_LIBRARY)
	# in cache already
  SET(GSASL_FIND_QUIETLY TRUE)
ENDIF (GSASL_INCLUDE_DIR AND GSASL_LIBRARY)

FIND_PATH(GSASL_INCLUDE_DIR gsasl.h)

FIND_LIBRARY(GSASL_LIBRARY gsasl)

INCLUDE(FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set GSASL_FOUND to TRUE if
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GSASL DEFAULT_MSG GSASL_LIBRARY GSASL_INCLUDE_DIR)

MARK_AS_ADVANCED(GSASL_INCLUDE_DIR GSASL_LIBRARY)
