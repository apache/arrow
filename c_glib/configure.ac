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

AC_PREREQ(2.65)

m4_define([arrow_glib_version], m4_include(version))
AC_INIT([arrow-glib],
        arrow_glib_version,
        [https://issues.apache.org/jira/browse/ARROW],
        [apache-arrow-glib])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])

AC_CONFIG_SRCDIR([arrow-glib/arrow-glib.h])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE([1.13 foreign])
AM_SILENT_RULES([yes])

AC_PROG_CC
AC_PROG_CXX
AX_CXX_COMPILE_STDCXX_11([ext], [mandatory])
LT_INIT

GARROW_CFLAGS="-Wall -Wconversion"
GARROW_CXXFLAGS="-Wall -Wconversion"
AC_ARG_ENABLE(debug,
  [AS_HELP_STRING([--enable-debug],
                  [Use debug flags (default=no)])],
  [GARROW_DEBUG="$enableval"],
  [GARROW_DEBUG="no"])
if test "x$GARROW_DEBUG" != "xno"; then
  GARROW_DEBUG="yes"
  if test "$CLANG" = "yes"; then
    CFLAGS="$CFLAGS -O0 -g"
    CXXFLAGS="$CXXFLAGS -O0 -g"
  elif test "$GCC" = "yes"; then
    CFLAGS="$CFLAGS -O0 -g3"
    CXXFLAGS="$CXXFLAGS -O0 -g3"
  fi
fi
AC_SUBST(GARROW_CFLAGS)
AC_SUBST(GARROW_CXXFLAGS)

AM_PATH_GLIB_2_0([2.32.4], [], [], [gobject gio])

GOBJECT_INTROSPECTION_REQUIRE([1.32.1])
GTK_DOC_CHECK([1.18-2])

AC_ARG_WITH(arrow-cpp-build-type,
  [AS_HELP_STRING([--with-arrow-cpp-build-type=TYPE],
                  [-DCMAKE_BUILD_TYPE option value for Arrow C++ (default=Release)])],
  [GARROW_ARROW_CPP_BUILD_TYPE="$withval"],
  [GARROW_ARROW_CPP_BUILD_TYPE="Release"])

AC_ARG_WITH(arrow-cpp-build-dir,
  [AS_HELP_STRING([--with-arrow-cpp-build-dir=PATH],
                  [Use this option to build with not installed Arrow C++])],
  [GARROW_ARROW_CPP_BUILD_DIR="$withval"],
  [GARROW_ARROW_CPP_BUILD_DIR=""])
if test "x$GARROW_ARROW_CPP_BUILD_DIR" = "x"; then
  PKG_CHECK_MODULES([ARROW], [arrow])
else
  ARROW_INCLUDE_DIR="\$(abs_top_srcdir)/../cpp/src"
  ARROW_LIB_DIR="${GARROW_ARROW_CPP_BUILD_DIR}/${GARROW_ARROW_CPP_BUILD_TYPE}"

  ARROW_CFLAGS="-I${ARROW_INCLUDE_DIR}"

  ARROW_LIBS="-L${ARROW_LIB_DIR} -larrow"

  AC_SUBST(ARROW_LIB_DIR)

  AC_SUBST(ARROW_CFLAGS)
  AC_SUBST(ARROW_LIBS)
fi

exampledir="\$(datadir)/arrow-glib/example"
AC_SUBST(exampledir)

AC_CONFIG_FILES([
  Makefile
  arrow-glib/Makefile
  arrow-glib/arrow-glib.pc
  doc/Makefile
  doc/reference/Makefile
  example/Makefile
  example/lua/Makefile
])

AC_OUTPUT
