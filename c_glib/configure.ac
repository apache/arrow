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
AC_INIT([arrow-glib], arrow_glib_version, [kou@clear-code.com])
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

AM_PATH_GLIB_2_0([2.32.4], [], [], [gobject])

GOBJECT_INTROSPECTION_REQUIRE([1.32.1])
GTK_DOC_CHECK([1.18-2])

PKG_CHECK_MODULES([ARROW], [arrow])
PKG_CHECK_MODULES([ARROW_IO], [arrow-io])
PKG_CHECK_MODULES([ARROW_IPC], [arrow-ipc])

AC_CONFIG_FILES([
  Makefile
  arrow-glib/Makefile
  arrow-glib/arrow-glib.pc
  arrow-glib/arrow-io-glib.pc
  arrow-glib/arrow-ipc-glib.pc
  doc/Makefile
  doc/reference/Makefile
  example/Makefile
])

AC_OUTPUT
