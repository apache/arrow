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

m4_define([arrow_glib_version],
           m4_esyscmd(grep "^  <version>" "$(dirname $0)/../java/pom.xml" | \
                        sed -E \
                            -e 's/(^  <version>)//g' \
                            -e 's/(<\/version>$)//g' | \
                        tr -d '\n'))
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

GARROW_VERSION_MAJOR=$(echo "arrow_glib_version" | \
                         sed -E -e 's/^([[0-9]]+)\..+$/\1/' | \
                         tr -d '\n')
GARROW_VERSION_MINOR=$(echo "arrow_glib_version" | \
                         sed -E -e 's/^[[0-9]]+\.([[0-9]]+)\..+$/\1/' | \
                         tr -d '\n')
GARROW_VERSION_MICRO=$(echo "arrow_glib_version" | \
                         sed -E -e 's/^[[0-9]]+\.[[0-9]]+\.([[0-9]]+).*$/\1/' | \
                         tr -d '\n')
if echo "arrow_glib_version" | grep -- "-" > /dev/null; then
  GARROW_VERSION_TAG=$(echo "arrow_glib_version" | \
                         sed -E -e 's/^[[0-9]]+\.[[0-9]]+\.[[0-9]]+-(.+)$/\1/' | \
                         tr -d '\n')
else
  GARROW_VERSION_TAG=
fi
AC_SUBST(GARROW_VERSION_MAJOR)
AC_SUBST(GARROW_VERSION_MINOR)
AC_SUBST(GARROW_VERSION_MICRO)
AC_SUBST(GARROW_VERSION_TAG)

AC_CANONICAL_HOST
AC_MSG_CHECKING([for macOS])
case "$host_os" in
darwin*)
  os_macos=yes
  ;;
*)
  os_macos=no
  ;;
esac
AC_MSG_RESULT([$os_macos])
AM_CONDITIONAL(OS_MACOS, test "$os_macos" = "yes")

AC_PROG_CC
AC_PROG_CXX
AX_CXX_COMPILE_STDCXX_11([ext], [mandatory])
LT_INIT

GARROW_CFLAGS="-Wall"
GARROW_CXXFLAGS="-Wall"
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
  USE_ARROW_BUILD_DIR=no

  PKG_CHECK_MODULES([ARROW], [arrow arrow-compute])
  PKG_CHECK_MODULES([ARROW_GPU],
                    [arrow-gpu],
                    [HAVE_ARROW_GPU=yes],
                    [HAVE_ARROW_GPU=no])
else
  USE_ARROW_BUILD_DIR=yes

  ARROW_BUILD_DIR="${GARROW_ARROW_CPP_BUILD_DIR}"
  AC_SUBST(ARROW_BUILD_DIR)

  ARROW_SOURCE_INCLUDE_DIR="\$(abs_top_srcdir)/../cpp/src"
  ARROW_BUILD_INCLUDE_DIR="${GARROW_ARROW_CPP_BUILD_DIR}/src"
  ARROW_LIB_DIR="${GARROW_ARROW_CPP_BUILD_DIR}/${GARROW_ARROW_CPP_BUILD_TYPE}"
  AC_SUBST(ARROW_LIB_DIR)

  ARROW_CFLAGS="-I${ARROW_BUILD_INCLUDE_DIR} -I${ARROW_SOURCE_INCLUDE_DIR}"
  ARROW_LIBS="-L${ARROW_LIB_DIR} -larrow"
  AC_SUBST(ARROW_CFLAGS)
  AC_SUBST(ARROW_LIBS)

  ARROW_GPU_CFLAGS=""
  if test -f "${GARROW_ARROW_CPP_BUILD_DIR}/src/arrow/gpu/arrow-gpu.pc"; then
    HAVE_ARROW_GPU=yes
    ARROW_GPU_LIBS="-larrow_gpu"
  else
    HAVE_ARROW_GPU=no
    ARROW_GPU_LIBS=""
  fi
  AC_SUBST(ARROW_GPU_CFLAGS)
  AC_SUBST(ARROW_GPU_LIBS)
fi

AM_CONDITIONAL([USE_ARROW_BUILD_DIR],
               [test "$USE_ARROW_BUILD_DIR" = "yes"])

AM_CONDITIONAL([HAVE_ARROW_GPU], [test "$HAVE_ARROW_GPU" = "yes"])
if test "$HAVE_ARROW_GPU" = "yes"; then
  AC_DEFINE(HAVE_ARROW_GPU, [1], [Define to 1 if Apache Arrow supports GPU.])
fi

exampledir="\$(datadir)/arrow-glib/example"
AC_SUBST(exampledir)

AC_CONFIG_FILES([
  Makefile
  arrow-glib/Makefile
  arrow-glib/arrow-glib.pc
  arrow-glib/version.h
  arrow-gpu-glib/Makefile
  arrow-gpu-glib/arrow-gpu-glib.pc
  doc/Makefile
  doc/reference/Makefile
  doc/reference/entities.xml
  example/Makefile
  example/lua/Makefile
  tool/Makefile
])

AC_OUTPUT
