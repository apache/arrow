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

m4_define([arrow_glib_version], 0.14.0)
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

LT_INIT
LT_CURRENT=${GARROW_VERSION_MINOR}
LT_REVISION=${GARROW_VERSION_MICRO}
LT_AGE=0
LT_VERSION_INFO="\$(LT_CURRENT):\$(LT_REVISION):\$(LT_AGE)"
AC_SUBST(LT_CURRENT)
AC_SUBST(LT_REVISION)
AC_SUBST(LT_AGE)
AC_SUBST(LT_VERSION_INFO)

AC_PROG_CC
AC_PROG_CXX
AX_CXX_COMPILE_STDCXX_11([ext], [mandatory])

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

AM_PATH_GLIB_2_0([2.32.4],
                 [],
                 [AC_MSG_ERROR(GLib isn't available)],
                 [gobject gio])

GOBJECT_INTROSPECTION_REQUIRE([1.32.1])
GTK_DOC_CHECK([1.18-2])

AC_ARG_WITH(arrow-cpp-build-type,
  [AS_HELP_STRING([--with-arrow-cpp-build-type=TYPE],
                  [-DCMAKE_BUILD_TYPE option value for Arrow C++ (default=release)])],
  [GARROW_ARROW_CPP_BUILD_TYPE="$withval"],
  [GARROW_ARROW_CPP_BUILD_TYPE="release"])

ARROW_CUDA_PKG_CONFIG_PATH=""
AC_ARG_WITH(arrow-cpp-build-dir,
  [AS_HELP_STRING([--with-arrow-cpp-build-dir=PATH],
                  [Use this option to build with not installed Arrow C++])],
  [GARROW_ARROW_CPP_BUILD_DIR="$withval"],
  [GARROW_ARROW_CPP_BUILD_DIR=""])
if test "x$GARROW_ARROW_CPP_BUILD_DIR" = "x"; then
  USE_ARROW_BUILD_DIR=no

  PKG_CHECK_MODULES([ARROW], [arrow arrow-compute])
  _PKG_CONFIG(ARROW_LIB_DIR, [variable=libdir], [arrow])
  ARROW_LIB_DIR="$pkg_cv_ARROW_LIB_DIR"
  PKG_CHECK_MODULES([ARROW_ORC],
                    [arrow-orc],
                    [HAVE_ARROW_ORC=yes],
                    [HAVE_ARROW_ORC=no])
  PKG_CHECK_MODULES([ARROW_CUDA],
                    [arrow-cuda],
                    [HAVE_ARROW_CUDA=yes],
                    [HAVE_ARROW_CUDA=no])
  PKG_CHECK_MODULES([GANDIVA],
                    [gandiva],
                    [HAVE_GANDIVA=yes],
                    [HAVE_GANDIVA=no])
  PKG_CHECK_MODULES([PARQUET],
                    [parquet],
                    [HAVE_PARQUET=yes],
                    [HAVE_PARQUET=no])
  PKG_CHECK_MODULES([PLASMA],
                    [plasma],
                    [HAVE_PLASMA=yes],
                    [HAVE_PLASMA=no])
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

  if test -f "${GARROW_ARROW_CPP_BUILD_DIR}/src/arrow/adapters/orc/arrow-orc.pc"; then
    HAVE_ARROW_ORC=yes
  else
    HAVE_ARROW_ORC=no
  fi

  ARROW_CUDA_CFLAGS=""
  if test -f "${GARROW_ARROW_CPP_BUILD_DIR}/src/arrow/gpu/arrow-cuda.pc"; then
    HAVE_ARROW_CUDA=yes
    ARROW_CUDA_LIBS="-larrow_cuda"
    ARROW_CUDA_PKG_CONFIG_PATH="\$(ARROW_BUILD_DIR)/src/arrow/gpu"
  else
    HAVE_ARROW_CUDA=no
    ARROW_CUDA_LIBS=""
    ARROW_CUDA_PKG_CONFIG_PATH=""
  fi
  AC_SUBST(ARROW_CUDA_CFLAGS)
  AC_SUBST(ARROW_CUDA_LIBS)
  AC_SUBST(ARROW_CUDA_PKG_CONFIG_PATH)

  GANDIVA_CFLAGS=""
  if test -f "${GARROW_ARROW_CPP_BUILD_DIR}/src/gandiva/gandiva.pc"; then
    HAVE_GANDIVA=yes
    GANDIVA_LIBS="-lgandiva"
  else
    HAVE_GANDIVA=no
    GANDIVA_LIBS=""
  fi
  AC_SUBST(GANDIVA_CFLAGS)
  AC_SUBST(GANDIVA_LIBS)

  PARQUET_CFLAGS=""
  if test -f "${GARROW_ARROW_CPP_BUILD_DIR}/src/parquet/parquet.pc"; then
    HAVE_PARQUET=yes
    PARQUET_LIBS="-lparquet"
  else
    HAVE_PARQUET=no
    PARQUET_LIBS=""
  fi
  AC_SUBST(PARQUET_CFLAGS)
  AC_SUBST(PARQUET_LIBS)

  PLASMA_CFLAGS=""
  if test -f "${GARROW_ARROW_CPP_BUILD_DIR}/src/plasma/plasma.pc"; then
    HAVE_PLASMA=yes
    PLASMA_LIBS="-lplasma"
  else
    HAVE_PLASMA=no
    PLASMA_LIBS=""
  fi
  AC_SUBST(PLASMA_CFLAGS)
  AC_SUBST(PLASMA_LIBS)
fi

AM_CONDITIONAL([USE_ARROW_BUILD_DIR],
               [test "$USE_ARROW_BUILD_DIR" = "yes"])

AM_CONDITIONAL([HAVE_ARROW_ORC], [test "$HAVE_ARROW_ORC" = "yes"])
if test "$HAVE_ARROW_ORC" = "yes"; then
  AC_DEFINE(HAVE_ARROW_ORC, [1], [Define to 1 if Apache Arrow supports ORC.])
fi

AM_CONDITIONAL([HAVE_ARROW_CUDA], [test "$HAVE_ARROW_CUDA" = "yes"])
if test "$HAVE_ARROW_CUDA" = "yes"; then
  ARROW_CUDA_GLIB_PACKAGE="arrow-cuda-glib"
  PLASMA_ARROW_CUDA_PKG_CONFIG_PATH=":\$(abs_top_builddir)/arrow-cuda-glib"
  if test -n "${ARROW_CUDA_PKG_CONFIG_PATH}"; then
    PLASMA_ARROW_CUDA_PKG_CONFIG_PATH=":${ARROW_CUDA_PKG_CONFIG_PATH}${PLASMA_ARROW_CUDA_PKG_CONFIG_PATH}"
  fi
  AC_DEFINE(HAVE_ARROW_CUDA, [1], [Define to 1 if Apache Arrow supports CUDA.])
else
  ARROW_CUDA_GLIB_PACKAGE=""
  PLASMA_ARROW_CUDA_PKG_CONFIG_PATH=""
fi
AC_SUBST(ARROW_CUDA_GLIB_PACKAGE)
AC_SUBST(PLASMA_ARROW_CUDA_PKG_CONFIG_PATH)

AM_CONDITIONAL([HAVE_GANDIVA], [test "$HAVE_GANDIVA" = "yes"])
if test "$HAVE_GANDIVA" = "yes"; then
  AC_DEFINE(HAVE_GANDIVA, [1], [Define to 1 if Gandiva exists.])
fi

AM_CONDITIONAL([HAVE_PARQUET], [test "$HAVE_PARQUET" = "yes"])
if test "$HAVE_PARQUET" = "yes"; then
  AC_DEFINE(HAVE_PARQUET, [1], [Define to 1 if Apache Parquet exists.])
fi

AM_CONDITIONAL([HAVE_PLASMA], [test "$HAVE_PLASMA" = "yes"])
if test "$HAVE_PLASMA" = "yes"; then
  AC_DEFINE(HAVE_PLASMA, [1], [Define to 1 if Plasma exists.])
fi

exampledir="\$(datadir)/arrow-glib/example"
AC_SUBST(exampledir)

AC_CONFIG_FILES([
  Makefile
  arrow-cuda-glib/Makefile
  arrow-cuda-glib/arrow-cuda-glib.pc
  arrow-glib/Makefile
  arrow-glib/arrow-glib.pc
  arrow-glib/arrow-orc-glib.pc
  arrow-glib/version.h
  gandiva-glib/Makefile
  gandiva-glib/gandiva-glib.pc
  parquet-glib/Makefile
  parquet-glib/parquet-glib.pc
  plasma-glib/Makefile
  plasma-glib/plasma-glib.pc
  doc/Makefile
  doc/arrow-glib/Makefile
  doc/arrow-glib/entities.xml
  doc/gandiva-glib/Makefile
  doc/gandiva-glib/entities.xml
  doc/parquet-glib/Makefile
  doc/parquet-glib/entities.xml
  doc/plasma-glib/Makefile
  doc/plasma-glib/entities.xml
  example/Makefile
  example/lua/Makefile
])

AC_OUTPUT
