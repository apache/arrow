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

CLEANFILES =
DISTCLEANFILES =

EXTRA_DIST =					\
	meson.build

AM_CPPFLAGS =					\
	-I$(top_builddir)			\
	-I$(top_srcdir)

AM_CFLAGS =					\
	$(GLIB_CFLAGS)				\
	$(GARROW_CFLAGS)			\
	$(GPARQUET_CFLAGS)

if HAVE_PARQUET
lib_LTLIBRARIES =				\
	libparquet-glib.la

libparquet_glib_la_CXXFLAGS =			\
	$(GLIB_CFLAGS)				\
	$(ARROW_CFLAGS)				\
	$(PARQUET_CFLAGS)			\
	$(GARROW_CFLAGS)			\
	$(GPARQUET_CXXFLAGS)

libparquet_glib_la_LDFLAGS =			\
	-version-info $(LT_VERSION_INFO)	\
	-no-undefined

libparquet_glib_la_LIBADD =			\
	$(GLIB_LIBS)				\
	$(ARROW_LIBS)				\
	$(PARQUET_LIBS)				\
	../arrow-glib/libarrow-glib.la

libparquet_glib_la_headers =			\
	arrow-file-reader.h			\
	arrow-file-writer.h			\
	parquet-glib.h

libparquet_glib_la_sources =			\
	arrow-file-reader.cpp			\
	arrow-file-writer.cpp			\
	$(libparquet_glib_la_headers)

libparquet_glib_la_cpp_headers =		\
	arrow-file-reader.hpp			\
	arrow-file-writer.hpp			\
	parquet-glib.hpp

libparquet_glib_la_SOURCES =			\
	$(libparquet_glib_la_sources)		\
	$(libparquet_glib_la_cpp_headers)

parquet_glib_includedir = $(includedir)/parquet-glib
parquet_glib_include_HEADERS =			\
	$(libparquet_glib_la_headers)		\
	$(libparquet_glib_la_cpp_headers)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA =				\
	parquet-glib.pc

# GObject Introspection
if HAVE_INTROSPECTION
-include $(INTROSPECTION_MAKEFILE)
INTROSPECTION_GIRS =
INTROSPECTION_SCANNER_ARGS =
INTROSPECTION_SCANNER_ENV =
if USE_ARROW_BUILD_DIR
INTROSPECTION_SCANNER_ENV +=			\
	PKG_CONFIG_PATH=${abs_top_builddir}/arrow-glib:$(ARROW_BUILD_DIR)/src/arrow:$${PKG_CONFIG_PATH}
else
INTROSPECTION_SCANNER_ENV +=			\
	PKG_CONFIG_PATH=${abs_top_builddir}/arrow-glib:$${PKG_CONFIG_PATH}
endif
INTROSPECTION_COMPILER_ARGS =				\
	--includedir=$(abs_top_builddir)/arrow-glib

Parquet-1.0.gir: libparquet-glib.la
Parquet_1_0_gir_PACKAGES =			\
	arrow-glib
Parquet_1_0_gir_EXPORT_PACKAGES =		\
	parquet-glib
Parquet_1_0_gir_INCLUDES =			\
	Arrow-1.0
Parquet_1_0_gir_CFLAGS =			\
	$(AM_CPPFLAGS)
Parquet_1_0_gir_LIBS =
Parquet_1_0_gir_FILES = $(libparquet_glib_la_sources)
Parquet_1_0_gir_SCANNERFLAGS =					\
	--add-include-path=$(abs_top_builddir)/arrow-glib	\
	--library-path=$(ARROW_LIB_DIR)				\
	--warn-all						\
	--identifier-prefix=GParquet				\
	--symbol-prefix=gparquet
if OS_MACOS
Parquet_1_0_gir_LIBS +=				\
	arrow-glib				\
	parquet-glib
Parquet_1_0_gir_SCANNERFLAGS +=					\
	--no-libtool						\
	--library-path=$(abs_top_builddir)/arrow-glib/.libs	\
	--library-path=$(abs_builddir)/.libs
else
Parquet_1_0_gir_LIBS +=					\
	$(abs_top_builddir)/arrow-glib/libarrow-glib.la	\
	libparquet-glib.la
endif
INTROSPECTION_GIRS += Parquet-1.0.gir

girdir = $(datadir)/gir-1.0
gir_DATA = $(INTROSPECTION_GIRS)

typelibdir = $(libdir)/girepository-1.0
typelib_DATA = $(INTROSPECTION_GIRS:.gir=.typelib)

CLEANFILES +=					\
	$(gir_DATA)				\
	$(typelib_DATA)
endif
endif
