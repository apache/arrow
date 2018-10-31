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
	$(GGANDIVA_CFLAGS)

if HAVE_GANDIVA
lib_LTLIBRARIES =				\
	libgandiva-glib.la

libgandiva_glib_la_CXXFLAGS =			\
	$(GLIB_CFLAGS)				\
	$(ARROW_CFLAGS)				\
	$(GANDIVA_CFLAGS)			\
	$(GARROW_CFLAGS)			\
	$(GGANDIVA_CFLAGS)

libgandiva_glib_la_LDFLAGS =			\
	-version-info $(LT_VERSION_INFO)	\
	-no-undefined

libgandiva_glib_la_LIBADD =			\
	$(GLIB_LIBS)				\
	$(ARROW_LIBS)				\
	$(GANDIVA_LIBS)				\
	../arrow-glib/libarrow-glib.la

libgandiva_glib_la_headers =			\
	expression.h				\
	gandiva-glib.h				\
	projector.h

libgandiva_glib_la_sources =			\
	expression.cpp				\
	projector.cpp				\
	$(libgandiva_glib_la_headers)

libgandiva_glib_la_cpp_headers =		\
	expression.hpp				\
	gandiva-glib.hpp			\
	projector.hpp

libgandiva_glib_la_SOURCES =			\
	$(libgandiva_glib_la_sources)		\
	$(libgandiva_glib_la_cpp_headers)

gandiva_glib_includedir = $(includedir)/gandiva-glib
gandiva_glib_include_HEADERS =			\
	$(libgandiva_glib_la_headers)		\
	$(libgandiva_glib_la_cpp_headers)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA =				\
	gandiva-glib.pc

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

Gandiva-1.0.gir: libgandiva-glib.la
Gandiva_1_0_gir_PACKAGES =			\
	arrow-glib
Gandiva_1_0_gir_EXPORT_PACKAGES =		\
	gandiva-glib
Gandiva_1_0_gir_INCLUDES =			\
	Arrow-1.0
Gandiva_1_0_gir_CFLAGS =			\
	$(AM_CPPFLAGS)
Gandiva_1_0_gir_LIBS =
Gandiva_1_0_gir_FILES = $(libgandiva_glib_la_sources)
Gandiva_1_0_gir_SCANNERFLAGS =					\
	--add-include-path=$(abs_top_builddir)/arrow-glib	\
	--library-path=$(ARROW_LIB_DIR)				\
	--warn-all						\
	--identifier-prefix=GGandiva				\
	--symbol-prefix=ggandiva
if OS_MACOS
Gandiva_1_0_gir_LIBS +=				\
	arrow-glib				\
	gandiva-glib
Gandiva_1_0_gir_SCANNERFLAGS +=					\
	--library-path=$(abs_top_builddir)/arrow-glib/.libs	\
	--library-path=$(abs_builddir)/.libs
else
Gandiva_1_0_gir_LIBS +=					\
	$(abs_top_builddir)/arrow-glib/libarrow-glib.la	\
	libgandiva-glib.la
endif
INTROSPECTION_GIRS += Gandiva-1.0.gir

girdir = $(datadir)/gir-1.0
gir_DATA = $(INTROSPECTION_GIRS)

typelibdir = $(libdir)/girepository-1.0
typelib_DATA = $(INTROSPECTION_GIRS:.gir=.typelib)

CLEANFILES +=					\
	$(gir_DATA)				\
	$(typelib_DATA)
endif
endif
