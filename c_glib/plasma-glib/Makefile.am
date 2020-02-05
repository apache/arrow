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
	-I$(top_srcdir)				\
	-DG_LOG_DOMAIN=\"Plasma\"

AM_CFLAGS =					\
	$(GLIB_CFLAGS)				\
	$(GARROW_CFLAGS)			\
	$(GPLASMA_CFLAGS)

PLASMA_ARROW_CUDA_LIBS =
PLASMA_INTROSPECTION_COMPILER_ARROW_CUDA_ARGS =
PLASMA_GIR_ARROW_CUDA_PACKAGE =
PLASMA_GIR_ARROW_CUDA_SCANNER_ADD_INCLUDE_PATH =
PLASMA_GIR_ARROW_CUDA_LIBS_MACOS =
PLASMA_GIR_ARROW_CUDA_SCANNER_LIBRARY_PATH_MACOS =
PLASMA_GIR_ARROW_CUDA_LIBS =
if HAVE_ARROW_CUDA
PLASMA_ARROW_CUDA_LIBS +=				\
	$(ARROW_CUDA_LIBS)				\
	../arrow-cuda-glib/libarrow-cuda-glib.la
PLASMA_INTROSPECTION_COMPILER_ARROW_CUDA_ARGS +=		\
	--includedir=$(abs_top_builddir)/arrow-cuda-glib
PLASMA_GIR_ARROW_CUDA_PACKAGE +=		\
	arrow-cuda-glib
PLASMA_GIR_ARROW_CUDA_SCANNER_ADD_INCLUDE_PATH +=		\
	--add-include-path=$(abs_top_builddir)/arrow-cuda-glib
PLASMA_GIR_ARROW_CUDA_LIBS_MACOS +=		\
	arrow-cuda-glib
PLASMA_GIR_ARROW_CUDA_SCANNER_LIBRARY_PATH_MACOS +=			\
	--library-path=$(abs_top_builddir)/arrow-cuda-glib/.libs
PLASMA_GIR_ARROW_CUDA_LIBS +=						\
	$(abs_top_builddir)/arrow-cuda-glib/libarrow-cuda-glib.la
endif

if HAVE_PLASMA
lib_LTLIBRARIES =				\
	libplasma-glib.la

libplasma_glib_la_CXXFLAGS =			\
	$(GLIB_CFLAGS)				\
	$(ARROW_CFLAGS)				\
	$(PLASMA_CFLAGS)			\
	$(GARROW_CFLAGS)			\
	$(GPLASMA_CFLAGS)

libplasma_glib_la_LDFLAGS =			\
	-version-info $(LT_VERSION_INFO)	\
	-no-undefined

libplasma_glib_la_LIBADD =			\
	$(GLIB_LIBS)				\
	$(ARROW_LIBS)				\
	$(PLASMA_LIBS)				\
	../arrow-glib/libarrow-glib.la		\
	$(PLASMA_ARROW_CUDA_LIBS)

libplasma_glib_la_headers =			\
	client.h				\
	object.h				\
	plasma-glib.h

libplasma_glib_la_sources =			\
	client.cpp				\
	object.cpp				\
	$(libplasma_glib_la_headers)

libplasma_glib_la_cpp_headers =			\
	client.hpp				\
	object.hpp				\
	plasma-glib.hpp

libplasma_glib_la_SOURCES =			\
	$(libplasma_glib_la_sources)		\
	$(libplasma_glib_la_cpp_headers)

plasma_glib_includedir = $(includedir)/plasma-glib
plasma_glib_include_HEADERS =			\
	$(libplasma_glib_la_headers)		\
	$(libplasma_glib_la_cpp_headers)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA =				\
	plasma-glib.pc

# GObject Introspection
if HAVE_INTROSPECTION
-include $(INTROSPECTION_MAKEFILE)
INTROSPECTION_GIRS =
INTROSPECTION_SCANNER_ARGS =
INTROSPECTION_SCANNER_ENV =
if USE_ARROW_BUILD_DIR
INTROSPECTION_SCANNER_ENV +=			\
	PKG_CONFIG_PATH=$(abs_top_builddir)/arrow-glib$(PLASMA_ARROW_CUDA_PKG_CONFIG_PATH):$(ARROW_BUILD_DIR)/src/arrow:$${PKG_CONFIG_PATH}
else
INTROSPECTION_SCANNER_ENV +=			\
	PKG_CONFIG_PATH=$(abs_top_builddir)/arrow-glib$(PLASMA_ARROW_CUDA_PKG_CONFIG_PATH):$${PKG_CONFIG_PATH}
endif
INTROSPECTION_COMPILER_ARGS =					\
	--includedir=$(abs_top_builddir)/arrow-glib		\
	$(PLASMA_INTROSPECTION_COMPILER_ARROW_CUDA_INCLUDEDIR)

Plasma-1.0.gir: libplasma-glib.la
Plasma_1_0_gir_PACKAGES =			\
	arrow-glib				\
	$(PLASMA_GIR_ARROW_CUDA_PACKAGE)
Plasma_1_0_gir_EXPORT_PACKAGES =		\
	plasma-glib
Plasma_1_0_gir_INCLUDES =			\
	Arrow-1.0
Plasma_1_0_gir_CFLAGS =			\
	$(AM_CPPFLAGS)
Plasma_1_0_gir_LIBS =
Plasma_1_0_gir_FILES = $(libplasma_glib_la_sources)
Plasma_1_0_gir_SCANNERFLAGS =					\
	--add-include-path=$(abs_top_builddir)/arrow-glib	\
	$(PLASMA_GIR_ARROW_CUDA_SCANNER_ADD_INCLUDE_PATH)	\
	--library-path=$(ARROW_LIB_DIR)				\
	--warn-all						\
	--identifier-prefix=GPlasma				\
	--symbol-prefix=gplasma
if OS_MACOS
Plasma_1_0_gir_LIBS +=				\
	arrow-glib				\
	$(PLASMA_GIR_ARROW_CUDA_LIBS_MACOS)	\
	plasma-glib
Plasma_1_0_gir_SCANNERFLAGS +=					\
	--no-libtool						\
	--library-path=$(abs_top_builddir)/arrow-glib/.libs	\
	$(PLASMA_GIR_ARROW_CUDA_SCANNER_LIBRARY_PATH_MACOS)	\
	--library-path=$(abs_builddir)/.libs
else
Plasma_1_0_gir_LIBS +=					\
	$(abs_top_builddir)/arrow-glib/libarrow-glib.la	\
	$(PLASMA_GIR_ARROW_CUDA_LIBS)			\
	libplasma-glib.la
endif
INTROSPECTION_GIRS += Plasma-1.0.gir

girdir = $(datadir)/gir-1.0
gir_DATA = $(INTROSPECTION_GIRS)

typelibdir = $(libdir)/girepository-1.0
typelib_DATA = $(INTROSPECTION_GIRS:.gir=.typelib)

CLEANFILES +=					\
	$(gir_DATA)				\
	$(typelib_DATA)
endif
endif
