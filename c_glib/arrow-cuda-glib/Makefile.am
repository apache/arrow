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

EXTRA_DIST =					\
	meson.build

AM_CPPFLAGS =					\
	-I$(top_builddir)			\
	-I$(top_srcdir)

if HAVE_ARROW_CUDA
lib_LTLIBRARIES =				\
	libarrow-cuda-glib.la

libarrow_cuda_glib_la_CXXFLAGS =		\
	$(GLIB_CFLAGS)				\
	$(ARROW_CFLAGS)				\
	$(ARROW_CUDA_CFLAGS)			\
	$(GARROW_CXXFLAGS)

libarrow_cuda_glib_la_LDFLAGS =			\
	-version-info $(LT_VERSION_INFO)	\
	-no-undefined

libarrow_cuda_glib_la_LIBADD =			\
	$(GLIB_LIBS)				\
	$(ARROW_LIBS)				\
	$(ARROW_CUDA_LIBS)			\
	../arrow-glib/libarrow-glib.la

libarrow_cuda_glib_la_headers =			\
	arrow-cuda-glib.h			\
	cuda.h

libarrow_cuda_glib_la_sources =			\
	cuda.cpp				\
	$(libarrow_cuda_glib_la_headers)

libarrow_cuda_glib_la_cpp_headers =		\
	arrow-cuda-glib.hpp			\
	cuda.hpp

libarrow_cuda_glib_la_SOURCES =			\
	$(libarrow_cuda_glib_la_sources)	\
	$(libarrow_cuda_glib_la_cpp_headers)

arrow_cuda_glib_includedir =			\
	$(includedir)/arrow-cuda-glib
arrow_cuda_glib_include_HEADERS =		\
	$(libarrow_cuda_glib_la_headers)	\
	$(libarrow_cuda_glib_la_cpp_headers)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA =				\
	arrow-cuda-glib.pc

if HAVE_INTROSPECTION
-include $(INTROSPECTION_MAKEFILE)
INTROSPECTION_GIRS =
INTROSPECTION_SCANNER_ARGS =
INTROSPECTION_SCANNER_ENV =
if USE_ARROW_BUILD_DIR
INTROSPECTION_SCANNER_ENV +=			\
	PKG_CONFIG_PATH=${abs_builddir}/../arrow-glib:$(ARROW_BUILD_DIR)/src/arrow:$${PKG_CONFIG_PATH}
else
INTROSPECTION_SCANNER_ENV +=			\
	PKG_CONFIG_PATH=${abs_builddir}/../arrow-glib:$${PKG_CONFIG_PATH}
endif
INTROSPECTION_COMPILER_ARGS =			\
	--includedir=$(abs_builddir)/../arrow-glib

ArrowCUDA-1.0.gir: libarrow-cuda-glib.la
ArrowCUDA_1_0_gir_PACKAGES =			\
	arrow-glib
ArrowCUDA_1_0_gir_EXPORT_PACKAGES =		\
	arrow-cuda-glib
ArrowCUDA_1_0_gir_INCLUDES =			\
	Arrow-1.0
ArrowCUDA_1_0_gir_CFLAGS =			\
	$(AM_CPPFLAGS)
ArrowCUDA_1_0_gir_LIBS =
ArrowCUDA_1_0_gir_FILES =			\
	$(libarrow_cuda_glib_la_sources)
ArrowCUDA_1_0_gir_SCANNERFLAGS =				\
	--library-path=$(ARROW_LIB_DIR)				\
	--warn-all						\
	--add-include-path=$(abs_builddir)/../arrow-glib	\
	--identifier-prefix=GArrowCUDA				\
	--symbol-prefix=garrow_cuda
if OS_MACOS
ArrowCUDA_1_0_gir_LIBS +=			\
	 arrow-glib				\
	 arrow-cuda-glib
ArrowCUDA_1_0_gir_SCANNERFLAGS +=				\
	--no-libtool						\
	--library-path=$(abs_builddir)/../arrow-glib/.libs	\
	--library-path=$(abs_builddir)/.libs
else
ArrowCUDA_1_0_gir_LIBS +=				\
	$(abs_builddir)/../arrow-glib/libarrow-glib.la	\
	libarrow-cuda-glib.la
endif

INTROSPECTION_GIRS += ArrowCUDA-1.0.gir

girdir = $(datadir)/gir-1.0
gir_DATA = $(INTROSPECTION_GIRS)

typelibdir = $(libdir)/girepository-1.0
typelib_DATA = $(INTROSPECTION_GIRS:.gir=.typelib)

CLEANFILES +=					\
	$(gir_DATA)				\
	$(typelib_DATA)
endif
endif
