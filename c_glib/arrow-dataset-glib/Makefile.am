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

if HAVE_ARROW_DATASET
lib_LTLIBRARIES =				\
	libarrow-dataset-glib.la

libarrow_dataset_glib_la_CXXFLAGS =		\
	$(GLIB_CFLAGS)				\
	$(ARROW_DATASET_CFLAGS)			\
	$(GARROW_CXXFLAGS)

libarrow_dataset_glib_la_LDFLAGS =		\
	-version-info $(LT_VERSION_INFO)	\
	-no-undefined

libarrow_dataset_glib_la_LIBADD =		\
	$(GLIB_LIBS)				\
	../arrow-glib/libarrow-glib.la		\
	$(ARROW_DATASET_LIBS)

libarrow_dataset_glib_la_headers =		\
	arrow-dataset-glib.h			\
	scanner.h

libarrow_dataset_glib_la_sources =		\
	scanner.cpp				\
	$(libarrow_dataset_glib_la_headers)

libarrow_dataset_glib_la_cpp_headers =		\
	arrow-dataset-glib.hpp			\
	scanner.hpp

libarrow_dataset_glib_la_SOURCES =		\
	$(libarrow_dataset_glib_la_sources)	\
	$(libarrow_dataset_glib_la_cpp_headers)

arrow_dataset_glib_includedir =			\
	$(includedir)/arrow-dataset-glib
arrow_dataset_glib_include_HEADERS =		\
	$(libarrow_dataset_glib_la_headers)	\
	$(libarrow_dataset_glib_la_cpp_headers)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA =				\
	arrow-dataset-glib.pc

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

ArrowDataset-1.0.gir: libarrow-dataset-glib.la
ArrowDataset_1_0_gir_PACKAGES =			\
	arrow-glib
ArrowDataset_1_0_gir_EXPORT_PACKAGES =		\
	arrow-dataset-glib
ArrowDataset_1_0_gir_INCLUDES =			\
	Arrow-1.0
ArrowDataset_1_0_gir_CFLAGS =			\
	$(AM_CPPFLAGS)
ArrowDataset_1_0_gir_LIBS =
ArrowDataset_1_0_gir_FILES =			\
	$(libarrow_dataset_glib_la_sources)
ArrowDataset_1_0_gir_SCANNERFLAGS =				\
	--add-include-path=$(abs_builddir)/../arrow-glib	\
	--identifier-prefix=GAD					\
	--library-path=$(ARROW_LIB_DIR)				\
	--symbol-prefix=gad					\
	--warn-all
if OS_MACOS
ArrowDataset_1_0_gir_LIBS +=			\
	 arrow-glib				\
	 arrow-dataset-glib
ArrowDataset_1_0_gir_SCANNERFLAGS +=				\
	--no-libtool						\
	--library-path=$(abs_builddir)/../arrow-glib/.libs	\
	--library-path=$(abs_builddir)/.libs
else
ArrowDataset_1_0_gir_LIBS +=				\
	$(abs_builddir)/../arrow-glib/libarrow-glib.la	\
	libarrow-dataset-glib.la
endif

INTROSPECTION_GIRS += ArrowDataset-1.0.gir

girdir = $(datadir)/gir-1.0
gir_DATA = $(INTROSPECTION_GIRS)

typelibdir = $(libdir)/girepository-1.0
typelib_DATA = $(INTROSPECTION_GIRS:.gir=.typelib)

CLEANFILES +=					\
	$(gir_DATA)				\
	$(typelib_DATA)
endif
endif
