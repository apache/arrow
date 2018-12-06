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

PLASMA_ARROW_CUDA_GTKDOC_LIBS =
if HAVE_ARROW_CUDA
PLASMA_ARROW_CUDA_GTKDOC_LIBS +=				\
	$(top_builddir)/arrow-cuda-glib/libarrow-cuda-glib.la
endif

if HAVE_PLASMA
DOC_MODULE = plasma-glib

DOC_MAIN_SGML_FILE = $(DOC_MODULE)-docs.xml

DOC_SOURCE_DIR =				\
	$(top_srcdir)/plasma-glib		\
	$(top_builddir)/plasma-glib

SCAN_OPTIONS =						\
	--deprecated-guards="GPLASMA_DISABLE_DEPRECATED"

MKDB_OPTIONS =					\
	--name-space=gplasma			\
	--source-suffixes="c,cpp,h"

HFILE_GLOB =					\
	$(top_srcdir)/plasma-glib/*.h

IGNORE_HFILES =

CFILE_GLOB =					\
	$(top_srcdir)/plasma-glib/*.cpp

AM_CPPFLAGS =					\
	-I$(top_builddir)			\
	-I$(top_srcdir)

AM_CFLAGS =					\
	$(GLIB_CFLAGS)				\
	$(ARROW_CFLAGS)				\
	$(PLASMA_CFLAGS)

GTKDOC_LIBS =						\
	$(top_builddir)/arrow-glib/libarrow-glib.la	\
	$(PLASMA_ARROW_CUDA_GTKDOC_LIBS)		\
	$(top_builddir)/plasma-glib/libplasma-glib.la

include $(top_srcdir)/gtk-doc.make

CLEANFILES +=					\
	$(DOC_MODULE)-decl-list.txt		\
	$(DOC_MODULE)-decl.txt			\
	$(DOC_MODULE)-overrides.txt		\
	$(DOC_MODULE)-sections.txt		\
	$(DOC_MODULE).types
else
EXTRA_DIST =
endif

EXTRA_DIST +=					\
	entities.xml.in				\
	meson.build
