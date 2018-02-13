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

DOC_MODULE = arrow-glib

DOC_MAIN_SGML_FILE = $(DOC_MODULE)-docs.xml

DOC_SOURCE_DIR =				\
	$(top_srcdir)/arrow-glib

SCAN_OPTIONS =						\
	--deprecated-guards="GARROW_DISABLE_DEPRECATED"

MKDB_OPTIONS =					\
	--name-space=garrow			\
	--source-suffixes="c,cpp,h"

HFILE_GLOB =					\
	$(top_srcdir)/arrow-glib/*.h

IGNORE_HFILES =

CFILE_GLOB =					\
	$(top_srcdir)/arrow-glib/*.cpp

AM_CPPFLAGS =					\
	-I$(top_builddir)			\
	-I$(top_srcdir)

AM_CFLAGS =					\
	$(GLIB_CFLAGS)				\
	$(ARROW_CFLAGS)

GTKDOC_LIBS =						\
	$(top_builddir)/arrow-glib/libarrow-glib.la

if HAVE_ARROW_GPU
DOC_SOURCE_DIR +=				\
	$(top_srcdir)/arrow-gpu-glib
HFILE_GLOB +=					\
	$(top_srcdir)/arrow-gpu-glib/*.h
CFILE_GLOB +=					\
	$(top_srcdir)/arrow-gpu-glib/*.cpp
GTKDOC_LIBS +=							\
	$(top_builddir)/arrow-gpu-glib/libarrow-gpu-glib.la
endif

include $(srcdir)/gtk-doc.make

CLEANFILES +=					\
	$(DOC_MODULE)-decl-list.txt		\
	$(DOC_MODULE)-decl.txt			\
	$(DOC_MODULE)-overrides.txt		\
	$(DOC_MODULE)-sections.txt		\
	$(DOC_MODULE).types

EXTRA_DIST +=					\
	entities.xml.in				\
	meson.build
