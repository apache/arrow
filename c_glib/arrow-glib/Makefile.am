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

EXTRA_DIST =

AM_CPPFLAGS =					\
	-I$(top_builddir)			\
	-I$(top_srcdir)

AM_CFLAGS =					\
	$(GLIB_CFLAGS)				\
	$(GARROW_CFLAGS)

# libarrow-glib
lib_LTLIBRARIES =				\
	libarrow-glib.la

libarrow_glib_la_CXXFLAGS =			\
	$(GLIB_CFLAGS)				\
	$(ARROW_CFLAGS)				\
	$(GARROW_CXXFLAGS)

libarrow_glib_la_LIBADD =			\
	$(GLIB_LIBS)				\
	$(ARROW_LIBS)

libarrow_glib_la_headers =			\
	array.h					\
	array-builder.h				\
	arrow-glib.h				\
	binary-array.h				\
	binary-array-builder.h			\
	binary-data-type.h			\
	boolean-array.h				\
	boolean-array-builder.h			\
	boolean-data-type.h			\
	buffer.h				\
	chunked-array.h				\
	column.h				\
	data-type.h				\
	double-array.h				\
	double-array-builder.h			\
	double-data-type.h			\
	error.h					\
	field.h					\
	float-array.h				\
	float-array-builder.h			\
	float-data-type.h			\
	int8-array.h				\
	int8-array-builder.h			\
	int8-data-type.h			\
	int16-array.h				\
	int16-array-builder.h			\
	int16-data-type.h			\
	int32-array.h				\
	int32-array-builder.h			\
	int32-data-type.h			\
	int64-array.h				\
	int64-array-builder.h			\
	int64-data-type.h			\
	list-array.h				\
	list-array-builder.h			\
	list-data-type.h			\
	null-array.h				\
	null-data-type.h			\
	record-batch.h				\
	schema.h				\
	string-array.h				\
	string-array-builder.h			\
	string-data-type.h			\
	struct-array.h				\
	struct-array-builder.h			\
	struct-data-type.h			\
	table.h					\
	type.h					\
	uint8-array.h				\
	uint8-array-builder.h			\
	uint8-data-type.h			\
	uint16-array.h				\
	uint16-array-builder.h			\
	uint16-data-type.h			\
	uint32-array.h				\
	uint32-array-builder.h			\
	uint32-data-type.h			\
	uint64-array.h				\
	uint64-array-builder.h			\
	uint64-data-type.h

libarrow_glib_la_headers +=			\
	file.h					\
	file-mode.h				\
	file-output-stream.h			\
	input-stream.h				\
	memory-mapped-file.h			\
	output-stream.h				\
	random-access-file.h			\
	readable.h				\
	writeable.h				\
	writeable-file.h

libarrow_glib_la_headers +=			\
	file-reader.h				\
	file-writer.h				\
	stream-reader.h				\
	stream-writer.h				\
	metadata-version.h

libarrow_glib_la_generated_headers =		\
	enums.h

libarrow_glib_la_generated_sources =		\
	enums.c					\
	$(libarrow_glib_la_generated_headers)

libarrow_glib_la_sources =			\
	array.cpp				\
	array-builder.cpp			\
	binary-array.cpp			\
	binary-array-builder.cpp		\
	binary-data-type.cpp			\
	boolean-array.cpp			\
	boolean-array-builder.cpp		\
	boolean-data-type.cpp			\
	buffer.cpp				\
	chunked-array.cpp			\
	column.cpp				\
	data-type.cpp				\
	double-array.cpp			\
	double-array-builder.cpp		\
	double-data-type.cpp			\
	error.cpp				\
	field.cpp				\
	float-array.cpp				\
	float-array-builder.cpp			\
	float-data-type.cpp			\
	int8-array.cpp				\
	int8-array-builder.cpp			\
	int8-data-type.cpp			\
	int16-array.cpp				\
	int16-array-builder.cpp			\
	int16-data-type.cpp			\
	int32-array.cpp				\
	int32-array-builder.cpp			\
	int32-data-type.cpp			\
	int64-array.cpp				\
	int64-array-builder.cpp			\
	int64-data-type.cpp			\
	list-array.cpp				\
	list-array-builder.cpp			\
	list-data-type.cpp			\
	null-array.cpp				\
	null-data-type.cpp			\
	record-batch.cpp			\
	schema.cpp				\
	string-array.cpp			\
	string-array-builder.cpp		\
	string-data-type.cpp			\
	struct-array.cpp			\
	struct-array-builder.cpp		\
	struct-data-type.cpp			\
	table.cpp				\
	type.cpp				\
	uint8-array.cpp				\
	uint8-array-builder.cpp			\
	uint8-data-type.cpp			\
	uint16-array.cpp			\
	uint16-array-builder.cpp		\
	uint16-data-type.cpp			\
	uint32-array.cpp			\
	uint32-array-builder.cpp		\
	uint32-data-type.cpp			\
	uint64-array.cpp			\
	uint64-array-builder.cpp		\
	uint64-data-type.cpp			\
	$(libarrow_glib_la_headers)		\
	$(libarrow_glib_la_generated_sources)

libarrow_glib_la_sources +=			\
	file.cpp				\
	file-mode.cpp				\
	file-output-stream.cpp			\
	input-stream.cpp			\
	memory-mapped-file.cpp			\
	output-stream.cpp			\
	random-access-file.cpp			\
	readable.cpp				\
	writeable.cpp				\
	writeable-file.cpp

libarrow_glib_la_sources +=			\
	file-reader.cpp				\
	file-writer.cpp				\
	metadata-version.cpp			\
	stream-reader.cpp			\
	stream-writer.cpp

libarrow_glib_la_cpp_headers =			\
	array.hpp				\
	array-builder.hpp			\
	arrow-glib.hpp				\
	buffer.hpp				\
	chunked-array.hpp			\
	column.hpp				\
	data-type.hpp				\
	error.hpp				\
	field.hpp				\
	record-batch.hpp			\
	schema.hpp				\
	table.hpp				\
	type.hpp

libarrow_glib_la_cpp_headers +=			\
	file.hpp				\
	file-mode.hpp				\
	file-output-stream.hpp			\
	input-stream.hpp			\
	memory-mapped-file.hpp			\
	output-stream.hpp			\
	random-access-file.hpp			\
	readable.hpp				\
	writeable.hpp				\
	writeable-file.hpp

libarrow_glib_la_cpp_headers +=			\
	file-reader.hpp				\
	file-writer.hpp				\
	metadata-version.hpp			\
	stream-reader.hpp			\
	stream-writer.hpp

libarrow_glib_la_SOURCES =			\
	$(libarrow_glib_la_sources)		\
	$(libarrow_glib_la_cpp_headers)

BUILT_SOURCES =					\
	$(libarrow_glib_la_genearted_sources)	\
	stamp-enums.c				\
	stamp-enums.h

EXTRA_DIST +=					\
	enums.c.template			\
	enums.h.template

enums.h: stamp-enums.h
	@true
stamp-enums.h: $(libarrow_glib_la_headers) enums.h.template
	$(AM_V_GEN)					\
	  (cd $(srcdir) &&				\
	   $(GLIB_MKENUMS)				\
	     --identifier-prefix GArrow			\
	     --symbol-prefix garrow			\
	     --template enums.h.template		\
	     $(libarrow_glib_la_headers)) > enums.h
	touch $@

enums.c: stamp-enums.c
	@true
stamp-enums.c: $(libarrow_glib_la_headers) enums.c.template
	$(AM_V_GEN)					\
	  (cd $(srcdir) &&				\
	   $(GLIB_MKENUMS)				\
	     --identifier-prefix GArrow			\
	     --symbol-prefix garrow			\
	     --template enums.c.template		\
	     $(libarrow_glib_la_headers)) > enums.c
	touch $@

arrow_glib_includedir = $(includedir)/arrow-glib
arrow_glib_include_HEADERS =			\
	$(libarrow_glib_la_headers)		\
	$(libarrow_glib_la_cpp_headers)		\
	$(libarrow_glib_la_generated_headers)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA =				\
	arrow-glib.pc

# GObject Introspection
-include $(INTROSPECTION_MAKEFILE)
INTROSPECTION_GIRS =
INTROSPECTION_SCANNER_ARGS =
INTROSPECTION_COMPILER_ARGS =

if HAVE_INTROSPECTION
Arrow-1.0.gir: libarrow-glib.la
Arrow_1_0_gir_PACKAGES =			\
	gobject-2.0
Arrow_1_0_gir_EXPORT_PACKAGES = arrow
Arrow_1_0_gir_INCLUDES = GObject-2.0
Arrow_1_0_gir_CFLAGS =				\
	$(AM_CPPFLAGS)
Arrow_1_0_gir_LIBS = libarrow-glib.la
Arrow_1_0_gir_FILES = $(libarrow_glib_la_sources)
Arrow_1_0_gir_SCANNERFLAGS =			\
	--warn-all				\
	--identifier-prefix=GArrow		\
	--symbol-prefix=garrow
INTROSPECTION_GIRS += Arrow-1.0.gir

girdir = $(datadir)/gir-1.0
gir_DATA = $(INTROSPECTION_GIRS)

typelibdir = $(libdir)/girepository-1.0
typelib_DATA = $(INTROSPECTION_GIRS:.gir=.typelib)

CLEANFILES +=					\
	$(gir_DATA)				\
	$(typelib_DATA)
endif
