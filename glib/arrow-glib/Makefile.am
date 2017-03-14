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
	chunked-array.h				\
	column.h				\
	binary-array.h				\
	binary-array-builder.h			\
	binary-data-type.h			\
	boolean-array.h				\
	boolean-array-builder.h			\
	boolean-data-type.h			\
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

libarrow_glib_la_cpp_headers =			\
	array.hpp				\
	array-builder.hpp			\
	arrow-glib.hpp				\
	chunked-array.hpp			\
	column.hpp				\
	data-type.hpp				\
	error.hpp				\
	field.hpp				\
	record-batch.hpp			\
	schema.hpp				\
	table.hpp				\
	type.hpp

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

# libarrow-io-glib
lib_LTLIBRARIES +=				\
	libarrow-io-glib.la

libarrow_io_glib_la_CXXFLAGS =			\
	$(GLIB_CFLAGS)				\
	$(ARROW_IO_CFLAGS)			\
	$(GARROW_CXXFLAGS)

libarrow_io_glib_la_LIBADD =			\
	$(GLIB_LIBS)				\
	$(ARROW_IO_LIBS)

libarrow_io_glib_la_headers =			\
	arrow-io-glib.h				\
	io-file.h				\
	io-file-mode.h				\
	io-file-output-stream.h			\
	io-input-stream.h			\
	io-memory-mapped-file.h			\
	io-output-stream.h			\
	io-readable.h				\
	io-readable-file.h			\
	io-writeable.h				\
	io-writeable-file.h

libarrow_io_glib_la_generated_headers =		\
	io-enums.h

libarrow_io_glib_la_generated_sources =			\
	io-enums.c					\
	$(libarrow_io_glib_la_generated_headers)

libarrow_io_glib_la_sources =			\
	io-file.cpp				\
	io-file-mode.cpp			\
	io-file-output-stream.cpp		\
	io-input-stream.cpp			\
	io-memory-mapped-file.cpp		\
	io-output-stream.cpp			\
	io-readable.cpp				\
	io-readable-file.cpp			\
	io-writeable.cpp			\
	io-writeable-file.cpp			\
	$(libarrow_io_glib_la_headers)		\
	$(libarrow_io_glib_la_generated_sources)

libarrow_io_glib_la_cpp_headers =		\
	arrow-io-glib.hpp			\
	io-file.hpp				\
	io-file-mode.hpp			\
	io-file-output-stream.hpp		\
	io-input-stream.hpp			\
	io-memory-mapped-file.hpp		\
	io-output-stream.hpp			\
	io-readable.hpp				\
	io-readable-file.hpp			\
	io-writeable.hpp			\
	io-writeable-file.hpp

libarrow_io_glib_la_SOURCES =			\
	$(libarrow_io_glib_la_sources)		\
	$(libarrow_io_glib_la_cpp_headers)

BUILT_SOURCES +=					\
	$(libarrow_io_glib_la_genearted_sources)	\
	stamp-io-enums.c				\
	stamp-io-enums.h

EXTRA_DIST +=					\
	io-enums.c.template			\
	io-enums.h.template

io-enums.h: stamp-io-enums.h
	@true
stamp-io-enums.h: $(libarrow_io_glib_la_headers) io-enums.h.template
	$(AM_V_GEN)					\
	  (cd $(srcdir) &&				\
	   $(GLIB_MKENUMS)				\
	     --identifier-prefix GArrowIO		\
	     --symbol-prefix garrow_io			\
	     --template io-enums.h.template		\
	     $(libarrow_io_glib_la_headers)) > io-enums.h
	touch $@

io-enums.c: stamp-io-enums.c
	@true
stamp-io-enums.c: $(libarrow_io_glib_la_headers) io-enums.c.template
	$(AM_V_GEN)					\
	  (cd $(srcdir) &&				\
	   $(GLIB_MKENUMS)				\
	     --identifier-prefix GArrowIO		\
	     --symbol-prefix garrow_io			\
	     --template io-enums.c.template		\
	     $(libarrow_io_glib_la_headers)) > io-enums.c
	touch $@

# libarrow-ipc-glib
lib_LTLIBRARIES +=				\
	libarrow-ipc-glib.la

libarrow_ipc_glib_la_CXXFLAGS =			\
	$(GLIB_CFLAGS)				\
	$(ARROW_IPC_CFLAGS)			\
	$(GARROW_CXXFLAGS)

libarrow_ipc_glib_la_LIBADD =			\
	$(GLIB_LIBS)				\
	$(ARROW_IPC_LIBS)

libarrow_ipc_glib_la_headers =			\
	arrow-ipc-glib.h			\
	ipc-file-reader.h			\
	ipc-file-writer.h			\
	ipc-stream-reader.h			\
	ipc-stream-writer.h			\
	ipc-metadata-version.h

libarrow_ipc_glib_la_generated_headers =	\
	ipc-enums.h

libarrow_ipc_glib_la_generated_sources =		\
	ipc-enums.c					\
	$(libarrow_ipc_glib_la_generated_headers)

libarrow_ipc_glib_la_sources =				\
	ipc-file-reader.cpp				\
	ipc-file-writer.cpp				\
	ipc-metadata-version.cpp			\
	ipc-stream-reader.cpp				\
	ipc-stream-writer.cpp				\
	$(libarrow_ipc_glib_la_headers)			\
	$(libarrow_ipc_glib_la_generated_sources)

libarrow_ipc_glib_la_cpp_headers =		\
	arrow-ipc-glib.hpp			\
	ipc-file-reader.hpp			\
	ipc-file-writer.hpp			\
	ipc-metadata-version.hpp		\
	ipc-stream-reader.hpp			\
	ipc-stream-writer.hpp

libarrow_ipc_glib_la_SOURCES =			\
	$(libarrow_ipc_glib_la_sources)		\
	$(libarrow_ipc_glib_la_cpp_headers)

BUILT_SOURCES +=					\
	$(libarrow_ipc_glib_la_genearted_sources)	\
	stamp-ipc-enums.c				\
	stamp-ipc-enums.h

EXTRA_DIST +=					\
	ipc-enums.c.template			\
	ipc-enums.h.template

ipc-enums.h: stamp-ipc-enums.h
	@true
stamp-ipc-enums.h: $(libarrow_ipc_glib_la_headers) ipc-enums.h.template
	$(AM_V_GEN)						\
	  (cd $(srcdir) &&					\
	   $(GLIB_MKENUMS)					\
	     --identifier-prefix GArrowIPC			\
	     --symbol-prefix garrow_ipc				\
	     --template ipc-enums.h.template			\
	     $(libarrow_ipc_glib_la_headers)) > ipc-enums.h
	touch $@

ipc-enums.c: stamp-ipc-enums.c
	@true
stamp-ipc-enums.c: $(libarrow_ipc_glib_la_headers) ipc-enums.c.template
	$(AM_V_GEN)						\
	  (cd $(srcdir) &&					\
	   $(GLIB_MKENUMS)					\
	     --identifier-prefix GArrowIPC			\
	     --symbol-prefix garrow_ipc				\
	     --template ipc-enums.c.template			\
	     $(libarrow_ipc_glib_la_headers)) > ipc-enums.c
	touch $@

pkginclude_HEADERS =					\
	$(libarrow_glib_la_headers)			\
	$(libarrow_glib_la_cpp_headers)			\
	$(libarrow_glib_la_generated_headers)		\
	$(libarrow_io_glib_la_headers)			\
	$(libarrow_io_glib_la_cpp_headers)		\
	$(libarrow_io_glib_la_generated_headers)	\
	$(libarrow_ipc_glib_la_headers)			\
	$(libarrow_ipc_glib_la_cpp_headers)		\
	$(libarrow_ipc_glib_la_generated_headers)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA =				\
	arrow-glib.pc				\
	arrow-io-glib.pc			\
	arrow-ipc-glib.pc

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

ArrowIO-1.0.gir: libarrow-io-glib.la
ArrowIO-1.0.gir: Arrow-1.0.gir
ArrowIO_1_0_gir_PACKAGES =			\
	gobject-2.0
ArrowIO_1_0_gir_EXPORT_PACKAGES = arrow-io
ArrowIO_1_0_gir_INCLUDES =			\
	GObject-2.0
ArrowIO_1_0_gir_CFLAGS =			\
	$(AM_CPPFLAGS)
ArrowIO_1_0_gir_LIBS =				\
	libarrow-io-glib.la			\
	libarrow-glib.la
ArrowIO_1_0_gir_FILES = $(libarrow_io_glib_la_sources)
ArrowIO_1_0_gir_SCANNERFLAGS =				\
	--include-uninstalled=$(builddir)/Arrow-1.0.gir	\
	--warn-all					\
	--identifier-prefix=GArrowIO			\
	--symbol-prefix=garrow_io
INTROSPECTION_GIRS += ArrowIO-1.0.gir

ArrowIPC-1.0.gir: libarrow-ipc-glib.la
ArrowIPC-1.0.gir: Arrow-1.0.gir
ArrowIPC-1.0.gir: ArrowIO-1.0.gir
ArrowIPC_1_0_gir_PACKAGES =			\
	gobject-2.0
ArrowIPC_1_0_gir_EXPORT_PACKAGES = arrow-ipc
ArrowIPC_1_0_gir_INCLUDES =			\
	GObject-2.0
ArrowIPC_1_0_gir_CFLAGS =			\
	$(AM_CPPFLAGS)
ArrowIPC_1_0_gir_LIBS =				\
	libarrow-ipc-glib.la			\
	libarrow-io-glib.la			\
	libarrow-glib.la
ArrowIPC_1_0_gir_FILES = $(libarrow_ipc_glib_la_sources)
ArrowIPC_1_0_gir_SCANNERFLAGS =					\
	--include-uninstalled=$(builddir)/Arrow-1.0.gir		\
	--include-uninstalled=$(builddir)/ArrowIO-1.0.gir	\
	--warn-all						\
	--identifier-prefix=GArrowIPC				\
	--symbol-prefix=garrow_ipc
INTROSPECTION_GIRS += ArrowIPC-1.0.gir

girdir = $(datadir)/gir-1.0
gir_DATA = $(INTROSPECTION_GIRS)

typelibdir = $(libdir)/girepository-1.0
typelib_DATA = $(INTROSPECTION_GIRS:.gir=.typelib)

CLEANFILES +=					\
	$(gir_DATA)				\
	$(typelib_DATA)
endif
