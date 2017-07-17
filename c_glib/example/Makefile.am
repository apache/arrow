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

SUBDIRS	=					\
	lua

AM_CPPFLAGS =					\
	-I$(top_builddir)			\
	-I$(top_srcdir)				\
	-DGARROW_DISABLE_DEPRECATED

AM_CFLAGS =					\
	$(GLIB_CFLAGS)				\
	$(GARROW_CFLAGS)

AM_LDFLAGS =						\
	$(GLIB_LIBS)					\
	$(builddir)/../arrow-glib/libarrow-glib.la

noinst_PROGRAMS =				\
	build					\
	read-batch				\
	read-stream

build_SOURCES =					\
	build.c

read_batch_SOURCES =				\
	read-batch.c

read_stream_SOURCES =				\
	read-stream.c

dist_example_DATA =				\
	README.md				\
	$(build_SOURCES)			\
	$(read_batch_SOURCES)			\
	$(read_stream_SOURCES)
