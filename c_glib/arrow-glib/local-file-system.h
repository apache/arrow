/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow-glib/file-system.h>

G_BEGIN_DECLS

/* arrow::fs::LocalFileSystemOptions */

#define GARROW_TYPE_LOCAL_FILE_SYSTEM_OPTIONS (garrow_local_file_system_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLocalFileSystemOptions,
                         garrow_local_file_system_options,
                         GARROW,
                         LOCAL_FILE_SYSTEM_OPTIONS,
                         GObject)
struct _GArrowLocalFileSystemOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowLocalFileSystemOptions *
garrow_local_file_system_options_new(void);

/* arrow::fs::LocalFileSystem */

#define GARROW_TYPE_LOCAL_FILE_SYSTEM (garrow_local_file_system_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLocalFileSystem,
                         garrow_local_file_system,
                         GARROW,
                         LOCAL_FILE_SYSTEM,
                         GArrowFileSystem)
struct _GArrowLocalFileSystemClass
{
  GArrowFileSystemClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowLocalFileSystem *
garrow_local_file_system_new(GArrowLocalFileSystemOptions *options);

G_END_DECLS
