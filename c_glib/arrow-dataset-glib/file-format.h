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

#include <arrow-glib/arrow-glib.h>

G_BEGIN_DECLS

#define GADATASET_TYPE_FILE_FORMAT (gadataset_file_format_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetFileFormat,
                         gadataset_file_format,
                         GADATASET,
                         FILE_FORMAT,
                         GObject)
struct _GADatasetFileFormatClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
gchar *
gadataset_file_format_get_type_name(GADatasetFileFormat *file_format);

GARROW_AVAILABLE_IN_3_0
gboolean
gadataset_file_format_equal(GADatasetFileFormat *file_format,
                            GADatasetFileFormat *other_file_format);


#define GADATASET_TYPE_CSV_FILE_FORMAT (gadataset_csv_file_format_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetCSVFileFormat,
                         gadataset_csv_file_format,
                         GADATASET,
                         CSV_FILE_FORMAT,
                         GADatasetFileFormat)
struct _GADatasetCSVFileFormatClass
{
  GADatasetFileFormatClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GADatasetCSVFileFormat *gadataset_csv_file_format_new(void);


#define GADATASET_TYPE_IPC_FILE_FORMAT (gadataset_ipc_file_format_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetIPCFileFormat,
                         gadataset_ipc_file_format,
                         GADATASET,
                         IPC_FILE_FORMAT,
                         GADatasetFileFormat)
struct _GADatasetIPCFileFormatClass
{
  GADatasetFileFormatClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GADatasetIPCFileFormat *gadataset_ipc_file_format_new(void);


#define GADATASET_TYPE_PARQUET_FILE_FORMAT      \
  (gadataset_parquet_file_format_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetParquetFileFormat,
                         gadataset_parquet_file_format,
                         GADATASET,
                         PARQUET_FILE_FORMAT,
                         GADatasetFileFormat)
struct _GADatasetParquetFileFormatClass
{
  GADatasetFileFormatClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GADatasetParquetFileFormat *gadataset_parquet_file_format_new(void);


G_END_DECLS
