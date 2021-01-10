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

#define GAD_TYPE_FILE_FORMAT (gad_file_format_get_type())
G_DECLARE_DERIVABLE_TYPE(GADFileFormat,
                         gad_file_format,
                         GAD,
                         FILE_FORMAT,
                         GObject)
struct _GADFileFormatClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
gchar *
gad_file_format_get_type_name(GADFileFormat *file_format);

GARROW_AVAILABLE_IN_3_0
gboolean
gad_file_format_equal(GADFileFormat *file_format,
                      GADFileFormat *other_file_format);


#define GAD_TYPE_CSV_FILE_FORMAT (gad_csv_file_format_get_type())
G_DECLARE_DERIVABLE_TYPE(GADCSVFileFormat,
                         gad_csv_file_format,
                         GAD,
                         CSV_FILE_FORMAT,
                         GADFileFormat)
struct _GADCSVFileFormatClass
{
  GADFileFormatClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GADCSVFileFormat *gad_csv_file_format_new(void);


#define GAD_TYPE_IPC_FILE_FORMAT (gad_ipc_file_format_get_type())
G_DECLARE_DERIVABLE_TYPE(GADIPCFileFormat,
                         gad_ipc_file_format,
                         GAD,
                         IPC_FILE_FORMAT,
                         GADFileFormat)
struct _GADIPCFileFormatClass
{
  GADFileFormatClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GADIPCFileFormat *gad_ipc_file_format_new(void);


#define GAD_TYPE_PARQUET_FILE_FORMAT (gad_parquet_file_format_get_type())
G_DECLARE_DERIVABLE_TYPE(GADParquetFileFormat,
                         gad_parquet_file_format,
                         GAD,
                         PARQUET_FILE_FORMAT,
                         GADFileFormat)
struct _GADParquetFileFormatClass
{
  GADFileFormatClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GADParquetFileFormat *gad_parquet_file_format_new(void);


G_END_DECLS
