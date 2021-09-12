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

#include <arrow-dataset-glib/file-format.h>

G_BEGIN_DECLS

typedef struct _GADatasetScannerBuilder GADatasetScannerBuilder;
typedef struct _GADatasetScanner GADatasetScanner;

#define GADATASET_TYPE_DATASET (gadataset_dataset_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetDataset,
                         gadataset_dataset,
                         GADATASET,
                         DATASET,
                         GObject)
struct _GADatasetDatasetClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GADatasetScannerBuilder *
gadataset_dataset_begin_scan(GADatasetDataset *dataset,
                             GError **error);
GARROW_AVAILABLE_IN_5_0
GArrowTable *
gadataset_dataset_to_table(GADatasetDataset *dataset,
                           GError **error);
GARROW_AVAILABLE_IN_5_0
gchar *
gadataset_dataset_get_type_name(GADatasetDataset *dataset);


#define GADATASET_TYPE_FILE_SYSTEM_DATASET_WRITE_OPTIONS        \
  (gadataset_file_system_dataset_write_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetFileSystemDatasetWriteOptions,
                         gadataset_file_system_dataset_write_options,
                         GADATASET,
                         FILE_SYSTEM_DATASET_WRITE_OPTIONS,
                         GObject)
struct _GADatasetFileSystemDatasetWriteOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GADatasetFileSystemDatasetWriteOptions *
gadataset_file_system_dataset_write_options_new(void);


#define GADATASET_TYPE_FILE_SYSTEM_DATASET      \
  (gadataset_file_system_dataset_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetFileSystemDataset,
                         gadataset_file_system_dataset,
                         GADATASET,
                         FILE_SYSTEM_DATASET,
                         GADatasetDataset)
struct _GADatasetFileSystemDatasetClass
{
  GADatasetDatasetClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
gboolean
gadataset_file_system_dataset_write_scanner(
  GADatasetScanner *scanner,
  GADatasetFileSystemDatasetWriteOptions *options,
  GError **error);


G_END_DECLS
