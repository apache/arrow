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

#include <arrow-dataset-glib/fragment.h>

G_BEGIN_DECLS

/* arrow::dataset::ScanOptions */

#define GADATASET_TYPE_SCAN_OPTIONS (gadataset_scan_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetScanOptions,
                         gadataset_scan_options,
                         GADATASET,
                         SCAN_OPTIONS,
                         GObject)
struct _GADatasetScanOptionsClass
{
  GObjectClass parent_class;
};


GARROW_AVAILABLE_IN_1_0
GADatasetScanOptions *
gadataset_scan_options_new(GArrowSchema *schema);
GARROW_AVAILABLE_IN_1_0
GArrowSchema *
gadataset_scan_options_get_schema(GADatasetScanOptions *scan_options);

/* arrow::dataset::ScanTask */

#define GADATASET_TYPE_SCAN_TASK (gadataset_scan_task_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetScanTask,
                         gadataset_scan_task,
                         GADATASET,
                         SCAN_TASK,
                         GObject)
struct _GADatasetScanTaskClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GADatasetScanOptions *
gadataset_scan_task_get_options(GADatasetScanTask *scan_task);
GARROW_AVAILABLE_IN_4_0
GADatasetFragment *
gadataset_scan_task_get_fragment(GADatasetScanTask *scan_task);
GARROW_AVAILABLE_IN_1_0
GArrowRecordBatchIterator *
gadataset_scan_task_execute(GADatasetScanTask *scan_task,
                            GError **error);

/* arrow::dataset::InMemoryScanTask */

#define GADATASET_TYPE_IN_MEMORY_SCAN_TASK      \
  (gadataset_in_memory_scan_task_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetInMemoryScanTask,
                         gadataset_in_memory_scan_task,
                         GADATASET,
                         IN_MEMORY_SCAN_TASK,
                         GADatasetScanTask)
struct _GADatasetInMemoryScanTaskClass
{
  GADatasetScanTaskClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GADatasetInMemoryScanTask *
gadataset_in_memory_scan_task_new(GArrowRecordBatch **record_batches,
                                  gsize n_record_batches,
                                  GADatasetScanOptions *options,
                                  GADatasetInMemoryFragment *fragment);

G_END_DECLS
