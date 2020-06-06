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

/* arrow::dataset::ScanContext */

#define GAD_TYPE_SCAN_CONTEXT (gad_scan_context_get_type())
G_DECLARE_DERIVABLE_TYPE(GADScanContext,
                         gad_scan_context,
                         GAD,
                         SCAN_CONTEXT,
                         GObject)
struct _GADScanContextClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GADScanContext *gad_scan_context_new(void);

/* arrow::dataset::ScanOptions */

#define GAD_TYPE_SCAN_OPTIONS (gad_scan_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GADScanOptions,
                         gad_scan_options,
                         GAD,
                         SCAN_OPTIONS,
                         GObject)
struct _GADScanOptionsClass
{
  GObjectClass parent_class;
};


GARROW_AVAILABLE_IN_1_0
GADScanOptions *gad_scan_options_new(GArrowSchema *schema);
GARROW_AVAILABLE_IN_1_0
GArrowSchema *gad_scan_options_get_schema(GADScanOptions *scan_options);
GARROW_AVAILABLE_IN_1_0
GADScanOptions *gad_scan_options_replace_schema(GADScanOptions *scan_options,
                                                GArrowSchema *schema);

/* arrow::dataset::ScanTask */

#define GAD_TYPE_SCAN_TASK (gad_scan_task_get_type())
G_DECLARE_DERIVABLE_TYPE(GADScanTask,
                         gad_scan_task,
                         GAD,
                         SCAN_TASK,
                         GObject)
struct _GADScanTaskClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GADScanOptions *gad_scan_task_get_options(GADScanTask *scan_task);
GARROW_AVAILABLE_IN_1_0
GADScanContext *gad_scan_task_get_context(GADScanTask *scan_task);
GARROW_AVAILABLE_IN_1_0
GArrowRecordBatchIterator *gad_scan_task_execute(GADScanTask *scan_task,
                                                 GError **error);

/* arrow::dataset::InMemoryScanTask */

#define GAD_TYPE_IN_MEMORY_SCAN_TASK (gad_in_memory_scan_task_get_type())
G_DECLARE_DERIVABLE_TYPE(GADInMemoryScanTask,
                         gad_in_memory_scan_task,
                         GAD,
                         IN_MEMORY_SCAN_TASK,
                         GADScanTask)
struct _GADInMemoryScanTaskClass
{
  GADScanTaskClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GADInMemoryScanTask *
gad_in_memory_scan_task_new(GArrowRecordBatch **record_batches,
                            gsize n_record_batches,
                            GADScanOptions *options,
                            GADScanContext *context);

G_END_DECLS
