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

#include <arrow-dataset-glib/dataset-definition.h>
#include <arrow-dataset-glib/fragment.h>
#include <arrow-dataset-glib/version.h>

G_BEGIN_DECLS

#define GADATASET_TYPE_SCANNER (gadataset_scanner_get_type())
GADATASET_AVAILABLE_IN_5_0
G_DECLARE_DERIVABLE_TYPE(GADatasetScanner, gadataset_scanner, GADATASET, SCANNER, GObject)
struct _GADatasetScannerClass
{
  GObjectClass parent_class;
};

GADATASET_AVAILABLE_IN_5_0
GArrowTable *
gadataset_scanner_to_table(GADatasetScanner *scanner, GError **error);

#define GADATASET_TYPE_SCANNER_BUILDER (gadataset_scanner_builder_get_type())
GADATASET_AVAILABLE_IN_5_0
G_DECLARE_DERIVABLE_TYPE(
  GADatasetScannerBuilder, gadataset_scanner_builder, GADATASET, SCANNER_BUILDER, GObject)
struct _GADatasetScannerBuilderClass
{
  GObjectClass parent_class;
};

GADATASET_AVAILABLE_IN_5_0
GADatasetScannerBuilder *
gadataset_scanner_builder_new(GADatasetDataset *dataset, GError **error);
GADATASET_AVAILABLE_IN_6_0
GADatasetScannerBuilder *
gadataset_scanner_builder_new_record_batch_reader(GArrowRecordBatchReader *reader);

GADATASET_AVAILABLE_IN_6_0
gboolean
gadataset_scanner_builder_set_filter(GADatasetScannerBuilder *builder,
                                     GArrowExpression *expression,
                                     GError **error);

GADATASET_AVAILABLE_IN_5_0
GADatasetScanner *
gadataset_scanner_builder_finish(GADatasetScannerBuilder *builder, GError **error);

G_END_DECLS
