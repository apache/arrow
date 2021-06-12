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

/* arrow::dataset::Fragment */

#define GADATASET_TYPE_FRAGMENT (gadataset_fragment_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetFragment,
                         gadataset_fragment,
                         GADATASET,
                         FRAGMENT,
                         GObject)
struct _GADatasetFragmentClass
{
  GObjectClass parent_class;
};

/* arrow::dataset::InMemoryFragment */

#define GADATASET_TYPE_IN_MEMORY_FRAGMENT       \
  (gadataset_in_memory_fragment_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetInMemoryFragment,
                         gadataset_in_memory_fragment,
                         GADATASET,
                         IN_MEMORY_FRAGMENT,
                         GADatasetFragment)
struct _GADatasetInMemoryFragmentClass
{
  GADatasetFragmentClass parent_class;
};

GARROW_AVAILABLE_IN_4_0
GADatasetInMemoryFragment *
gadataset_in_memory_fragment_new(GArrowSchema *schema,
                                 GArrowRecordBatch **record_batches,
                                 gsize n_record_batches);

G_END_DECLS
