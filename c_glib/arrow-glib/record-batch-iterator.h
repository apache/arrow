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

#include <arrow-glib/record-batch.h>

G_BEGIN_DECLS

#define GARROW_TYPE_RECORD_BATCH_ITERATOR (garrow_record_batch_iterator_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchIterator,
                         garrow_record_batch_iterator,
                         GARROW,
                         RECORD_BATCH_ITERATOR,
                         GObject)
struct _GArrowRecordBatchIteratorClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowRecordBatchIterator *
garrow_record_batch_iterator_new(GList *record_batches);

GARROW_AVAILABLE_IN_0_17
GArrowRecordBatch *
garrow_record_batch_iterator_next(GArrowRecordBatchIterator *iterator,
                                  GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean
garrow_record_batch_iterator_equal(GArrowRecordBatchIterator *iterator,
                                   GArrowRecordBatchIterator *other_iterator);

GARROW_AVAILABLE_IN_0_17
GList*
garrow_record_batch_iterator_to_list(GArrowRecordBatchIterator *iterator,
                                     GError **error);

G_END_DECLS
