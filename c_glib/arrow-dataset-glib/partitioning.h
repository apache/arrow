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

/**
 * GADatasetSegmentEncoding
 * @GADATASET_SEGMENT_ENCODING_NONE: No encoding.
 * @GADATASET_SEGMENT_ENCODING_URI: Segment values are URL-encoded.
 *
 * They are corresponding to `arrow::dataset::SegmentEncoding` values.
 *
 * Since: 6.0.0
 */
typedef enum {
  GADATASET_SEGMENT_ENCODING_NONE,
  GADATASET_SEGMENT_ENCODING_URI,
} GADatasetSegmentEncoding;


#define GADATASET_TYPE_PARTITIONING_FACTORY_OPTIONS   \
  (gadataset_partitioning_factory_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetPartitioningFactoryOptions,
                         gadataset_partitioning_factory_options,
                         GADATASET,
                         PARTITIONING_FACTORY_OPTIONS,
                         GObject)
struct _GADatasetPartitioningFactoryOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GADatasetPartitioningFactoryOptions *
gadataset_partitioning_factory_options_new(void);


#define GADATASET_TYPE_PARTITIONING (gadataset_partitioning_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetPartitioning,
                         gadataset_partitioning,
                         GADATASET,
                         PARTITIONING,
                         GObject)
struct _GADatasetPartitioningClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
gchar *
gadataset_partitioning_get_type_name(GADatasetPartitioning *partitioning);


GARROW_AVAILABLE_IN_12_0
GADatasetPartitioning *
gadataset_partitioning_create_default(void);


#define GADATASET_TYPE_KEY_VALUE_PARTITIONING_OPTIONS   \
  (gadataset_key_value_partitioning_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetKeyValuePartitioningOptions,
                         gadataset_key_value_partitioning_options,
                         GADATASET,
                         KEY_VALUE_PARTITIONING_OPTIONS,
                         GObject)
struct _GADatasetKeyValuePartitioningOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GADatasetKeyValuePartitioningOptions *
gadataset_key_value_partitioning_options_new(void);


#define GADATASET_TYPE_KEY_VALUE_PARTITIONING   \
  (gadataset_key_value_partitioning_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetKeyValuePartitioning,
                         gadataset_key_value_partitioning,
                         GADATASET,
                         KEY_VALUE_PARTITIONING,
                         GADatasetPartitioning)
struct _GADatasetKeyValuePartitioningClass
{
  GADatasetPartitioningClass parent_class;
};


#define GADATASET_TYPE_DIRECTORY_PARTITIONING   \
  (gadataset_directory_partitioning_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetDirectoryPartitioning,
                         gadataset_directory_partitioning,
                         GADATASET,
                         DIRECTORY_PARTITIONING,
                         GADatasetKeyValuePartitioning)
struct _GADatasetDirectoryPartitioningClass
{
  GADatasetKeyValuePartitioningClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GADatasetDirectoryPartitioning *
gadataset_directory_partitioning_new(
  GArrowSchema *schema,
  GList *dictionaries,
  GADatasetKeyValuePartitioningOptions *options,
  GError **error);


#define GADATASET_TYPE_HIVE_PARTITIONING_OPTIONS   \
  (gadataset_hive_partitioning_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetHivePartitioningOptions,
                         gadataset_hive_partitioning_options,
                         GADATASET,
                         HIVE_PARTITIONING_OPTIONS,
                         GADatasetKeyValuePartitioningOptions)
struct _GADatasetHivePartitioningOptionsClass
{
  GADatasetKeyValuePartitioningOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GADatasetHivePartitioningOptions *
gadataset_hive_partitioning_options_new(void);


#define GADATASET_TYPE_HIVE_PARTITIONING        \
  (gadataset_hive_partitioning_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetHivePartitioning,
                         gadataset_hive_partitioning,
                         GADATASET,
                         HIVE_PARTITIONING,
                         GADatasetKeyValuePartitioning)
struct _GADatasetHivePartitioningClass
{
  GADatasetKeyValuePartitioningClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GADatasetHivePartitioning *
gadataset_hive_partitioning_new(GArrowSchema *schema,
                                GList *dictionaries,
                                GADatasetHivePartitioningOptions *options,
                                GError **error);
GARROW_AVAILABLE_IN_11_0
gchar *
gadataset_hive_partitioning_get_null_fallback(
  GADatasetHivePartitioning *partitioning);


G_END_DECLS
