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

#include <arrow-dataset-glib/dataset.h>

G_BEGIN_DECLS

#define GADATASET_TYPE_FINISH_OPTIONS (gadataset_finish_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetFinishOptions,
                         gadataset_finish_options,
                         GADATASET,
                         FINISH_OPTIONS,
                         GObject)
struct _GADatasetFinishOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GADatasetFinishOptions *
gadataset_finish_options_new(void);

#define GADATASET_TYPE_DATASET_FACTORY (gadataset_dataset_factory_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetDatasetFactory,
                         gadataset_dataset_factory,
                         GADATASET,
                         DATASET_FACTORY,
                         GObject)
struct _GADatasetDatasetFactoryClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GADatasetDataset *
gadataset_dataset_factory_finish(GADatasetDatasetFactory *factory,
                                 GADatasetFinishOptions *options,
                                 GError **error);


#define GADATASET_TYPE_FILE_SYSTEM_DATASET_FACTORY      \
  (gadataset_file_system_dataset_factory_get_type())
G_DECLARE_DERIVABLE_TYPE(GADatasetFileSystemDatasetFactory,
                         gadataset_file_system_dataset_factory,
                         GADATASET,
                         FILE_SYSTEM_DATASET_FACTORY,
                         GADatasetDatasetFactory)
struct _GADatasetFileSystemDatasetFactoryClass
{
  GADatasetDatasetFactoryClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GADatasetFileSystemDatasetFactory *
gadataset_file_system_dataset_factory_new(GADatasetFileFormat *file_format);
GARROW_AVAILABLE_IN_5_0
gboolean
gadataset_file_system_dataset_factory_set_file_system(
  GADatasetFileSystemDatasetFactory *factory,
  GArrowFileSystem *file_system,
  GError **error);
gboolean
gadataset_file_system_dataset_factory_set_file_system_uri(
  GADatasetFileSystemDatasetFactory *factory,
  const gchar *uri,
  GError **error);

GARROW_AVAILABLE_IN_5_0
gboolean
gadataset_file_system_dataset_factory_add_path(
  GADatasetFileSystemDatasetFactory *factory,
  const gchar *path,
  GError **error);
/*
GARROW_AVAILABLE_IN_5_0
gboolean
gadataset_file_system_dataset_factory_add_file(
  GADatasetFileSystemDatasetFactory *factory,
  GArrowFileInfo *file,
  GError **error);
GARROW_AVAILABLE_IN_5_0
gboolean
gadataset_file_system_dataset_factory_add_selector(
  GADatasetFileSystemDatasetFactory *factory,
  GArrorFileSelector *selector,
  GError **error);
*/

GARROW_AVAILABLE_IN_5_0
GADatasetFileSystemDataset *
gadataset_file_system_dataset_factory_finish(
  GADatasetFileSystemDatasetFactory *factory,
  GADatasetFinishOptions *options,
  GError **error);


G_END_DECLS
