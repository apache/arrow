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

#include <arrow-glib/file-system.h>

G_BEGIN_DECLS

class GArrowPathForestNode;

/* arrow::fs::PathForest */

#define GARROW_TYPE_PATH_FOREST (garrow_path_forest_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowPathForest,
                         garrow_path_forest,
                         GARROW,
                         PATH_FOREST,
                         GObject)
struct _GArrowPathForestClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowPathForest *
garrow_path_forest_new(GArrowFileInfo **file_infos,
                       gsize n_file_infos);

/* TODO: how to support associated arrays in garrow_path_forest_new */

GARROW_AVAILABLE_IN_1_0
GArrowPathForest *
garrow_path_forest_new_sorted(GArrowFileInfo **sorted_file_infos,
                              gsize n_sorted_file_infos);

GARROW_AVAILABLE_IN_1_0
gboolean
garrow_path_forest_equal(GArrowPathForest *path_forest,
                         GArrowPathForest *other_path_forest);

GARROW_AVAILABLE_IN_1_0
gchar *
garrow_path_forest_to_string(GArrowPathForest *path_forest);

GARROW_AVAILABLE_IN_1_0
GArrowPathForestNode *
garrow_path_forest_get_node(GArrowPathForest *path_forest,
                            gint i);

/* FIXME: Can we make `roots` a property? */
GARROW_AVAILABLE_IN_1_0
GArrowPathForestNode **
garrow_path_forest_get_roots(GArrowPathForest *path_forest,
                             gsize **n_roots);

/* FIXME: Can we make `infos` a property? */
GARROW_AVAILABLE_IN_1_0
GArrowFileInfo **
garrow_path_forest_get_infos(GArrowPathForest *path_forest,
                             gsize **n_infos);

typedef gboolean (* GArrowPathForestVisitor)(GArrowPathForestNode *, gpointer);

GARROW_AVAILABLE_IN_1_0
gboolean
garrow_path_forest_foreach(GArrowPathForest *path_forest,
                           GArrowPathForestVisitor *visitor,
                           gpointer visitor_data);

G_END_DECLS
