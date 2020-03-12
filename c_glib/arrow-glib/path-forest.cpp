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

#include "arrow-glib/path-forest.hpp"

#include <memory>

G_BEGIN_DECLS

/**
 * SECTION: path-forest
 * @section_id: path-forest-classes
 * @title: Path forest classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowPathForest is a utility to transform an array of #GArrowFileInfo
 * into a forest representation for tree traversal purpose.
 */

typedef struct GArrowPathForestPrivate_ {
  std::shared_ptr<arrow::fs::PathForest> path_forest;
} GArrowPathForestPrivate;

enum {
  PROP_PATH_FOREST_SIZE = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowPathForest, garrow_path_forest, G_TYPE_OBJECT)

#define GARROW_PATH_FOREST_GET_PRIVATE(obj)       \
  static_cast<GArrowPathForestPrivate *>(         \
     garrow_path_forest_get_instance_private(     \
       GARROW_PATH_FOREST(obj)))

static void
garrow_path_forest_finalize(GObject *object)
{
  auto priv = GARROW_PATH_FOREST_GET_PRIVATE(object);

  priv->path_forest.~shared_ptr();

  G_OBJECT_CLASS(garrow_path_forest_parent_class)->finalize(object);
}

static void
garrow_file_info_get_property(GObject *object,
                              guint prop_id,
                              GValue *value,
                              GParamSpec *pspec)
{
  const auto arrow_path_forest =
    garrow_path_forest_get_raw(GARROW_PATH_FOREST(object));

  switch (prop_id) {
  case PROP_PATH_FOREST_SIZE:
    g_value_set_size(value, arrow_path_forest->size());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_path_forest_init(GArrowPathForest *object)
{
  auto priv = GARROW_PATH_FOREST_GET_PRIVATE(object);
  new(&priv->path_forest) std::shared_ptr<PathForest>;
}

static void
garrow_file_info_class_init(GArrowFileInfoClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_path_forest_finalize;
  gobject_class->get_property = garrow_path_forest_get_property;

  /**
   * GArrowPathForest:size:
   *
   * The number of FileInfos in the PathForest.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int("size",
                          "Size",
                          "The size of the PathForest",
                          0,
                          INT_MAX,
                          0,
                          static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class, PROP_PATH_FOREST_SIZE, spec);
}

/**
 * garrow_path_forest_new:
 * @file_infos: (array length=n_file_infos):
 *   The record batches that are transformed into a forest representation.
 * @n_file_infos: The number of file infos.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowPathForest or %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowPathForest *
garrow_path_forest_new(GArrowFileInfo **file_infos,
                       gsize n_file_infos,
                       GError **error)
{
}

/**
 * garrow_path_forest_new_sorted:
 * @sorted_file_infos: (array length=n_sorted_file_infos):
 *   The path-sorted record batches that are transformed into a forest representation.
 * @n_sorted_file_infos: The number of file infos.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowPathForest or %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowPathForest *
garrow_path_forest_new_sorted(GArrowFileInfo *sorted_file_infos,
                              gsize n_sorted_file_infos,
                              GError **error)

/**
 * garrow_path_forest_equal:
 * @path_forest: A #GArrowPathForest.
 * @other_path_forest: A #GArrowPathForest to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 1.0.0
 */
gboolean
garrow_path_forest_equal(GArrowPathForest *path_forest,
                         GArrowPathForest *other_path_forest);

gchar *
garrow_path_forest_to_string(GArrowPathForest *path_forest);

GArrowPathForestNode *
garrow_path_forest_get_node(GArrowPathForest *path_forest,
                            gint i);

GArrowPathForestNode **
garrow_path_forest_get_roots(GArrowPathForest *path_forest,
                             gsize **n_roots);

GArrowFileInfo **
garrow_path_forest_get_infos(GArrowPathForest *path_forest,
                             gsize **n_infos);

gboolean
garrow_path_forest_foreach(GArrowPathForest *path_forest,
                           GArrowPathForestVisitor *visitor,
                           gpointer visitor_data);


G_END_DECLS
