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

#include <arrow-glib/file-system.hpp>
#include <arrow-glib/local-file-system.hpp>

G_BEGIN_DECLS

typedef struct GArrowLocalFileSystemOptionsPrivate_ {
  std::shared_ptr<arrow::fs::LocalFileSystemOptions> local_file_system_options;
} GArrowLocalFileSystemOptionsPrivate;

enum {
  PROP_LOCAL_FILE_SYSTEM_OPTIONS = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowLocalFileSystemOptions,
                           garrow_local_file_system_options,
                           G_TYPE_OBJECT)

#define GARROW_LOCAL_FILE_SYSTEM_OPTIONS_GET_PRIVATE(obj)       \
  static_cast<GArrowLocalFileSystemOptionsPrivate *>(           \
     garrow_local_file_system_options_get_instance_private(     \
       GARROW_LOCAL_FILE_SYSTEM_OPTIONS(obj)))

static void
garrow_local_file_system_options_finalize(GObject *object)
{
  auto priv = GARROW_LOCAL_FILE_SYSTEM_OPTIONS_GET_PRIVATE(object);

  priv->local_file_system_options = nullptr;

  G_OBJECT_CLASS(garrow_local_file_system_options_parent_class)->finalize(object);
}

static void
garrow_local_file_system_options_set_property(GObject *object,
                                              guint prop_id,
                                              const GValue *value,
                                              GParamSpec *pspec)
{
  auto priv = GARROW_LOCAL_FILE_SYSTEM_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_LOCAL_FILE_SYSTEM_OPTIONS:
    priv->local_file_system_options =
      *static_cast<std::shared_ptr<arrow::fs::LocalFileSystemOptions> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_local_file_system_options_init(GArrowLocalFileSystemOptions *object)
{
}

static void
garrow_local_file_system_options_class_init(GArrowLocalFileSystemOptionsClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_local_file_system_options_finalize;
  gobject_class->set_property = garrow_local_file_system_options_set_property;

  spec = g_param_spec_pointer("local_file_system_options",
                              "LocalFileSystemOptions",
                              "The raw std::shared<arrow::fs::LocalFileSystemOptions> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_LOCAL_FILE_SYSTEM_OPTIONS, spec);
}

/**
 * garrow_local_file_system_options_defaults:
 *
 * Returns: (transfer full): A newly created #GArrowLocalFileSystemOptions.
 *
 * Since: 1.0.0
 */
GArrowLocalFileSystemOptions *
garrow_local_file_system_options_defaults(void)
{
  auto arrow_local_file_system_options =
    std::make_shared<arrow::fs::LocalFileSystemOptions>(
        arrow::fs::LocalFileSystemOptions::Defaults());
  return garrow_local_file_system_options_new_raw(&arrow_local_file_system_options);
}

/* arrow::fs::LocalFileSystem */

G_DEFINE_TYPE(GArrowLocalFileSystem,
              garrow_local_file_system,
              GARROW_TYPE_FILE_SYSTEM)

static void
garrow_local_file_system_init(GArrowLocalFileSystem *file_system)
{
}

static void
garrow_local_file_system_class_init(GArrowLocalFileSystemClass *klass)
{
}

/**
 * garrow_local_file_system_new:
 *
 * Returns: (transfer full): A newly created #GArrowLocalFileSystem.
 *
 * Since: 1.0.0
 */
GArrowLocalFileSystem *
garrow_local_file_system_new()
{
  auto arrow_local_file_system = std::make_shared<arrow::fs::LocalFileSystem>();
  std::shared_ptr<arrow::fs::FileSystem> arrow_file_system = arrow_local_file_system;
  auto file_system = garrow_file_system_new_raw(&arrow_file_system, GARROW_TYPE_LOCAL_FILE_SYSTEM);
  return GARROW_LOCAL_FILE_SYSTEM(file_system);
}

/**
 * garrow_local_file_system_new_with_options:
 * @options: A #GArrowLocalFileSystemOptions.
 *
 * Returns: (transfer full): A newly created #GArrowLocalFileSystem.
 *
 * Since: 1.0.0
 */
GArrowLocalFileSystem *
garrow_local_file_system_new_with_options(GArrowLocalFileSystemOptions *options)
{
  auto arrow_options = garrow_local_file_system_options_get_raw(options);
  auto arrow_local_file_system = std::make_shared<arrow::fs::LocalFileSystem>(*arrow_options);
  std::shared_ptr<arrow::fs::FileSystem> arrow_file_system = arrow_local_file_system;
  auto file_system = garrow_file_system_new_raw(&arrow_file_system, GARROW_TYPE_LOCAL_FILE_SYSTEM);
  return GARROW_LOCAL_FILE_SYSTEM(file_system);
}

G_END_DECLS

GArrowLocalFileSystemOptions *
garrow_local_file_system_options_new_raw(
    std::shared_ptr<arrow::fs::LocalFileSystemOptions> *arrow_local_file_system_options)
{
  return GARROW_LOCAL_FILE_SYSTEM_OPTIONS(g_object_new(
        GARROW_TYPE_LOCAL_FILE_SYSTEM_OPTIONS,
        "local_file_system_options", arrow_local_file_system_options,
        NULL));
}

std::shared_ptr<arrow::fs::LocalFileSystemOptions>
garrow_local_file_system_options_get_raw(GArrowLocalFileSystemOptions *local_file_system_options)
{
  if (!local_file_system_options)
    return nullptr;

  auto priv = GARROW_LOCAL_FILE_SYSTEM_OPTIONS_GET_PRIVATE(local_file_system_options);
  return priv->local_file_system_options;
}
