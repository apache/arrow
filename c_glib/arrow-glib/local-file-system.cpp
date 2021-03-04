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

/**
 * SECTION: local-file-system
 * @section_id: local-file-system-classes
 * @title: Local file system classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowLocalFileSystemOptions is a class for specifyiing options of
 * an instance of #GArrowLocalFileSystem.
 *
 * #GArrowLocalFileSystem is a class for an implementation of a file system
 * that accesses files on the local machine.
 */

typedef struct GArrowLocalFileSystemOptionsPrivate_ {
  arrow::fs::LocalFileSystemOptions local_file_system_options;
} GArrowLocalFileSystemOptionsPrivate;

enum {
  PROP_LOCAL_FILE_SYSTEM_OPTIONS_USE_MMAP = 1,
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

  priv->local_file_system_options.~LocalFileSystemOptions();

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
  case PROP_LOCAL_FILE_SYSTEM_OPTIONS_USE_MMAP:
    priv->local_file_system_options.use_mmap = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_local_file_system_options_get_property(GObject *object,
                                              guint prop_id,
                                              GValue *value,
                                              GParamSpec *pspec)
{
  auto priv = GARROW_LOCAL_FILE_SYSTEM_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_LOCAL_FILE_SYSTEM_OPTIONS_USE_MMAP:
    g_value_set_boolean(value, priv->local_file_system_options.use_mmap);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_local_file_system_options_init(GArrowLocalFileSystemOptions *object)
{
  auto priv = GARROW_LOCAL_FILE_SYSTEM_OPTIONS_GET_PRIVATE(object);
  new(&priv->local_file_system_options) arrow::fs::LocalFileSystemOptions;
}

static void
garrow_local_file_system_options_class_init(GArrowLocalFileSystemOptionsClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_local_file_system_options_finalize;
  gobject_class->set_property = garrow_local_file_system_options_set_property;
  gobject_class->get_property = garrow_local_file_system_options_get_property;

  auto local_file_system_options = arrow::fs::LocalFileSystemOptions::Defaults();

  /**
   * GArrowLocalFileSystemOptions:use-mmap:
   *
   * Whether open_input_stream and open_input_file return a mmap'ed file,
   * or a regular one.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_boolean("use-mmap",
                              "Use mmap",
                              "Whether to use mmap",
                              local_file_system_options.use_mmap,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_LOCAL_FILE_SYSTEM_OPTIONS_USE_MMAP,
                                  spec);
}

/**
 * garrow_local_file_system_options_new:
 *
 * Returns: (transfer full): A newly created #GArrowLocalFileSystemOptions.
 *
 * Since: 0.17.0
 */
GArrowLocalFileSystemOptions *
garrow_local_file_system_options_new(void)
{
  return GARROW_LOCAL_FILE_SYSTEM_OPTIONS(
    g_object_new(GARROW_TYPE_LOCAL_FILE_SYSTEM_OPTIONS, NULL));
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
 * @options: (nullable): A #GArrowLocalFileSystemOptions.
 *
 * Returns: (transfer full): A newly created #GArrowLocalFileSystem.
 *
 * Since: 0.17.0
 */
GArrowLocalFileSystem *
garrow_local_file_system_new(GArrowLocalFileSystemOptions *options)
{
  if (options) {
    const auto &arrow_options =
      garrow_local_file_system_options_get_raw(options);
    auto arrow_local_file_system =
      std::static_pointer_cast<arrow::fs::FileSystem>(
        std::make_shared<arrow::fs::LocalFileSystem>(arrow_options));
    return garrow_local_file_system_new_raw(&arrow_local_file_system);
  } else {
    auto arrow_local_file_system =
      std::static_pointer_cast<arrow::fs::FileSystem>(
        std::make_shared<arrow::fs::LocalFileSystem>());
    return garrow_local_file_system_new_raw(&arrow_local_file_system);
  }
}

G_END_DECLS

arrow::fs::LocalFileSystemOptions &
garrow_local_file_system_options_get_raw(GArrowLocalFileSystemOptions *options)
{
  auto priv = GARROW_LOCAL_FILE_SYSTEM_OPTIONS_GET_PRIVATE(options);
  return priv->local_file_system_options;
}

GArrowLocalFileSystem *
garrow_local_file_system_new_raw(std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system)
{
  return GARROW_LOCAL_FILE_SYSTEM(
    g_object_new(GARROW_TYPE_LOCAL_FILE_SYSTEM,
                 "file-system", arrow_file_system,
                 NULL));
}
