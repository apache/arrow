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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/compute.hpp>

G_BEGIN_DECLS

/**
 * SECTION: compute
 * @section_id: compute-classes
 * @title: Compute classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowCastOptions is a class to custom garrow_array_cast().
 */

typedef struct GArrowCastOptionsPrivate_ {
  arrow::compute::CastOptions options;
} GArrowCastOptionsPrivate;

enum {
  PROP_0,
  PROP_ALLOW_INT_OVERFLOW
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCastOptions,
                           garrow_cast_options,
                           G_TYPE_OBJECT)

#define GARROW_CAST_OPTIONS_GET_PRIVATE(object) \
  static_cast<GArrowCastOptionsPrivate *>(      \
    garrow_cast_options_get_instance_private(   \
      GARROW_CAST_OPTIONS(object)))

static void
garrow_cast_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ALLOW_INT_OVERFLOW:
    priv->options.allow_int_overflow = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cast_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ALLOW_INT_OVERFLOW:
    g_value_set_boolean(value, priv->options.allow_int_overflow);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cast_options_init(GArrowCastOptions *object)
{
}

static void
garrow_cast_options_class_init(GArrowCastOptionsClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_cast_options_set_property;
  gobject_class->get_property = garrow_cast_options_get_property;

  spec = g_param_spec_boolean("allow_int_overflow",
                              "Allow int overflow",
                              "Whether integer overflow is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_INT_OVERFLOW, spec);
}

/**
 * garrow_cast_options_new:
 *
 * Returns: A newly created #GArrowCastOptions.
 *
 * Since: 0.7.0
 */
GArrowCastOptions *
garrow_cast_options_new(void)
{
  auto cast_options = g_object_new(GARROW_TYPE_CAST_OPTIONS, NULL);
  return GARROW_CAST_OPTIONS(cast_options);
}

G_END_DECLS

GArrowCastOptions *
garrow_cast_options_new_raw(arrow::compute::CastOptions *arrow_cast_options)
{
  auto cast_options =
    g_object_new(GARROW_TYPE_CAST_OPTIONS,
                 "allow-int-overflow", arrow_cast_options->allow_int_overflow,
                 NULL);
  return GARROW_CAST_OPTIONS(cast_options);
}

arrow::compute::CastOptions *
garrow_cast_options_get_raw(GArrowCastOptions *cast_options)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(cast_options);
  return &(priv->options);
}
