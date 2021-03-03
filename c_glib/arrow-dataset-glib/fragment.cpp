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

#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-dataset-glib/fragment.hpp>

G_BEGIN_DECLS

/**
 * SECTION: fragment
 * @section_id: fragment
 * @title: Fragment classes
 * @include: arrow-dataset-glib/arrow-dataset-glib.h
 *
 * #GADFragment is a base class for all fragment classes.
 *
 * #GADInMemoryFragment is a class for in-memory fragment.
 *
 * Since: 4.0.0
 */

/* arrow::dataset::Fragment */

typedef struct GADFragmentPrivate_ {
  std::shared_ptr<arrow::dataset::Fragment> fragment;
} GADFragmentPrivate;

enum {
  PROP_FRAGMENT = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GADFragment,
                                    gad_fragment,
                                    G_TYPE_OBJECT)

#define GAD_FRAGMENT_GET_PRIVATE(obj)           \
  static_cast<GADFragmentPrivate *>(            \
    gad_fragment_get_instance_private(          \
      GAD_FRAGMENT(obj)))

static void
gad_fragment_finalize(GObject *object)
{
  auto priv = GAD_FRAGMENT_GET_PRIVATE(object);

  priv->fragment.~shared_ptr();

  G_OBJECT_CLASS(gad_fragment_parent_class)->finalize(object);
}

static void
gad_fragment_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  auto priv = GAD_FRAGMENT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FRAGMENT:
    priv->fragment =
      *static_cast<std::shared_ptr<arrow::dataset::Fragment> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gad_fragment_init(GADFragment *object)
{
  auto priv = GAD_FRAGMENT_GET_PRIVATE(object);
  new(&priv->fragment) std::shared_ptr<arrow::dataset::Fragment>;
}

static void
gad_fragment_class_init(GADFragmentClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gad_fragment_finalize;
  gobject_class->set_property = gad_fragment_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("fragment",
                              "Fragment",
                              "The raw std::shared<arrow::dataset::Fragment> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FRAGMENT, spec);
}

/* arrow::dataset::InMemoryFragment */

G_DEFINE_TYPE(GADInMemoryFragment,
              gad_in_memory_fragment,
              GAD_TYPE_FRAGMENT)

static void
gad_in_memory_fragment_init(GADInMemoryFragment *object)
{
}

static void
gad_in_memory_fragment_class_init(GADInMemoryFragmentClass *klass)
{
}

/**
 * gad_in_memory_fragment_new:
 * @schema: A #GArrowSchema.
 * @record_batches: (array length=n_record_batches):
 *   (element-type GArrowRecordBatch): The record batches of the table.
 * @n_record_batches: The number of record batches.
 *
 * Returns: A newly created #GADInMemoryFragment.
 *
 * Since: 4.0.0
 */
GADInMemoryFragment *
gad_in_memory_fragment_new(GArrowSchema *schema,
                           GArrowRecordBatch **record_batches,
                           gsize n_record_batches)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_record_batches;
  arrow_record_batches.reserve(n_record_batches);
  for (gsize i = 0; i < n_record_batches; ++i) {
    auto arrow_record_batch = garrow_record_batch_get_raw(record_batches[i]);
    arrow_record_batches.push_back(arrow_record_batch);
  }
  auto arrow_in_memory_fragment =
    std::make_shared<arrow::dataset::InMemoryFragment>(arrow_schema,
                                                       arrow_record_batches);
  return gad_in_memory_fragment_new_raw(&arrow_in_memory_fragment);
}

G_END_DECLS

GADFragment *
gad_fragment_new_raw(std::shared_ptr<arrow::dataset::Fragment> *arrow_fragment)
{
  auto fragment =
    GAD_FRAGMENT(g_object_new(GAD_TYPE_FRAGMENT,
                              "fragment", arrow_fragment,
                              NULL));
  return fragment;
}

std::shared_ptr<arrow::dataset::Fragment>
gad_fragment_get_raw(GADFragment *fragment)
{
  auto priv = GAD_FRAGMENT_GET_PRIVATE(fragment);
  return priv->fragment;
}

GADInMemoryFragment *
gad_in_memory_fragment_new_raw(std::shared_ptr<arrow::dataset::InMemoryFragment> *arrow_fragment)
{
  auto fragment =
    GAD_IN_MEMORY_FRAGMENT(g_object_new(GAD_TYPE_IN_MEMORY_FRAGMENT,
                                        "fragment", arrow_fragment,
                                        NULL));
  return fragment;
}
