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

#include <arrow-glib/error.hpp>
#include <arrow-glib/table.hpp>

#include <arrow-dataset-glib/dataset.hpp>
#include <arrow-dataset-glib/scanner.hpp>

G_BEGIN_DECLS

/**
 * SECTION: scanner
 * @section_id: scanner
 * @title: Scanner related classes
 * @include: arrow-dataset-glib/arrow-dataset-glib.h
 *
 * #GADatasetScanner is a class for scanning dataset.
 *
 * #GADatasetScannerBuilder is a class for building a scanner.
 *
 * Since: 5.0.0
 */

typedef struct GADatasetScannerPrivate_ {
  std::shared_ptr<arrow::dataset::Scanner> scanner;
} GADatasetScannerPrivate;

enum {
  PROP_SCANNER = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetScanner,
                           gadataset_scanner,
                           G_TYPE_OBJECT)

#define GADATASET_SCANNER_GET_PRIVATE(obj)        \
  static_cast<GADatasetScannerPrivate *>(         \
    gadataset_scanner_get_instance_private(       \
      GADATASET_SCANNER(obj)))

static void
gadataset_scanner_finalize(GObject *object)
{
  auto priv = GADATASET_SCANNER_GET_PRIVATE(object);
  priv->scanner.~shared_ptr();
  G_OBJECT_CLASS(gadataset_scanner_parent_class)->finalize(object);
}

static void
gadataset_scanner_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GADATASET_SCANNER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCANNER:
    priv->scanner =
      *static_cast<std::shared_ptr<arrow::dataset::Scanner> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_scanner_init(GADatasetScanner *object)
{
  auto priv = GADATASET_SCANNER_GET_PRIVATE(object);
  new(&priv->scanner) std::shared_ptr<arrow::dataset::Scanner>;
}

static void
gadataset_scanner_class_init(GADatasetScannerClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize     = gadataset_scanner_finalize;
  gobject_class->set_property = gadataset_scanner_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("scanner",
                              "Scanner",
                              "The raw std::shared<arrow::dataset::Scanner> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCANNER, spec);
}

/**
 * gadataset_scanner_to_table:
 * @scanner: A #GADatasetScanner.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable):
 *   A newly created #GArrowTable on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GArrowTable *
gadataset_scanner_to_table(GADatasetScanner *scanner,
                           GError **error)
{
  auto arrow_scanner = gadataset_scanner_get_raw(scanner);
  auto arrow_table_result = arrow_scanner->ToTable();
  if (garrow::check(error, arrow_table_result, "[scanner][to-table]")) {
    auto arrow_table = *arrow_table_result;
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
}


typedef struct GADatasetScannerBuilderPrivate_ {
  std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder;
} GADatasetScannerBuilderPrivate;

enum {
  PROP_SCANNER_BUILDER = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetScannerBuilder,
                           gadataset_scanner_builder,
                           G_TYPE_OBJECT)

#define GADATASET_SCANNER_BUILDER_GET_PRIVATE(obj)        \
  static_cast<GADatasetScannerBuilderPrivate *>(          \
    gadataset_scanner_builder_get_instance_private(       \
      GADATASET_SCANNER_BUILDER(obj)))

static void
gadataset_scanner_builder_finalize(GObject *object)
{
  auto priv = GADATASET_SCANNER_BUILDER_GET_PRIVATE(object);
  priv->scanner_builder.~shared_ptr();
  G_OBJECT_CLASS(gadataset_scanner_builder_parent_class)->finalize(object);
}

static void
gadataset_scanner_builder_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GADATASET_SCANNER_BUILDER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCANNER_BUILDER:
    priv->scanner_builder =
      *static_cast<std::shared_ptr<arrow::dataset::ScannerBuilder> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_scanner_builder_init(GADatasetScannerBuilder *object)
{
  auto priv = GADATASET_SCANNER_BUILDER_GET_PRIVATE(object);
  new(&priv->scanner_builder) std::shared_ptr<arrow::dataset::ScannerBuilder>;
}

static void
gadataset_scanner_builder_class_init(GADatasetScannerBuilderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize     = gadataset_scanner_builder_finalize;
  gobject_class->set_property = gadataset_scanner_builder_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("scanner-builder",
                              "Scanner builder",
                              "The raw "
                              "std::shared<arrow::dataset::ScannerBuilder> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCANNER_BUILDER, spec);
}

/**
 * gadataset_scanner_builder_new:
 * @dataset: A #GADatasetDataset to be scanned.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GADatasetScannerBuilder on success,
 *   %NULL on error.
 *
 * Since: 5.0.0
 */
GADatasetScannerBuilder *
gadataset_scanner_builder_new(GADatasetDataset *dataset, GError **error)
{
  auto arrow_dataset = gadataset_dataset_get_raw(dataset);
  auto arrow_scanner_builder_result = arrow_dataset->NewScan();
  if (garrow::check(error,
                    arrow_scanner_builder_result,
                    "[scanner-builder][new]")) {
    auto arrow_scanner_builder = *arrow_scanner_builder_result;
    return gadataset_scanner_builder_new_raw(&arrow_scanner_builder);
  } else {
    return NULL;
  }
}

/**
 * gadataset_scanner_builder_finish:
 * @builder: A #GADatasetScannerBuilder.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable):
 *   A newly created #GADatasetScanner on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GADatasetScanner *
gadataset_scanner_builder_finish(GADatasetScannerBuilder *builder,
                                 GError **error)
{
  auto arrow_builder = gadataset_scanner_builder_get_raw(builder);
  auto arrow_scanner_result = arrow_builder->Finish();
  if (garrow::check(error, arrow_scanner_result, "[scanner-builder][finish]")) {
    auto arrow_scanner = *arrow_scanner_result;
    return gadataset_scanner_new_raw(&arrow_scanner);
  } else {
    return NULL;
  }
}


G_END_DECLS

GADatasetScanner *
gadataset_scanner_new_raw(
  std::shared_ptr<arrow::dataset::Scanner> *arrow_scanner)
{
  auto scanner =
    GADATASET_SCANNER(g_object_new(GADATASET_TYPE_SCANNER,
                                   "scanner", arrow_scanner,
                                   NULL));
  return scanner;
}

std::shared_ptr<arrow::dataset::Scanner>
gadataset_scanner_get_raw(GADatasetScanner *scanner)
{
  auto priv = GADATASET_SCANNER_GET_PRIVATE(scanner);
  return priv->scanner;
}

GADatasetScannerBuilder *
gadataset_scanner_builder_new_raw(
  std::shared_ptr<arrow::dataset::ScannerBuilder> *arrow_scanner_builder)
{
  return GADATASET_SCANNER_BUILDER(
    g_object_new(GADATASET_TYPE_SCANNER_BUILDER,
                 "scanner-builder", arrow_scanner_builder,
                 NULL));
}

std::shared_ptr<arrow::dataset::ScannerBuilder>
gadataset_scanner_builder_get_raw(GADatasetScannerBuilder *scanner_builder)
{
  auto priv = GADATASET_SCANNER_BUILDER_GET_PRIVATE(scanner_builder);
  return priv->scanner_builder;
}
