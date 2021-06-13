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

#include <arrow/util/iterator.h>

#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-dataset-glib/fragment.hpp>
#include <arrow-dataset-glib/scanner.hpp>

G_BEGIN_DECLS

/**
 * SECTION: scanner
 * @section_id: scanner
 * @title: Scanner classes
 * @include: arrow-dataset-glib/arrow-dataset-glib.h
 *
 * #GADatasetScanOptions is a class for a set of scan options.
 *
 * #GADatasetScanTask is an abstract class for a scan task.
 *
 * #GADatasetInMemoryScanTask is a class for a scan task of record batches.
 *
 * Since: 1.0.0
 */

/* arrow::dataset::ScanOptions */

typedef struct GADatasetScanOptionsPrivate_ {
  std::shared_ptr<arrow::dataset::ScanOptions> scan_options;
} GADatasetScanOptionsPrivate;

enum {
  PROP_SCAN_OPTIONS = 1,
  PROP_FILTER,
  PROP_EVALUATOR,
  PROP_PROJECTOR,
  PROP_BATCH_SIZE,
  PROP_USE_THREADS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetScanOptions,
                           gadataset_scan_options,
                           G_TYPE_OBJECT)

#define GADATASET_SCAN_OPTIONS_GET_PRIVATE(obj)       \
  static_cast<GADatasetScanOptionsPrivate *>(         \
    gadataset_scan_options_get_instance_private(      \
      GADATASET_SCAN_OPTIONS(obj)))

static void
gadataset_scan_options_finalize(GObject *object)
{
  auto priv = GADATASET_SCAN_OPTIONS_GET_PRIVATE(object);

  priv->scan_options.~shared_ptr();

  G_OBJECT_CLASS(gadataset_scan_options_parent_class)->finalize(object);
}

static void
gadataset_scan_options_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GADATASET_SCAN_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCAN_OPTIONS:
    priv->scan_options =
      *static_cast<std::shared_ptr<arrow::dataset::ScanOptions> *>(
        g_value_get_pointer(value));
    break;
  case PROP_BATCH_SIZE:
    priv->scan_options->batch_size = g_value_get_int64(value);
    break;
  case PROP_USE_THREADS:
    priv->scan_options->use_threads = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_scan_options_get_property(GObject *object,
                                    guint prop_id,
                                    GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GADATASET_SCAN_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BATCH_SIZE:
    g_value_set_int64(value, priv->scan_options->batch_size);
    break;
  case PROP_USE_THREADS:
    g_value_set_boolean(value, priv->scan_options->use_threads);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_scan_options_init(GADatasetScanOptions *object)
{
  auto priv = GADATASET_SCAN_OPTIONS_GET_PRIVATE(object);
  new(&priv->scan_options) std::shared_ptr<arrow::dataset::ScanOptions>;
}

static void
gadataset_scan_options_class_init(GADatasetScanOptionsClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gadataset_scan_options_finalize;
  gobject_class->set_property = gadataset_scan_options_set_property;
  gobject_class->get_property = gadataset_scan_options_get_property;

  auto scan_options = std::make_shared<arrow::dataset::ScanOptions>();

  spec = g_param_spec_pointer("scan-options",
                              "ScanOptions",
                              "The raw std::shared<arrow::dataset::ScanOptions> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCAN_OPTIONS, spec);

  // TODO: PROP_FILTER
  // TODO: PROP_EVALUATOR
  // TODO: PROP_PROJECTOR

  /**
   * GADatasetScanOptions:batch-size:
   *
   * Maximum row count for scanned batches.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int64("batch-size",
                            "Batch size",
                            "Maximum row count for scanned batches",
                            0,
                            G_MAXINT64,
                            scan_options->batch_size,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_BATCH_SIZE, spec);

  /**
   * GADatasetScanOptions:use-threads:
   *
   * Indicate if the Scanner should make use of a ThreadPool.
   *
   * Since: 4.0.0
   */
  spec = g_param_spec_boolean("use-threads",
                              "Use threads",
                              "Indicate if the Scanner should make use of a ThreadPool",
                              scan_options->use_threads,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_USE_THREADS, spec);
}

/**
 * gadataset_scan_options_new:
 * @schema: A #GArrowSchema.
 *
 * Returns: A newly created #GADatasetScanOptions.
 *
 * Since: 1.0.0
 */
GADatasetScanOptions *
gadataset_scan_options_new(GArrowSchema *schema)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto arrow_scan_options = std::make_shared<arrow::dataset::ScanOptions>();
  arrow_scan_options->dataset_schema = arrow_schema;
  return gadataset_scan_options_new_raw(&arrow_scan_options);
}

/**
 * gadataset_scan_options_get_schema:
 * @scan_options: A #GADatasetScanOptions.
 *
 * Returns: (transfer full): A #GArrowSchema.
 *
 * Since: 1.0.0
 */
GArrowSchema *
gadataset_scan_options_get_schema(GADatasetScanOptions *scan_options)
{
  auto priv = GADATASET_SCAN_OPTIONS_GET_PRIVATE(scan_options);
  auto arrow_schema = priv->scan_options->dataset_schema;
  return garrow_schema_new_raw(&arrow_schema);
}

/* arrow::dataset::ScanTask */

typedef struct GADatasetScanTaskPrivate_ {
  std::shared_ptr<arrow::dataset::ScanTask> scan_task;
  GADatasetScanOptions *options;
  GADatasetFragment *fragment;
} GADatasetScanTaskPrivate;

enum {
  PROP_SCAN_TASK = 1,
  PROP_OPTIONS,
  PROP_FRAGMENT,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GADatasetScanTask,
                                    gadataset_scan_task,
                                    G_TYPE_OBJECT)

#define GADATASET_SCAN_TASK_GET_PRIVATE(obj)          \
  static_cast<GADatasetScanTaskPrivate *>(            \
    gadataset_scan_task_get_instance_private(         \
      GADATASET_SCAN_TASK(obj)))

static void
gadataset_scan_task_dispose(GObject *object)
{
  auto priv = GADATASET_SCAN_TASK_GET_PRIVATE(object);

  if (priv->options) {
    g_object_unref(priv->options);
    priv->options = NULL;
  }

  if (priv->fragment) {
    g_object_unref(priv->fragment);
    priv->fragment = NULL;
  }

  G_OBJECT_CLASS(gadataset_scan_task_parent_class)->dispose(object);
}

static void
gadataset_scan_task_finalize(GObject *object)
{
  auto priv = GADATASET_SCAN_TASK_GET_PRIVATE(object);

  priv->scan_task.~shared_ptr();

  G_OBJECT_CLASS(gadataset_scan_task_parent_class)->finalize(object);
}

static void
gadataset_scan_task_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GADATASET_SCAN_TASK_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCAN_TASK:
    priv->scan_task =
      *static_cast<std::shared_ptr<arrow::dataset::ScanTask> *>(
        g_value_get_pointer(value));
    break;
  case PROP_OPTIONS:
    priv->options = GADATASET_SCAN_OPTIONS(g_value_dup_object(value));
    break;
  case PROP_FRAGMENT:
    priv->fragment = GADATASET_FRAGMENT(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_scan_task_get_property(GObject *object,
                           guint prop_id,
                           GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GADATASET_SCAN_TASK_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_OPTIONS:
    g_value_set_object(value, priv->options);
    break;
  case PROP_FRAGMENT:
    g_value_set_object(value, priv->fragment);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_scan_task_init(GADatasetScanTask *object)
{
  auto priv = GADATASET_SCAN_TASK_GET_PRIVATE(object);
  new(&priv->scan_task) std::shared_ptr<arrow::dataset::ScanTask>;
}

static void
gadataset_scan_task_class_init(GADatasetScanTaskClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = gadataset_scan_task_dispose;
  gobject_class->finalize     = gadataset_scan_task_finalize;
  gobject_class->set_property = gadataset_scan_task_set_property;
  gobject_class->get_property = gadataset_scan_task_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("scan-task",
                              "ScanTask",
                              "The raw std::shared<arrow::dataset::ScanTask> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCAN_TASK, spec);

  /**
   * GADatasetScanTask:options:
   *
   * The options of the scan task.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_object("options",
                             "Options",
                             "The options of the scan task",
                             GADATASET_TYPE_SCAN_OPTIONS,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_OPTIONS, spec);

  /**
   * GADatasetScanTask:fragment:
   *
   * The fragment of the scan task.
   *
   * Since: 4.0.0
   */
  spec = g_param_spec_object("fragment",
                             "Fragment",
                             "The fragment of the scan task",
                             GADATASET_TYPE_FRAGMENT,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FRAGMENT, spec);
}

/**
 * gadataset_scan_task_get_options:
 * @scan_task: A #GADatasetScanTask.
 *
 * Returns: (transfer full): A #GADatasetScanOptions.
 *
 * Since: 1.0.0
 */
GADatasetScanOptions *
gadataset_scan_task_get_options(GADatasetScanTask *scan_task)
{
  auto priv = GADATASET_SCAN_TASK_GET_PRIVATE(scan_task);
  if (priv->options) {
    g_object_ref(priv->options);
    return priv->options;
  }

  auto arrow_options = priv->scan_task->options();
  return gadataset_scan_options_new_raw(&arrow_options);
}

/**
 * gadataset_scan_task_get_fragment:
 * @scan_task: A #GADatasetFragment.
 *
 * Returns: (transfer full): A #GADatasetFragment.
 *
 * Since: 4.0.0
 */
GADatasetFragment *
gadataset_scan_task_get_fragment(GADatasetScanTask *scan_task)
{
  auto priv = GADATASET_SCAN_TASK_GET_PRIVATE(scan_task);
  if (priv->fragment) {
    g_object_ref(priv->fragment);
    return priv->fragment;
  }

  auto arrow_fragment = priv->scan_task->fragment();
  return gadataset_fragment_new_raw(&arrow_fragment);
}

/**
 * gadataset_scan_task_execute:
 * @scan_task: A #GADatasetScanTask.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly created #GArrowRecordBatchIterator,
 *   or %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowRecordBatchIterator *
gadataset_scan_task_execute(GADatasetScanTask *scan_task,
                            GError **error)
{
  auto priv = GADATASET_SCAN_TASK_GET_PRIVATE(scan_task);
  auto arrow_result = priv->scan_task->Execute();
  if (garrow::check(error, arrow_result, "[datasets][scan-task][execute]")) {
    auto arrow_record_batch_iteraor = std::move(*arrow_result);
    return garrow_record_batch_iterator_new_raw(&arrow_record_batch_iteraor);
  } else {
    return NULL;
  }
}

/* arrow::dataset::InMemoryScanTask */

G_DEFINE_TYPE(GADatasetInMemoryScanTask,
              gadataset_in_memory_scan_task,
              GADATASET_TYPE_SCAN_TASK)

static void
gadataset_in_memory_scan_task_init(GADatasetInMemoryScanTask *object)
{
}

static void
gadataset_in_memory_scan_task_class_init(GADatasetInMemoryScanTaskClass *klass)
{
}

/**
 * gadataset_in_memory_scan_task_new:
 * @record_batches: (array length=n_record_batches):
 *   (element-type GArrowRecordBatch): The record batches of the table.
 * @n_record_batches: The number of record batches.
 * @options: A #GADatasetScanOptions.
 * @fragment: A #GADatasetInMemoryFragment.
 *
 * Returns: A newly created #GADatasetInMemoryScanTask.
 *
 * Since: 1.0.0
 */
GADatasetInMemoryScanTask *
gadataset_in_memory_scan_task_new(GArrowRecordBatch **record_batches,
                                  gsize n_record_batches,
                                  GADatasetScanOptions *options,
                                  GADatasetInMemoryFragment *fragment)
{
  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_record_batches;
  arrow_record_batches.reserve(n_record_batches);
  for (gsize i = 0; i < n_record_batches; ++i) {
    auto arrow_record_batch = garrow_record_batch_get_raw(record_batches[i]);
    arrow_record_batches.push_back(arrow_record_batch);
  }
  auto arrow_options = gadataset_scan_options_get_raw(options);
  auto arrow_fragment = gadataset_fragment_get_raw(GADATASET_FRAGMENT(fragment));
  auto arrow_in_memory_scan_task =
    std::make_shared<arrow::dataset::InMemoryScanTask>(arrow_record_batches,
                                                       arrow_options,
                                                       arrow_fragment);
  return gadataset_in_memory_scan_task_new_raw(&arrow_in_memory_scan_task,
                                         options,
                                         fragment);
}

G_END_DECLS

GADatasetScanOptions *
gadataset_scan_options_new_raw(
  std::shared_ptr<arrow::dataset::ScanOptions> *arrow_scan_options)
{
  auto scan_options =
    GADATASET_SCAN_OPTIONS(g_object_new(GADATASET_TYPE_SCAN_OPTIONS,
                                  "scan-options", arrow_scan_options,
                                  NULL));
  return scan_options;
}

std::shared_ptr<arrow::dataset::ScanOptions>
gadataset_scan_options_get_raw(GADatasetScanOptions *scan_options)
{
  auto priv = GADATASET_SCAN_OPTIONS_GET_PRIVATE(scan_options);
  return priv->scan_options;
}

GADatasetInMemoryScanTask *
gadataset_in_memory_scan_task_new_raw(
  std::shared_ptr<arrow::dataset::InMemoryScanTask> *arrow_in_memory_scan_task,
  GADatasetScanOptions *options,
  GADatasetInMemoryFragment *fragment)
{
  auto in_memory_scan_task =
    GADATASET_IN_MEMORY_SCAN_TASK(g_object_new(GADATASET_TYPE_IN_MEMORY_SCAN_TASK,
                                         "scan-task", arrow_in_memory_scan_task,
                                         "options", options,
                                         "fragment", fragment,
                                         NULL));
  return in_memory_scan_task;
}
