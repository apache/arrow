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
#include <arrow-glib/executor.hpp>

#include <arrow/util/thread_pool.h>

G_BEGIN_DECLS

/**
 * SECTION: executor
 * @section_id: executor-classes
 * @title: Executor classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowExecutor is the base class for executor implementations.
 *
 * #GArrowThreadPool is a class for thread pool management.
 */

struct GArrowExecutorPrivate
{
  std::shared_ptr<arrow::internal::Executor> executor;
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowExecutor, garrow_executor, G_TYPE_OBJECT)

#define GARROW_EXECUTOR_GET_PRIVATE(obj)                                                 \
  static_cast<GArrowExecutorPrivate *>(                                                  \
    garrow_executor_get_instance_private(GARROW_EXECUTOR(obj)))

enum {
  PROP_EXECUTOR = 1,
};

static void
garrow_executor_init(GArrowExecutor *object)
{
  auto priv = GARROW_EXECUTOR_GET_PRIVATE(object);
  new (&priv->executor) std::shared_ptr<arrow::internal::Executor>;
}

static void
garrow_executor_finalize(GObject *object)
{
  auto priv = GARROW_EXECUTOR_GET_PRIVATE(object);
  priv->executor.~shared_ptr();
  G_OBJECT_CLASS(garrow_executor_parent_class)->finalize(object);
}

static void
garrow_executor_set_property(GObject *object,
                             guint prop_id,
                             const GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GARROW_EXECUTOR_GET_PRIVATE(GARROW_EXECUTOR(object));

  switch (prop_id) {
  case PROP_EXECUTOR:
    priv->executor = *static_cast<std::shared_ptr<arrow::internal::Executor> *>(
      g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_executor_class_init(GArrowExecutorClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_executor_finalize;
  gobject_class->set_property = garrow_executor_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "executor",
    "Executor",
    "The raw std::shared_ptr<arrow::internal::Executor> *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_EXECUTOR, spec);
}

G_DEFINE_TYPE(GArrowThreadPool, garrow_thread_pool, GARROW_TYPE_EXECUTOR)

static void
garrow_thread_pool_init(GArrowThreadPool *object)
{
}

static void
garrow_thread_pool_class_init(GArrowThreadPoolClass *klass)
{
}

/**
 * garrow_thread_pool_new:
 * @n_threads: The number of threads in the pool.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowThreadPool on success,
 *   %NULL on error.
 *
 * Since: 23.0.0
 */
GArrowThreadPool *
garrow_thread_pool_new(guint n_threads, GError **error)
{
  auto arrow_thread_pool_result = arrow::internal::ThreadPool::Make(n_threads);
  if (garrow::check(error, arrow_thread_pool_result, "[thread-pool][new]")) {
    auto arrow_thread_pool = *arrow_thread_pool_result;
    auto thread_pool = GARROW_THREAD_POOL(
      g_object_new(GARROW_TYPE_THREAD_POOL, "executor", &arrow_thread_pool, nullptr));
    return thread_pool;
  } else {
    return nullptr;
  }
}

G_END_DECLS

std::shared_ptr<arrow::internal::Executor>
garrow_executor_get_raw(GArrowExecutor *executor)
{
  if (!executor)
    return nullptr;

  auto priv = GARROW_EXECUTOR_GET_PRIVATE(executor);
  return priv->executor;
}
