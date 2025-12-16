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

typedef struct GArrowExecutorPrivate_
{
  arrow::internal::Executor *executor;
} GArrowExecutorPrivate;

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowExecutor, garrow_executor, G_TYPE_OBJECT)

#define GARROW_EXECUTOR_GET_PRIVATE(obj)                                                 \
  static_cast<GArrowExecutorPrivate *>(                                                  \
    garrow_executor_get_instance_private(GARROW_EXECUTOR(obj)))

static void
garrow_executor_init(GArrowExecutor *object)
{
  auto priv = GARROW_EXECUTOR_GET_PRIVATE(object);
  priv->executor = nullptr;
}

static void
garrow_executor_class_init(GArrowExecutorClass *klass)
{
}

typedef struct GArrowThreadPoolPrivate_
{
  std::shared_ptr<arrow::internal::ThreadPool> thread_pool;
} GArrowThreadPoolPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowThreadPool, garrow_thread_pool, GARROW_TYPE_EXECUTOR)

#define GARROW_THREAD_POOL_GET_PRIVATE(obj)                                              \
  static_cast<GArrowThreadPoolPrivate *>(                                                \
    garrow_thread_pool_get_instance_private(GARROW_THREAD_POOL(obj)))

enum {
  PROP_THREAD_POOL = 1,
};

static void
garrow_thread_pool_finalize(GObject *object)
{
  auto priv = GARROW_THREAD_POOL_GET_PRIVATE(object);
  priv->thread_pool.~shared_ptr();
  G_OBJECT_CLASS(garrow_thread_pool_parent_class)->finalize(object);
}

static void
garrow_thread_pool_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_THREAD_POOL_GET_PRIVATE(object);
  auto executor_priv = GARROW_EXECUTOR_GET_PRIVATE(GARROW_EXECUTOR(object));

  switch (prop_id) {
  case PROP_THREAD_POOL:
    priv->thread_pool = *static_cast<std::shared_ptr<arrow::internal::ThreadPool> *>(
      g_value_get_pointer(value));
    if (priv->thread_pool) {
      executor_priv->executor = priv->thread_pool.get();
    } else {
      executor_priv->executor = nullptr;
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_thread_pool_init(GArrowThreadPool *object)
{
  auto priv = GARROW_THREAD_POOL_GET_PRIVATE(object);
  new (&priv->thread_pool) std::shared_ptr<arrow::internal::ThreadPool>;
  auto executor_priv = GARROW_EXECUTOR_GET_PRIVATE(GARROW_EXECUTOR(object));
  executor_priv->executor = nullptr;
}

static void
garrow_thread_pool_class_init(GArrowThreadPoolClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_thread_pool_finalize;
  gobject_class->set_property = garrow_thread_pool_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "thread-pool",
    "Thread pool",
    "The raw std::shared_ptr<arrow::internal::ThreadPool> *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_THREAD_POOL, spec);
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
      g_object_new(GARROW_TYPE_THREAD_POOL, "thread-pool", &arrow_thread_pool, nullptr));
    return thread_pool;
  } else {
    return NULL;
  }
}

G_END_DECLS

arrow::internal::Executor *
garrow_executor_get_raw(GArrowExecutor *executor)
{
  if (!executor)
    return nullptr;

  auto priv = GARROW_EXECUTOR_GET_PRIVATE(executor);
  return priv->executor;
}

std::shared_ptr<arrow::internal::ThreadPool>
garrow_thread_pool_get_raw(GArrowThreadPool *thread_pool)
{
  if (!thread_pool)
    return nullptr;

  auto priv = GARROW_THREAD_POOL_GET_PRIVATE(thread_pool);
  return priv->thread_pool;
}
