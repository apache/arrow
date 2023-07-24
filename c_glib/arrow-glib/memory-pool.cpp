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

#include <arrow-glib/memory-pool.hpp>

G_BEGIN_DECLS

/**
 * SECTION: memory-pool
 * @section_id: memory-pool-classes
 * @title: Memory pool classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowMemoryPool is a class for memory allocation.
 */

typedef struct GArrowMemoryPoolPrivate_ {
  arrow::MemoryPool *memory_pool;
} GArrowMemoryPoolPrivate;

enum {
  PROP_MEMORY_POOL = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowMemoryPool, garrow_memory_pool, G_TYPE_OBJECT)

#define GARROW_MEMORY_POOL_GET_PRIVATE(obj)        \
  static_cast<GArrowMemoryPoolPrivate *>(          \
     garrow_memory_pool_get_instance_private(      \
       GARROW_MEMORY_POOL(obj)))

static void
garrow_memory_pool_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_MEMORY_POOL_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_MEMORY_POOL:
    priv->memory_pool =
      static_cast<arrow::MemoryPool *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_memory_pool_init(GArrowMemoryPool *object)
{
}

static void
garrow_memory_pool_class_init(GArrowMemoryPoolClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_memory_pool_set_property;

  spec = g_param_spec_pointer("memory-pool",
                              "Memory Pool",
                              "The raw arrow::MemoryPool *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_MEMORY_POOL, spec);
}

/**
 * garrow_memory_pool_default:
 *
 * Returns: (transfer full): The process-wide default memory pool.
 *
 * Since: 9.0.0
 */
GArrowMemoryPool *
garrow_memory_pool_default()
{
  auto memory_pool = arrow::default_memory_pool();
  return garrow_memory_pool_new_raw(memory_pool);
}

/**
 * garrow_memory_pool_get_bytes_allocated:
 * @memory_pool: A #GArrowMemoryPool.
 *
 * Returns: The number of bytes that were allocated and not yet free’d
 *   through this allocator.
 *
 * Since: 9.0.0
 */
gint64
garrow_memory_pool_get_bytes_allocated(GArrowMemoryPool *memory_pool)
{
  auto arrow_memory_pool = garrow_memory_pool_get_raw(memory_pool);
  return arrow_memory_pool->bytes_allocated();
}

/**
 * garrow_memory_pool_get_max_memory:
 * @memory_pool: A #GArrowMemoryPool.
 *
 * Return peak memory allocation in this memory pool.
 *
 * Returns: Maximum bytes allocated. If not known (or not implemented),
 *   returns -1.
 *
 * Since: 9.0.0
 */
gint64
garrow_memory_pool_get_max_memory(GArrowMemoryPool *memory_pool)
{
  auto arrow_memory_pool = garrow_memory_pool_get_raw(memory_pool);
  return arrow_memory_pool->max_memory();
}

/**
 * garrow_memory_pool_get_backend_name:
 * @memory_pool: A #GArrowMemoryPool.
 *
 * Returns: The name of the backend used by this MemoryPool
 *   (e.g. "system" or "jemalloc").
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 9.0.0
 */
gchar *
garrow_memory_pool_get_backend_name(GArrowMemoryPool *memory_pool)
{
  auto arrow_memory_pool = garrow_memory_pool_get_raw(memory_pool);
  const auto name = arrow_memory_pool->backend_name();
  return g_strdup(name.c_str());
}

G_END_DECLS

GArrowMemoryPool *
garrow_memory_pool_new_raw(arrow::MemoryPool *memory_pool)
{
  return GARROW_MEMORY_POOL(g_object_new(GARROW_TYPE_MEMORY_POOL,
                            "memory-pool", memory_pool,
                            NULL));
}

arrow::MemoryPool *
garrow_memory_pool_get_raw(GArrowMemoryPool *memory_pool)
{
  if (!memory_pool)
    return nullptr;

  auto priv = GARROW_MEMORY_POOL_GET_PRIVATE(memory_pool);
  return priv->memory_pool;
}
