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

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/tensor.hpp>
#include <arrow-glib/type.hpp>

G_BEGIN_DECLS

/**
 * SECTION: tensor
 * @short_description: Tensor class
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowTensor is a tensor class.
 *
 * Since: 0.3.0
 */

typedef struct GArrowTensorPrivate_ {
  std::shared_ptr<arrow::Tensor> tensor;
} GArrowTensorPrivate;

enum {
  PROP_0,
  PROP_TENSOR
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowTensor, garrow_tensor, G_TYPE_OBJECT)

#define GARROW_TENSOR_GET_PRIVATE(obj)                                   \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj), GARROW_TYPE_TENSOR, GArrowTensorPrivate))

static void
garrow_tensor_finalize(GObject *object)
{
  auto priv = GARROW_TENSOR_GET_PRIVATE(object);

  priv->tensor = nullptr;

  G_OBJECT_CLASS(garrow_tensor_parent_class)->finalize(object);
}

static void
garrow_tensor_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  auto priv = GARROW_TENSOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TENSOR:
    priv->tensor =
      *static_cast<std::shared_ptr<arrow::Tensor> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_tensor_get_property(GObject *object,
                          guint prop_id,
                          GValue *value,
                          GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_tensor_init(GArrowTensor *object)
{
}

static void
garrow_tensor_class_init(GArrowTensorClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_tensor_finalize;
  gobject_class->set_property = garrow_tensor_set_property;
  gobject_class->get_property = garrow_tensor_get_property;

  spec = g_param_spec_pointer("tensor",
                              "Tensor",
                              "The raw std::shared<arrow::Tensor> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_TENSOR, spec);
}

/**
 * garrow_tensor_new:
 * @data_type: A #GArrowDataType that indicates each element type
 *   in the tensor.
 * @data: A #GArrowBuffer that contains tensor data.
 * @shape: (array length=n_dimensions): A list of dimension sizes.
 * @n_dimensions: The number of dimensions.
 * @strides: (array length=n_strides) (nullable): A list of the number of
 *   bytes in each dimension.
 * @n_strides: The number of strides.
 * @dimention_names: (array length=n_dimention_names) (nullable): A list of
 *   dimension names.
 * @n_dimention_names: The number of dimension names
 *
 * Returns: The newly created #GArrowTensor.
 *
 * Since: 0.3.0
 */
GArrowTensor *
garrow_tensor_new(GArrowDataType *data_type,
                  GArrowBuffer *data,
                  gint64 *shape,
                  gsize n_dimensions,
                  gint64 *strides,
                  gsize n_strides,
                  gchar **dimension_names,
                  gsize n_dimension_names)
{
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_data = garrow_buffer_get_raw(data);
  std::vector<int64_t> arrow_shape;
  for (gsize i = 0; i < n_dimensions; ++i) {
    arrow_shape.push_back(shape[i]);
  }
  std::vector<int64_t> arrow_strides;
  for (gsize i = 0; i < n_strides; ++i) {
    arrow_strides.push_back(strides[i]);
  }
  std::vector<std::string> arrow_dimension_names;
  for (gsize i = 0; i < n_dimension_names; ++i) {
    arrow_dimension_names.push_back(dimension_names[i]);
  }
  auto arrow_tensor =
    std::make_shared<arrow::Tensor>(arrow_data_type,
                                    arrow_data,
                                    arrow_shape,
                                    arrow_strides,
                                    arrow_dimension_names);
  auto tensor = garrow_tensor_new_raw(&arrow_tensor);
  return tensor;
}

/**
 * garrow_tensor_equal:
 * @tensor: A #GArrowTensor.
 * @other_tensor: A #GArrowTensor to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_tensor_equal(GArrowTensor *tensor, GArrowTensor *other_tensor)
{
  const auto arrow_tensor = garrow_tensor_get_raw(tensor);
  const auto arrow_other_tensor = garrow_tensor_get_raw(other_tensor);
  return arrow_tensor->Equals(*arrow_other_tensor);
}

/**
 * garrow_tensor_get_value_data_type:
 * @tensor: A #GArrowTensor.
 *
 * Returns: (transfer full): The data type of each value in the tensor.
 *
 * Since: 0.3.0
 */
GArrowDataType *
garrow_tensor_get_value_data_type(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  auto arrow_data_type = arrow_tensor->type();
  return garrow_data_type_new_raw(&arrow_data_type);
}

/**
 * garrow_tensor_get_value_type:
 * @tensor: A #GArrowTensor.
 *
 * Returns: The type of each value in the tensor.
 *
 * Since: 0.3.0
 */
GArrowType
garrow_tensor_get_value_type(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  auto arrow_type = arrow_tensor->type_id();
  return garrow_type_from_raw(arrow_type);
}

/**
 * garrow_tensor_get_buffer:
 * @tensor: A #GArrowTensor.
 *
 * Returns: (transfer full): The data of the tensor.
 *
 * Since: 0.3.0
 */
GArrowBuffer *
garrow_tensor_get_buffer(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  auto arrow_buffer = arrow_tensor->data();
  return garrow_buffer_new_raw(&arrow_buffer);
}

/**
 * garrow_tensor_get_shape:
 * @tensor: A #GArrowTensor.
 * @n_dimensions: (out): The number of dimensions.
 *
 * Returns: (array length=n_dimensions): The shape of the tensor.
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.3.0
 */
gint64 *
garrow_tensor_get_shape(GArrowTensor *tensor, gint *n_dimensions)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  auto arrow_shape = arrow_tensor->shape();
  auto n_dimensions_raw = arrow_shape.size();
  auto shape =
    static_cast<gint64 *>(g_malloc_n(sizeof(gint64), n_dimensions_raw));
  for (gsize i = 0; i < n_dimensions_raw; ++i) {
    shape[i] = arrow_shape[i];
  }
  *n_dimensions = static_cast<gint>(n_dimensions_raw);
  return shape;
}

/**
 * garrow_tensor_get_strides:
 * @tensor: A #GArrowTensor.
 * @n_strides: (out): The number of strides.
 *
 * Returns: (array length=n_strides): The strides of the tensor.
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.3.0
 */
gint64 *
garrow_tensor_get_strides(GArrowTensor *tensor, gint *n_strides)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  auto arrow_strides = arrow_tensor->strides();
  auto n_strides_raw = arrow_strides.size();
  auto strides =
    static_cast<gint64 *>(g_malloc_n(sizeof(gint64), n_strides_raw));
  for (gsize i = 0; i < n_strides_raw; ++i) {
    strides[i] = arrow_strides[i];
  }
  *n_strides = static_cast<gint>(n_strides_raw);
  return strides;
}

/**
 * garrow_tensor_get_n_dimensions:
 * @tensor: A #GArrowTensor.
 *
 * Returns: The number of dimensions of the tensor.
 *
 * Since: 0.3.0
 */
gint
garrow_tensor_get_n_dimensions(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  return arrow_tensor->ndim();
}

/**
 * garrow_tensor_get_dimension_name:
 * @tensor: A #GArrowTensor.
 * @i: The index of the target dimension.
 *
 * Returns: The i-th dimension name of the tensor.
 *
 * Since: 0.3.0
 */
const gchar *
garrow_tensor_get_dimension_name(GArrowTensor *tensor, gint i)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  auto arrow_dimension_name = &(arrow_tensor->dim_name(i));
  return arrow_dimension_name->c_str();
}

/**
 * garrow_tensor_get_size:
 * @tensor: A #GArrowTensor.
 *
 * Returns: The number of value cells in the tensor.
 *
 * Since: 0.3.0
 */
gint64
garrow_tensor_get_size(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  return arrow_tensor->size();
}

/**
 * garrow_tensor_is_mutable:
 * @tensor: A #GArrowTensor.
 *
 * Returns: %TRUE if the tensor is mutable, %FALSE otherwise.
 *
 * Since: 0.3.0
 */
gboolean
garrow_tensor_is_mutable(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  return arrow_tensor->is_mutable();
}

/**
 * garrow_tensor_is_contiguous:
 * @tensor: A #GArrowTensor.
 *
 * Returns: %TRUE if the tensor is contiguous, %FALSE otherwise.
 *
 * Since: 0.3.0
 */
gboolean
garrow_tensor_is_contiguous(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  return arrow_tensor->is_contiguous();
}

/**
 * garrow_tensor_is_row_major:
 * @tensor: A #GArrowTensor.
 *
 * Returns: %TRUE if the tensor is row major a.k.a. C order,
 *   %FALSE otherwise.
 *
 * Since: 0.3.0
 */
gboolean
garrow_tensor_is_row_major(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  return arrow_tensor->is_row_major();
}

/**
 * garrow_tensor_is_column_major:
 * @tensor: A #GArrowTensor.
 *
 * Returns: %TRUE if the tensor is column major a.k.a. Fortran order,
 *   %FALSE otherwise.
 *
 * Since: 0.3.0
 */
gboolean
garrow_tensor_is_column_major(GArrowTensor *tensor)
{
  auto arrow_tensor = garrow_tensor_get_raw(tensor);
  return arrow_tensor->is_column_major();
}

G_END_DECLS

GArrowTensor *
garrow_tensor_new_raw(std::shared_ptr<arrow::Tensor> *arrow_tensor)
{
  auto tensor = GARROW_TENSOR(g_object_new(GARROW_TYPE_TENSOR,
                                           "tensor", arrow_tensor,
                                           NULL));
  return tensor;
}

std::shared_ptr<arrow::Tensor>
garrow_tensor_get_raw(GArrowTensor *tensor)
{
  auto priv = GARROW_TENSOR_GET_PRIVATE(tensor);
  return priv->tensor;
}
