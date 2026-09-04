// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/buffer.h"
#include "arrow/c/dlpack.h"
#include "arrow/c/dlpack_abi.h"
#include "arrow/memory_pool.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::dlpack {

struct LegacyProducer {
  using ManagedTensor = DLManagedTensor;
  static constexpr bool copy = false;  // Unsupported
  static constexpr const char* name = "Legacy";

  static Result<ManagedTensor*> Export(const std::shared_ptr<Array>& arr) {
    return ExportArray(arr);
  }
  static Result<ManagedTensor*> Export(const std::shared_ptr<Tensor>& t) {
    return ExportTensor(t);
  }
};

template <bool kCopy>
struct VersionedProducer {
  using ManagedTensor = DLManagedTensorVersioned;
  static constexpr bool copy = kCopy;
  static constexpr const char* name = copy ? "VersionedCopied" : "Versioned";

  static Result<ManagedTensor*> Export(const std::shared_ptr<Array>& arr) {
    return ExportArrayVersioned(arr, copy);
  }
  static Result<ManagedTensor*> Export(const std::shared_ptr<Tensor>& t) {
    return ExportTensorVersioned(t, copy);
  }
};

using ProducerTypes =
    ::testing::Types<LegacyProducer, VersionedProducer<false>, VersionedProducer<true>>;

struct ProducerNames {
  template <typename Producer>
  static std::string GetName(int) {
    return Producer::name;
  }
};

template <typename Producer>
class TestExportArray : public ::testing::Test {};

TYPED_TEST_SUITE(TestExportArray, ProducerTypes, ProducerNames);

template <typename Producer>
void CheckDLTensor(const std::shared_ptr<Array>& arr,
                   const std::shared_ptr<DataType>& arrow_type,
                   DLDataTypeCode dlpack_type, const std::vector<int64_t>& shape,
                   const std::vector<int64_t>& strides) {
  ASSERT_OK_AND_ASSIGN(auto* dlmtensor, Producer::Export(arr));
  auto dltensor = dlmtensor->dl_tensor;

  ASSERT_EQ(arrow_type->id(), arr->type_id());
  const auto byte_width = arrow_type->byte_width();
  const auto start = arr->offset() * byte_width;
  ASSERT_OK_AND_ASSIGN(auto sliced_buffer,
                       SliceBufferSafe(arr->data()->buffers[1], start));
  if constexpr (Producer::copy) {
    ASSERT_NE(sliced_buffer->data(), dltensor.data);
    ASSERT_EQ(
        0, std::memcmp(sliced_buffer->data(), dltensor.data, arr->length() * byte_width));
  } else {
    ASSERT_EQ(sliced_buffer->data(), dltensor.data);
  }

  ASSERT_EQ(0, dltensor.byte_offset);
  ASSERT_EQ(shape.size(), static_cast<size_t>(dltensor.ndim));
  ASSERT_THAT(shape, ::testing::ElementsAreArray(dltensor.shape, dltensor.ndim));
  ASSERT_THAT(strides, ::testing::ElementsAreArray(dltensor.strides, dltensor.ndim));

  ASSERT_EQ(dlpack_type, dltensor.dtype.code);
  ASSERT_EQ(arrow_type->bit_width(), dltensor.dtype.bits);
  ASSERT_EQ(1, dltensor.dtype.lanes);
  ASSERT_EQ(DLDeviceType::kDLCPU, dltensor.device.device_type);
  ASSERT_EQ(0, dltensor.device.device_id);

  ASSERT_OK_AND_ASSIGN(auto device, arrow::dlpack::ExportDevice(arr));
  ASSERT_EQ(DLDeviceType::kDLCPU, device.device_type);
  ASSERT_EQ(0, device.device_id);

  if constexpr (std::is_same_v<decltype(dlmtensor), DLManagedTensorVersioned*>) {
    ASSERT_EQ(DLPACK_MAJOR_VERSION, dlmtensor->version.major);
    ASSERT_EQ(DLPACK_MINOR_VERSION, dlmtensor->version.minor);
    if constexpr (Producer::copy) {
      // Arrow array data is immutable once constructed, but a copy is ours to hand out
      ASSERT_EQ(dlmtensor->flags, DLPACK_FLAG_BITMASK_IS_COPIED);
    } else {
      ASSERT_EQ(dlmtensor->flags, DLPACK_FLAG_BITMASK_READ_ONLY);
    }
  }

  dlmtensor->deleter(dlmtensor);
}

TYPED_TEST(TestExportArray, TestSupportedArray) {
  const std::vector<std::pair<std::shared_ptr<DataType>, DLDataTypeCode>> cases = {
      {int8(), DLDataTypeCode::kDLInt},
      {uint8(), DLDataTypeCode::kDLUInt},
      {
          int16(),
          DLDataTypeCode::kDLInt,
      },
      {uint16(), DLDataTypeCode::kDLUInt},
      {
          int32(),
          DLDataTypeCode::kDLInt,
      },
      {uint32(), DLDataTypeCode::kDLUInt},
      {
          int64(),
          DLDataTypeCode::kDLInt,
      },
      {uint64(), DLDataTypeCode::kDLUInt},
      {float16(), DLDataTypeCode::kDLFloat},
      {float32(), DLDataTypeCode::kDLFloat},
      {float64(), DLDataTypeCode::kDLFloat}};

  const auto allocated_bytes = arrow::default_memory_pool()->bytes_allocated();

  for (auto [arrow_type, dlpack_type] : cases) {
    const std::shared_ptr<Array> array =
        ArrayFromJSON(arrow_type, "[1, 0, 10, 0, 2, 1, 3, 5, 1, 0]");
    CheckDLTensor<TypeParam>(array, arrow_type, dlpack_type, {10}, {1});
    ASSERT_OK_AND_ASSIGN(auto sliced_1, array->SliceSafe(1, 5));
    CheckDLTensor<TypeParam>(sliced_1, arrow_type, dlpack_type, {5}, {1});
    ASSERT_OK_AND_ASSIGN(auto sliced_2, array->SliceSafe(0, 5));
    CheckDLTensor<TypeParam>(sliced_2, arrow_type, dlpack_type, {5}, {1});
    ASSERT_OK_AND_ASSIGN(auto sliced_3, array->SliceSafe(3));
    CheckDLTensor<TypeParam>(sliced_3, arrow_type, dlpack_type, {7}, {1});
  }

  ASSERT_EQ(allocated_bytes, arrow::default_memory_pool()->bytes_allocated());
}

TYPED_TEST(TestExportArray, TestErrors) {
  const std::shared_ptr<Array> array_null = ArrayFromJSON(null(), "[]");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: DataType is not compatible with DLPack spec: " +
                                 array_null->type()->ToString() +
                                 ", try converting to a Tensor for multi"
                                 " dimensional data support",
                             TypeParam::Export(array_null));

  const std::shared_ptr<Array> array_with_null = ArrayFromJSON(int8(), "[1, 100, null]");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: Can only use DLPack on arrays with no nulls.",
                             TypeParam::Export(array_with_null));

  const std::shared_ptr<Array> array_string =
      ArrayFromJSON(utf8(), R"(["itsy", "bitsy", "spider"])");
  ASSERT_RAISES_WITH_MESSAGE(TypeError,
                             "Type error: DataType is not compatible with DLPack spec: " +
                                 array_string->type()->ToString() +
                                 ", try converting to a Tensor for multi"
                                 " dimensional data support",
                             TypeParam::Export(array_string));

  const std::shared_ptr<Array> array_boolean = ArrayFromJSON(boolean(), "[true, false]");
  ASSERT_RAISES_WITH_MESSAGE(
      TypeError, "Type error: Bit-packed boolean data type not supported by DLPack.",
      TypeParam::Export(array_boolean));

  // ExportDevice only reports the device, it does not validate the type
  ASSERT_OK(arrow::dlpack::ExportDevice(array_boolean));
  ASSERT_OK(arrow::dlpack::ExportDevice(array_null));
}

template <typename Producer>
class TestExportTensor : public ::testing::Test {};

TYPED_TEST_SUITE(TestExportTensor, ProducerTypes, ProducerNames);

template <typename Producer>
void CheckDLTensor(const std::shared_ptr<Tensor>& t,
                   const std::shared_ptr<DataType>& tensor_type,
                   DLDataTypeCode dlpack_type, std::vector<int64_t> shape,
                   std::vector<int64_t> strides) {
  ASSERT_OK_AND_ASSIGN(auto* dlmtensor, Producer::Export(t));
  auto dltensor = dlmtensor->dl_tensor;

  if constexpr (Producer::copy) {
    ASSERT_NE(t->data()->data(), dltensor.data);
    ASSERT_EQ(0, std::memcmp(t->data()->data(), dltensor.data, t->data()->size()));
  } else {
    ASSERT_EQ(t->data()->data(), dltensor.data);
  }
  ASSERT_EQ(t->ndim(), dltensor.ndim);
  ASSERT_EQ(0, dltensor.byte_offset);
  for (int i = 0; i < t->ndim(); i++) {
    ASSERT_EQ(shape.data()[i], dltensor.shape[i]);
    ASSERT_EQ(strides.data()[i], dltensor.strides[i]);
  }

  ASSERT_EQ(dlpack_type, dltensor.dtype.code);
  ASSERT_EQ(tensor_type->bit_width(), dltensor.dtype.bits);
  ASSERT_EQ(1, dltensor.dtype.lanes);
  ASSERT_EQ(DLDeviceType::kDLCPU, dltensor.device.device_type);
  ASSERT_EQ(0, dltensor.device.device_id);

  ASSERT_OK_AND_ASSIGN(auto device, arrow::dlpack::ExportDevice(t));
  ASSERT_EQ(DLDeviceType::kDLCPU, device.device_type);
  ASSERT_EQ(0, device.device_id);

  if constexpr (std::is_same_v<decltype(dlmtensor), DLManagedTensorVersioned*>) {
    ASSERT_EQ(DLPACK_MAJOR_VERSION, dlmtensor->version.major);
    ASSERT_EQ(DLPACK_MINOR_VERSION, dlmtensor->version.minor);
    if constexpr (Producer::copy) {
      ASSERT_EQ(dlmtensor->flags, DLPACK_FLAG_BITMASK_IS_COPIED);
    } else {
      ASSERT_EQ(dlmtensor->flags, (t->is_mutable() ? 0 : DLPACK_FLAG_BITMASK_READ_ONLY));
    }
  }

  dlmtensor->deleter(dlmtensor);
}

TYPED_TEST(TestExportTensor, TestTensor) {
  const std::vector<std::pair<std::shared_ptr<DataType>, DLDataTypeCode>> cases = {
      {int8(), DLDataTypeCode::kDLInt},
      {uint8(), DLDataTypeCode::kDLUInt},
      {
          int16(),
          DLDataTypeCode::kDLInt,
      },
      {uint16(), DLDataTypeCode::kDLUInt},
      {
          int32(),
          DLDataTypeCode::kDLInt,
      },
      {uint32(), DLDataTypeCode::kDLUInt},
      {
          int64(),
          DLDataTypeCode::kDLInt,
      },
      {uint64(), DLDataTypeCode::kDLUInt},
      {float16(), DLDataTypeCode::kDLFloat},
      {float32(), DLDataTypeCode::kDLFloat},
      {float64(), DLDataTypeCode::kDLFloat}};

  const auto allocated_bytes = arrow::default_memory_pool()->bytes_allocated();

  for (auto [arrow_type, dlpack_type] : cases) {
    std::vector<int64_t> shape = {3, 6};
    std::vector<int64_t> dlpack_strides = {6, 1};
    std::shared_ptr<Tensor> tensor = TensorFromJSON(
        arrow_type, "[1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9]", shape);

    CheckDLTensor<TypeParam>(tensor, arrow_type, dlpack_type, shape, dlpack_strides);
  }

  ASSERT_EQ(allocated_bytes, arrow::default_memory_pool()->bytes_allocated());
}

TYPED_TEST(TestExportTensor, TestTensorReadOnly) {
  const std::vector<int64_t> shape = {2, 2};
  const std::vector<int64_t> dlpack_strides = {2, 1};
  std::shared_ptr<Tensor> tensor = TensorFromJSON(float32(), "[1, 2, 3, 4]", shape);
  ASSERT_TRUE(tensor->is_mutable());

  // Slicing yields an immutable view of the same data
  ASSERT_OK_AND_ASSIGN(auto read_only_buffer, SliceBufferSafe(tensor->data(), 0));
  ASSERT_OK_AND_ASSIGN(auto read_only_tensor,
                       Tensor::Make(float32(), read_only_buffer, shape));
  ASSERT_FALSE(read_only_tensor->is_mutable());

  if constexpr (std::is_same_v<typename TypeParam::ManagedTensor, DLManagedTensor>) {
    ASSERT_RAISES_WITH_MESSAGE(
        NotImplemented,
        "NotImplemented: Legacy DLPack support is not implemented for immutable tensors."
        " Please move to the DLPack version >=1.0",
        TypeParam::Export(read_only_tensor));
  } else {
    CheckDLTensor<TypeParam>(read_only_tensor, float32(), DLDataTypeCode::kDLFloat, shape,
                             dlpack_strides);
  }
}

TYPED_TEST(TestExportTensor, TestTensorStrided) {
  std::vector<int64_t> shape = {2, 2, 2};
  std::vector<int64_t> strides = {sizeof(float) * 4, sizeof(float) * 2,
                                  sizeof(float) * 1};
  std::vector<int64_t> dlpack_strides = {4, 2, 1};
  std::shared_ptr<Tensor> tensor =
      TensorFromJSON(float32(), "[1, 2, 3, 4, 5, 6, 1, 1]", shape, strides);

  CheckDLTensor<TypeParam>(tensor, float32(), DLDataTypeCode::kDLFloat, shape,
                           dlpack_strides);

  std::vector<int64_t> f_strides = {sizeof(float) * 1, sizeof(float) * 2,
                                    sizeof(float) * 4};
  std::vector<int64_t> f_dlpack_strides = {1, 2, 4};
  std::shared_ptr<Tensor> f_tensor =
      TensorFromJSON(float32(), "[1, 2, 3, 4, 5, 6, 1, 1]", shape, f_strides);

  CheckDLTensor<TypeParam>(f_tensor, float32(), DLDataTypeCode::kDLFloat, shape,
                           f_dlpack_strides);
}

/***************
 *  Consumers  *
 ***************/

/// A DLPack tensor as a foreign library would produce it.
struct ForeignTensor {
  DLDataType dtype = {.code = kDLFloat, .bits = 32, .lanes = 1};
  std::vector<int64_t> shape = {};
  /// In number of elements, as mandated by DLPack.
  std::vector<int64_t> strides = {};
  std::vector<uint8_t> data = {};
  DLDevice device = {.device_type = kDLCPU, .device_id = 0};
  uint64_t byte_offset = 0;
  uint64_t flags = 0;
  /// Incremented when the consumer releases the tensor.
  std::shared_ptr<int> deleted = std::make_shared<int>(0);

  DLManagedTensorVersioned managed = {};
};

template <typename T>
std::vector<uint8_t> ToBytes(const std::vector<T>& values) {
  std::vector<uint8_t> bytes(values.size() * sizeof(T));
  std::memcpy(bytes.data(), values.data(), bytes.size());
  return bytes;
}

/// Hand out a DLPack tensor owning ``foreign``, releasing it through its deleter.
DLManagedTensorVersioned* Produce(ForeignTensor foreign) {
  auto owned = std::make_unique<ForeignTensor>(std::move(foreign));
  owned->managed = {
      .version = {.major = DLPACK_MAJOR_VERSION, .minor = DLPACK_MINOR_VERSION},
      .manager_ctx = owned.get(),
      .deleter =
          [](DLManagedTensorVersioned* self) {
            auto* ctx = static_cast<ForeignTensor*>(self->manager_ctx);
            ++(*ctx->deleted);
            delete ctx;
          },
      .flags = owned->flags,
      .dl_tensor =
          {
              .data = owned->data.data(),
              .device = owned->device,
              .ndim = static_cast<int32_t>(owned->shape.size()),
              .dtype = owned->dtype,
              .shape = owned->shape.data(),
              .strides = owned->strides.data(),
              .byte_offset = owned->byte_offset,
          },
  };
  return &owned.release()->managed;
}

template <bool kCopy>
struct TensorConsumer {
  using Imported = std::shared_ptr<Tensor>;
  static constexpr bool copy = kCopy;
  static constexpr const char* name = copy ? "TensorCopied" : "TensorShared";

  static Result<Imported> Import(DLManagedTensorVersioned* raw) {
    return ImportTensorVersioned(raw, copy);
  }
  static std::shared_ptr<DataType> ValueType(const Imported& t) { return t->type(); }
  static const uint8_t* RawData(const Imported& t) { return t->raw_data(); }
  static bool IsMutable(const Imported& t) { return t->is_mutable(); }
  static int64_t Size(const Imported& t) { return t->size(); }
};

template <bool kCopy>
struct ArrayConsumer {
  using Imported = std::shared_ptr<Array>;
  static constexpr bool copy = kCopy;
  static constexpr const char* name = copy ? "ArrayCopied" : "ArrayShared";

  static Result<Imported> Import(DLManagedTensorVersioned* raw) {
    return ImportArrayVersioned(raw, copy);
  }
  static std::shared_ptr<DataType> ValueType(const Imported& arr) { return arr->type(); }
  static const uint8_t* RawData(const Imported& arr) {
    return arr->data()->buffers[1]->data() + arr->offset() * arr->type()->byte_width();
  }
  static bool IsMutable(const Imported& arr) {
    return arr->data()->buffers[1]->is_mutable();
  }
  static int64_t Size(const Imported& arr) { return arr->length(); }
};

struct ConsumerNames {
  template <typename Consumer>
  static std::string GetName(int) {
    return Consumer::name;
  }
};

using ConsumerTypes = ::testing::Types<TensorConsumer<false>, TensorConsumer<true>,
                                       ArrayConsumer<false>, ArrayConsumer<true>>;
using TensorConsumerTypes = ::testing::Types<TensorConsumer<false>, TensorConsumer<true>>;
using ArrayConsumerTypes = ::testing::Types<ArrayConsumer<false>, ArrayConsumer<true>>;

/// Tests sharing the same expectations for Arrow Tensor and Array imports.
template <typename Consumer>
class TestImport : public ::testing::Test {};

TYPED_TEST_SUITE(TestImport, ConsumerTypes, ConsumerNames);

TYPED_TEST(TestImport, Basic) {
  auto foreign = ForeignTensor{
      .shape = {6},
      .strides = {1},
      .data = ToBytes(std::vector<float>{0, 0, 1, 2, 3, 4, 5, 6}),
      .byte_offset = 2 * sizeof(float),
      .flags = DLPACK_FLAG_BITMASK_READ_ONLY,
  };
  const auto deleted = foreign.deleted;
  const auto expected = std::vector<float>{1, 2, 3, 4, 5, 6};
  const auto* values = foreign.data.data() + foreign.byte_offset;

  ASSERT_OK_AND_ASSIGN(auto imported, TypeParam::Import(Produce(std::move(foreign))));

  AssertTypeEqual(*float32(), *TypeParam::ValueType(imported));
  ASSERT_EQ(6, TypeParam::Size(imported));
  // A copy is ours to mutate, whatever the producer flagged
  ASSERT_EQ(TypeParam::copy, TypeParam::IsMutable(imported));
  ASSERT_EQ(0, std::memcmp(TypeParam::RawData(imported), expected.data(),
                           expected.size() * sizeof(float)));

  if constexpr (TypeParam::copy) {
    // The producer tensor is released as soon as its data has been copied
    ASSERT_EQ(1, *deleted);
  } else {
    ASSERT_EQ(TypeParam::RawData(imported), values);
    // The producer tensor is kept alive by the imported data
    ASSERT_EQ(0, *deleted);
    imported.reset();
    ASSERT_EQ(1, *deleted);
  }
}

TYPED_TEST(TestImport, Mutable) {
  auto foreign = ForeignTensor{
      .shape = {4},
      .strides = {1},
      .data = ToBytes(std::vector<float>{1, 2, 3, 4}),
      .flags = 0,
  };
  ASSERT_OK_AND_ASSIGN(auto imported, TypeParam::Import(Produce(std::move(foreign))));
  ASSERT_TRUE(TypeParam::IsMutable(imported));
}

TYPED_TEST(TestImport, NullDeleter) {
  // The DLPack spec allows producers not to set a deleter
  auto* managed =
      Produce({.shape = {2}, .strides = {1}, .data = std::vector<uint8_t>(8)});
  auto* foreign = static_cast<ForeignTensor*>(managed->manager_ctx);
  managed->deleter = nullptr;

  ASSERT_OK_AND_ASSIGN(auto imported, TypeParam::Import(managed));
  imported.reset();
  delete foreign;
}

TYPED_TEST(TestImport, DataTypes) {
  const std::vector<std::pair<DLDataType, std::shared_ptr<DataType>>> cases = {
      {{kDLInt, 8, 1}, int8()},       {{kDLInt, 16, 1}, int16()},
      {{kDLInt, 32, 1}, int32()},     {{kDLInt, 64, 1}, int64()},
      {{kDLUInt, 8, 1}, uint8()},     {{kDLUInt, 16, 1}, uint16()},
      {{kDLUInt, 32, 1}, uint32()},   {{kDLUInt, 64, 1}, uint64()},
      {{kDLFloat, 16, 1}, float16()}, {{kDLFloat, 32, 1}, float32()},
      {{kDLFloat, 64, 1}, float64()}};

  for (const auto& [dtype, expected] : cases) {
    ARROW_SCOPED_TRACE("dtype ", expected->ToString());
    ASSERT_OK_AND_ASSIGN(
        auto imported, TypeParam::Import(Produce(
                           {.dtype = dtype,
                            .shape = {3},
                            .strides = {1},
                            .data = std::vector<uint8_t>(3 * expected->byte_width())})));
    AssertTypeEqual(*expected, *TypeParam::ValueType(imported));
  }
}

TYPED_TEST(TestImport, Empty) {
  // DLPack mandates a null data pointer when the tensor holds no element
  auto* managed = Produce({.shape = {0}, .strides = {1}});
  managed->dl_tensor.data = nullptr;

  ASSERT_OK_AND_ASSIGN(auto imported, TypeParam::Import(managed));
  ASSERT_EQ(0, TypeParam::Size(imported));
}

TYPED_TEST(TestImport, Errors) {
  auto check = [](ForeignTensor foreign, const std::string& message) {
    const auto deleted = foreign.deleted;
    const auto status = TypeParam::Import(Produce(std::move(foreign))).status();
    EXPECT_EQ(message, status.ToStringWithoutContextLines());
    // Ownership is taken even when the import fails
    EXPECT_EQ(1, *deleted);
  };

  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Received null pointer.",
                             TypeParam::Import(nullptr));
  check({.shape = {2},
         .strides = {1},
         .data = std::vector<uint8_t>(8),
         .device = {.device_type = kDLCUDA, .device_id = 0}},
        "NotImplemented: DLPack support is implemented only for buffers on CPU device.");
  check({.dtype = {kDLFloat, 32, 2}, .shape = {2}, .strides = {1}},
        "Type error: Only type with one lane are supported.");
  check({.dtype = {kDLInt, 4, 1}, .shape = {2}, .strides = {1}},
        "Invalid: unsupported integer bit width 4");
  check({.dtype = {kDLBool, 8, 1}, .shape = {2}, .strides = {1}},
        "Invalid: unsupported DLPack type " + std::to_string(kDLBool));
}

TYPED_TEST(TestImport, UnsupportedVersion) {
  auto* managed =
      Produce({.shape = {2}, .strides = {1}, .data = std::vector<uint8_t>(8)});
  const auto deleted = static_cast<ForeignTensor*>(managed->manager_ctx)->deleted;
  const auto major = DLPACK_MAJOR_VERSION + 1;
  managed->version.major = major;

  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Unsupported DLPack major version " +
                                 std::to_string(major) + ", expected " +
                                 std::to_string(DLPACK_MAJOR_VERSION),
                             TypeParam::Import(managed));
  // The spec mandates the deleter to be called on major version mismatch
  ASSERT_EQ(1, *deleted);
}

template <typename Consumer>
class TestImportTensor : public ::testing::Test {};

TYPED_TEST_SUITE(TestImportTensor, TensorConsumerTypes, ConsumerNames);

TYPED_TEST(TestImportTensor, ShapeAndStrides) {
  auto foreign = ForeignTensor{
      .shape = {2, 3},
      .strides = {3, 1},
      .data = ToBytes(std::vector<float>{1, 2, 3, 4, 5, 6}),
  };
  ASSERT_OK_AND_ASSIGN(auto tensor, TypeParam::Import(Produce(std::move(foreign))));

  ASSERT_THAT(tensor->shape(), ::testing::ElementsAre(2, 3));
  // Arrow strides are in bytes, DLPack strides in elements
  ASSERT_THAT(tensor->strides(),
              ::testing::ElementsAre(3 * sizeof(float), sizeof(float)));
}

TYPED_TEST(TestImportTensor, Empty) {
  auto* managed = Produce({.shape = {0, 3}, .strides = {3, 1}});
  managed->dl_tensor.data = nullptr;

  ASSERT_OK_AND_ASSIGN(auto tensor, TypeParam::Import(managed));
  ASSERT_THAT(tensor->shape(), ::testing::ElementsAre(0, 3));
}

TYPED_TEST(TestImportTensor, Strided) {
  auto column_major = ForeignTensor{
      .shape = {2, 3},
      .strides = {1, 2},
      .data = ToBytes(std::vector<float>{1, 2, 3, 4, 5, 6}),
  };
  ASSERT_OK_AND_ASSIGN(auto tensor, TypeParam::Import(Produce(std::move(column_major))));
  ASSERT_TRUE(tensor->is_column_major());

  // A 2x2 window over every other row of a 4x2 buffer
  auto non_contiguous = ForeignTensor{
      .shape = {2, 2},
      .strides = {4, 1},
      .data = ToBytes(std::vector<float>{1, 2, 3, 4, 5, 6, 7, 8}),
  };
  ASSERT_OK_AND_ASSIGN(tensor, TypeParam::Import(Produce(std::move(non_contiguous))));
  ASSERT_FALSE(tensor->is_contiguous());
  ASSERT_EQ(6, tensor->template Value<FloatType>({1, 1}));
}

TYPED_TEST(TestImportTensor, NegativeStrides) {
  auto foreign = ForeignTensor{
      .shape = {2, 2},
      .strides = {-2, 1},
      .data = std::vector<uint8_t>(16),
  };
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: negative strides not supported",
                             TypeParam::Import(Produce(std::move(foreign))));
}

TYPED_TEST(TestImportTensor, RoundTrip) {
  const auto original = TensorFromJSON(float64(), "[1, 2, 3, 4, 5, 6]", {3, 2});

  ASSERT_OK_AND_ASSIGN(auto* managed, ExportTensorVersioned(original, /*copy=*/false));
  ASSERT_OK_AND_ASSIGN(auto tensor, TypeParam::Import(managed));

  ASSERT_TRUE(tensor->Equals(*original));
  if constexpr (!TypeParam::copy) {
    ASSERT_EQ(original->raw_data(), tensor->raw_data());
  }
}

template <typename Consumer>
class TestImportArray : public ::testing::Test {};

TYPED_TEST_SUITE(TestImportArray, ArrayConsumerTypes, ConsumerNames);

TYPED_TEST(TestImportArray, OneDimension) {
  auto foreign = ForeignTensor{
      .dtype = {.code = kDLInt, .bits = 32, .lanes = 1},
      .shape = {4},
      .strides = {1},
      .data = ToBytes(std::vector<int32_t>{1, 2, 3, 4}),
  };
  ASSERT_OK_AND_ASSIGN(auto array, TypeParam::Import(Produce(std::move(foreign))));
  AssertArraysEqual(*ArrayFromJSON(int32(), "[1, 2, 3, 4]"), *array);
}

TYPED_TEST(TestImportArray, Unsupported) {
  auto check = [](ForeignTensor foreign) {
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Only contiguous one dimensional tensor can be imported as"
        " arrays. Try importing to Tensor first.",
        TypeParam::Import(Produce(std::move(foreign))));
  };

  // Only a Tensor can hold more than one dimension
  check({.shape = {2, 3},
         .strides = {3, 1},
         .data = ToBytes(std::vector<float>{1, 2, 3, 4, 5, 6})});
  check({.shape = {0, 3}, .strides = {1, 1}});
  // Array values are contiguous, whatever the dimension count
  check(
      {.shape = {3}, .strides = {2}, .data = ToBytes(std::vector<float>{1, 2, 3, 4, 5})});
  check({.shape = {2, 2}, .strides = {-2, 1}, .data = std::vector<uint8_t>(16)});
}

TYPED_TEST(TestImportArray, RoundTrip) {
  const auto original = ArrayFromJSON(float64(), "[1, 2, 3, 4, 5, 6]");

  ASSERT_OK_AND_ASSIGN(auto* managed, ExportArrayVersioned(original, /*copy=*/false));
  ASSERT_OK_AND_ASSIGN(auto array, TypeParam::Import(managed));

  AssertArraysEqual(*original, *array);
  if constexpr (!TypeParam::copy) {
    ASSERT_EQ(TypeParam::RawData(original), TypeParam::RawData(array));
  }
}

}  // namespace arrow::dlpack
