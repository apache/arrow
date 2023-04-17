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

#include <cerrno>
#include <deque>
#include <functional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/c/util_internal.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/endian.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::ArrayExportGuard;
using internal::ArrayExportTraits;
using internal::ArrayStreamExportGuard;
using internal::ArrayStreamExportTraits;
using internal::checked_cast;
using internal::SchemaExportGuard;
using internal::SchemaExportTraits;

template <typename T>
struct ExportTraits {};

template <typename T>
using Exporter = std::function<Status(const T&, struct ArrowSchema*)>;

template <>
struct ExportTraits<DataType> {
  static Exporter<DataType> ExportFunc;
};

template <>
struct ExportTraits<Field> {
  static Exporter<Field> ExportFunc;
};

template <>
struct ExportTraits<Schema> {
  static Exporter<Schema> ExportFunc;
};

Exporter<DataType> ExportTraits<DataType>::ExportFunc = ExportType;
Exporter<Field> ExportTraits<Field>::ExportFunc = ExportField;
Exporter<Schema> ExportTraits<Schema>::ExportFunc = ExportSchema;

// An interceptor that checks whether a release callback was called.
// (for import tests)
template <typename Traits>
class ReleaseCallback {
 public:
  using CType = typename Traits::CType;

  explicit ReleaseCallback(CType* c_struct) : called_(false) {
    orig_release_ = c_struct->release;
    orig_private_data_ = c_struct->private_data;
    c_struct->release = StaticRelease;
    c_struct->private_data = this;
  }

  static void StaticRelease(CType* c_struct) {
    reinterpret_cast<ReleaseCallback*>(c_struct->private_data)->Release(c_struct);
  }

  void Release(CType* c_struct) {
    ASSERT_FALSE(called_) << "ReleaseCallback called twice";
    called_ = true;
    ASSERT_FALSE(Traits::IsReleasedFunc(c_struct))
        << "ReleaseCallback called with released Arrow"
        << (std::is_same<CType, ArrowSchema>::value ? "Schema" : "Array");
    // Call original release callback
    c_struct->release = orig_release_;
    c_struct->private_data = orig_private_data_;
    Traits::ReleaseFunc(c_struct);
    ASSERT_TRUE(Traits::IsReleasedFunc(c_struct))
        << "ReleaseCallback did not release ArrowSchema";
  }

  void AssertCalled() { ASSERT_TRUE(called_) << "ReleaseCallback was not called"; }

  void AssertNotCalled() { ASSERT_FALSE(called_) << "ReleaseCallback was called"; }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ReleaseCallback);

  bool called_;
  void (*orig_release_)(CType*);
  void* orig_private_data_;
};

using SchemaReleaseCallback = ReleaseCallback<SchemaExportTraits>;
using ArrayReleaseCallback = ReleaseCallback<ArrayExportTraits>;

// Whether c_struct or any of its descendents have non-null data pointers.
bool HasData(const ArrowArray* c_struct) {
  for (int64_t i = 0; i < c_struct->n_buffers; ++i) {
    if (c_struct->buffers[i] != nullptr) {
      return true;
    }
  }
  if (c_struct->dictionary && HasData(c_struct->dictionary)) {
    return true;
  }
  for (int64_t i = 0; i < c_struct->n_children; ++i) {
    if (HasData(c_struct->children[i])) {
      return true;
    }
  }
  return false;
}

static const std::vector<std::string> kMetadataKeys1{"key1", "key2"};
static const std::vector<std::string> kMetadataValues1{"", "bar"};

static const std::vector<std::string> kMetadataKeys2{"key"};
static const std::vector<std::string> kMetadataValues2{"abcde"};

// clang-format off
static const std::string kEncodedMetadata1{  // NOLINT: runtime/string
#if ARROW_LITTLE_ENDIAN
    2, 0, 0, 0,
    4, 0, 0, 0, 'k', 'e', 'y', '1', 0, 0, 0, 0,
    4, 0, 0, 0, 'k', 'e', 'y', '2', 3, 0, 0, 0, 'b', 'a', 'r'};
#else
    0, 0, 0, 2,
    0, 0, 0, 4, 'k', 'e', 'y', '1', 0, 0, 0, 0,
    0, 0, 0, 4, 'k', 'e', 'y', '2', 0, 0, 0, 3, 'b', 'a', 'r'};
#endif

static const std::string kEncodedMetadata2{  // NOLINT: runtime/string
#if ARROW_LITTLE_ENDIAN
    1, 0, 0, 0,
    3, 0, 0, 0, 'k', 'e', 'y', 5, 0, 0, 0, 'a', 'b', 'c', 'd', 'e'};
#else
    0, 0, 0, 1,
    0, 0, 0, 3, 'k', 'e', 'y', 0, 0, 0, 5, 'a', 'b', 'c', 'd', 'e'};
#endif

static const std::string kEncodedUuidMetadata =  // NOLINT: runtime/string
#if ARROW_LITTLE_ENDIAN
    std::string {2, 0, 0, 0} +
    std::string {20, 0, 0, 0} + kExtensionTypeKeyName +
    std::string {4, 0, 0, 0} + "uuid" +
    std::string {24, 0, 0, 0} + kExtensionMetadataKeyName +
    std::string {15, 0, 0, 0} + "uuid-serialized";
#else
    std::string {0, 0, 0, 2} +
    std::string {0, 0, 0, 20} + kExtensionTypeKeyName +
    std::string {0, 0, 0, 4} + "uuid" +
    std::string {0, 0, 0, 24} + kExtensionMetadataKeyName +
    std::string {0, 0, 0, 15} + "uuid-serialized";
#endif

static const std::string kEncodedDictExtensionMetadata =  // NOLINT: runtime/string
#if ARROW_LITTLE_ENDIAN
    std::string {2, 0, 0, 0} +
    std::string {20, 0, 0, 0} + kExtensionTypeKeyName +
    std::string {14, 0, 0, 0} + "dict-extension" +
    std::string {24, 0, 0, 0} + kExtensionMetadataKeyName +
    std::string {25, 0, 0, 0} + "dict-extension-serialized";
#else
    std::string {0, 0, 0, 2} +
    std::string {0, 0, 0, 20} + kExtensionTypeKeyName +
    std::string {0, 0, 0, 14} + "dict-extension" +
    std::string {0, 0, 0, 24} + kExtensionMetadataKeyName +
    std::string {0, 0, 0, 25} + "dict-extension-serialized";
#endif

static const std::string kEncodedComplex128Metadata =  // NOLINT: runtime/string
#if ARROW_LITTLE_ENDIAN
    std::string {2, 0, 0, 0} +
    std::string {20, 0, 0, 0} + kExtensionTypeKeyName +
    std::string {10, 0, 0, 0} + "complex128" +
    std::string {24, 0, 0, 0} + kExtensionMetadataKeyName +
    std::string {21, 0, 0, 0} + "complex128-serialized";
#else
    std::string {0, 0, 0, 2} +
    std::string {0, 0, 0, 20} + kExtensionTypeKeyName +
    std::string {0, 0, 0, 10} + "complex128" +
    std::string {0, 0, 0, 24} + kExtensionMetadataKeyName +
    std::string {0, 0, 0, 21} + "complex128-serialized";
#endif
// clang-format on

static constexpr int64_t kDefaultFlags = ARROW_FLAG_NULLABLE;

////////////////////////////////////////////////////////////////////////////
// Schema export tests

struct SchemaExportChecker {
  SchemaExportChecker(std::vector<std::string> flattened_formats,
                      std::vector<std::string> flattened_names,
                      std::vector<int64_t> flattened_flags = {},
                      std::vector<std::string> flattened_metadata = {})
      : flattened_formats_(std::move(flattened_formats)),
        flattened_names_(std::move(flattened_names)),
        flattened_flags_(
            flattened_flags.empty()
                ? std::vector<int64_t>(flattened_formats_.size(), kDefaultFlags)
                : std::move(flattened_flags)),
        flattened_metadata_(std::move(flattened_metadata)),
        flattened_index_(0) {}

  void operator()(struct ArrowSchema* c_export, bool inner = false) {
    ASSERT_LT(flattened_index_, flattened_formats_.size());
    ASSERT_LT(flattened_index_, flattened_names_.size());
    ASSERT_LT(flattened_index_, flattened_flags_.size());
    ASSERT_EQ(std::string(c_export->format), flattened_formats_[flattened_index_]);
    ASSERT_EQ(std::string(c_export->name), flattened_names_[flattened_index_]);
    std::string expected_md;
    if (!flattened_metadata_.empty()) {
      expected_md = flattened_metadata_[flattened_index_];
    }
    if (!expected_md.empty()) {
      ASSERT_NE(c_export->metadata, nullptr);
      ASSERT_EQ(std::string(c_export->metadata, expected_md.size()), expected_md);
    } else {
      ASSERT_EQ(c_export->metadata, nullptr);
    }
    ASSERT_EQ(c_export->flags, flattened_flags_[flattened_index_]);
    ++flattened_index_;

    if (c_export->dictionary != nullptr) {
      // Recurse into dictionary
      operator()(c_export->dictionary, true);
    }

    if (c_export->n_children > 0) {
      ASSERT_NE(c_export->children, nullptr);
      // Recurse into children
      for (int64_t i = 0; i < c_export->n_children; ++i) {
        ASSERT_NE(c_export->children[i], nullptr);
        operator()(c_export->children[i], true);
      }
    } else {
      ASSERT_EQ(c_export->children, nullptr);
    }

    if (!inner) {
      // Caller gave the right number of names and format strings
      ASSERT_EQ(flattened_index_, flattened_formats_.size());
      ASSERT_EQ(flattened_index_, flattened_names_.size());
      ASSERT_EQ(flattened_index_, flattened_flags_.size());
    }
  }

  const std::vector<std::string> flattened_formats_;
  const std::vector<std::string> flattened_names_;
  std::vector<int64_t> flattened_flags_;
  const std::vector<std::string> flattened_metadata_;
  size_t flattened_index_;
};

class TestSchemaExport : public ::testing::Test {
 public:
  void SetUp() override { pool_ = default_memory_pool(); }

  template <typename T>
  void TestNested(const std::shared_ptr<T>& schema_like,
                  std::vector<std::string> flattened_formats,
                  std::vector<std::string> flattened_names,
                  std::vector<int64_t> flattened_flags = {},
                  std::vector<std::string> flattened_metadata = {}) {
    SchemaExportChecker checker(std::move(flattened_formats), std::move(flattened_names),
                                std::move(flattened_flags),
                                std::move(flattened_metadata));

    auto orig_bytes = pool_->bytes_allocated();

    struct ArrowSchema c_export;
    ASSERT_OK(ExportTraits<T>::ExportFunc(*schema_like, &c_export));

    SchemaExportGuard guard(&c_export);
    auto new_bytes = pool_->bytes_allocated();
    ASSERT_GT(new_bytes, orig_bytes);

    checker(&c_export);

    // Release the ArrowSchema, underlying data should be destroyed
    guard.Release();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename T>
  void TestPrimitive(const std::shared_ptr<T>& schema_like, const char* format,
                     const std::string& name = "", int64_t flags = kDefaultFlags,
                     const std::string& metadata = "") {
    TestNested(schema_like, {format}, {name}, {flags}, {metadata});
  }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestSchemaExport, Primitive) {
  TestPrimitive(int8(), "c");
  TestPrimitive(int16(), "s");
  TestPrimitive(int32(), "i");
  TestPrimitive(int64(), "l");
  TestPrimitive(uint8(), "C");
  TestPrimitive(uint16(), "S");
  TestPrimitive(uint32(), "I");
  TestPrimitive(uint64(), "L");

  TestPrimitive(boolean(), "b");
  TestPrimitive(null(), "n");

  TestPrimitive(float16(), "e");
  TestPrimitive(float32(), "f");
  TestPrimitive(float64(), "g");

  TestPrimitive(fixed_size_binary(3), "w:3");
  TestPrimitive(binary(), "z");
  TestPrimitive(large_binary(), "Z");
  TestPrimitive(utf8(), "u");
  TestPrimitive(large_utf8(), "U");

  TestPrimitive(decimal(16, 4), "d:16,4");
  TestPrimitive(decimal256(16, 4), "d:16,4,256");

  TestPrimitive(decimal(15, 0), "d:15,0");
  TestPrimitive(decimal256(15, 0), "d:15,0,256");

  TestPrimitive(decimal(15, -4), "d:15,-4");
  TestPrimitive(decimal256(15, -4), "d:15,-4,256");
}

TEST_F(TestSchemaExport, Temporal) {
  TestPrimitive(date32(), "tdD");
  TestPrimitive(date64(), "tdm");
  TestPrimitive(time32(TimeUnit::SECOND), "tts");
  TestPrimitive(time32(TimeUnit::MILLI), "ttm");
  TestPrimitive(time64(TimeUnit::MICRO), "ttu");
  TestPrimitive(time64(TimeUnit::NANO), "ttn");
  TestPrimitive(duration(TimeUnit::SECOND), "tDs");
  TestPrimitive(duration(TimeUnit::MILLI), "tDm");
  TestPrimitive(duration(TimeUnit::MICRO), "tDu");
  TestPrimitive(duration(TimeUnit::NANO), "tDn");
  TestPrimitive(month_interval(), "tiM");
  TestPrimitive(month_day_nano_interval(), "tin");
  TestPrimitive(day_time_interval(), "tiD");

  TestPrimitive(timestamp(TimeUnit::SECOND), "tss:");
  TestPrimitive(timestamp(TimeUnit::SECOND, "Europe/Paris"), "tss:Europe/Paris");
  TestPrimitive(timestamp(TimeUnit::MILLI), "tsm:");
  TestPrimitive(timestamp(TimeUnit::MILLI, "Europe/Paris"), "tsm:Europe/Paris");
  TestPrimitive(timestamp(TimeUnit::MICRO), "tsu:");
  TestPrimitive(timestamp(TimeUnit::MICRO, "Europe/Paris"), "tsu:Europe/Paris");
  TestPrimitive(timestamp(TimeUnit::NANO), "tsn:");
  TestPrimitive(timestamp(TimeUnit::NANO, "Europe/Paris"), "tsn:Europe/Paris");
}

TEST_F(TestSchemaExport, List) {
  TestNested(list(int8()), {"+l", "c"}, {"", "item"});
  TestNested(large_list(uint16()), {"+L", "S"}, {"", "item"});
  TestNested(fixed_size_list(int64(), 2), {"+w:2", "l"}, {"", "item"});

  TestNested(list(large_list(int32())), {"+l", "+L", "i"}, {"", "item", "item"});
}

TEST_F(TestSchemaExport, Struct) {
  auto type = struct_({field("a", int8()), field("b", utf8())});
  TestNested(type, {"+s", "c", "u"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE});

  // With nullable = false
  type = struct_({field("a", int8(), /*nullable=*/false), field("b", utf8())});
  TestNested(type, {"+s", "c", "u"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, 0, ARROW_FLAG_NULLABLE});

  // With metadata
  auto f0 = type->field(0);
  auto f1 =
      type->field(1)->WithMetadata(key_value_metadata(kMetadataKeys1, kMetadataValues1));
  type = struct_({f0, f1});
  TestNested(type, {"+s", "c", "u"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, 0, ARROW_FLAG_NULLABLE}, {"", "", kEncodedMetadata1});
}

TEST_F(TestSchemaExport, Map) {
  TestNested(map(int8(), utf8()), {"+m", "+s", "c", "u"}, {"", "entries", "key", "value"},
             {ARROW_FLAG_NULLABLE, 0, 0, ARROW_FLAG_NULLABLE});
  TestNested(
      map(int8(), utf8(), /*keys_sorted=*/true), {"+m", "+s", "c", "u"},
      {"", "entries", "key", "value"},
      {ARROW_FLAG_NULLABLE | ARROW_FLAG_MAP_KEYS_SORTED, 0, 0, ARROW_FLAG_NULLABLE});

  // Exports field names and nullability
  TestNested(map(int8(), field("something", utf8(), false)), {"+m", "+s", "c", "u"},
             {"", "entries", "key", "something"}, {ARROW_FLAG_NULLABLE, 0, 0, 0});
}

TEST_F(TestSchemaExport, Union) {
  // Dense
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean(), /*nullable=*/false);
  auto type = dense_union({field_a, field_b}, {42, 43});
  TestNested(type, {"+ud:42,43", "c", "b"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE, 0});
  TestNested(dense_union(arrow::FieldVector{}, std::vector<int8_t>{}), {"+ud:"}, {""},
             {ARROW_FLAG_NULLABLE});
  // Sparse
  field_a = field("a", int8(), /*nullable=*/false);
  field_b = field("b", boolean());
  type = sparse_union({field_a, field_b}, {42, 43});
  TestNested(type, {"+us:42,43", "c", "b"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, 0, ARROW_FLAG_NULLABLE});
  TestNested(sparse_union(arrow::FieldVector{}, std::vector<int8_t>{}), {"+us:"}, {""},
             {ARROW_FLAG_NULLABLE});
}

std::string GetIndexFormat(Type::type type_id) {
  switch (type_id) {
    case Type::UINT8:
      return "C";
    case Type::INT8:
      return "c";
    case Type::UINT16:
      return "S";
    case Type::INT16:
      return "s";
    case Type::UINT32:
      return "I";
    case Type::INT32:
      return "i";
    case Type::UINT64:
      return "L";
    case Type::INT64:
      return "l";
    default:
      DCHECK(false);
      return "";
  }
}

TEST_F(TestSchemaExport, Dictionary) {
  for (auto index_ty : all_dictionary_index_types()) {
    std::string index_fmt = GetIndexFormat(index_ty->id());
    TestNested(dictionary(index_ty, utf8()), {index_fmt, "u"}, {"", ""});
    TestNested(dictionary(index_ty, list(utf8()), /*ordered=*/true),
               {index_fmt, "+l", "u"}, {"", "", "item"},
               {ARROW_FLAG_NULLABLE | ARROW_FLAG_DICTIONARY_ORDERED, ARROW_FLAG_NULLABLE,
                ARROW_FLAG_NULLABLE});
    TestNested(large_list(dictionary(index_ty, list(utf8()))),
               {"+L", index_fmt, "+l", "u"}, {"", "item", "", "item"});
  }
}

TEST_F(TestSchemaExport, Extension) {
  TestPrimitive(uuid(), "w:16", "", kDefaultFlags, kEncodedUuidMetadata);

  TestNested(dict_extension_type(), {"c", "u"}, {"", ""}, {kDefaultFlags, kDefaultFlags},
             {kEncodedDictExtensionMetadata, ""});

  TestNested(complex128(), {"+s", "g", "g"}, {"", "real", "imag"},
             {ARROW_FLAG_NULLABLE, 0, 0}, {kEncodedComplex128Metadata, "", ""});
}

TEST_F(TestSchemaExport, ExportField) {
  TestPrimitive(field("thing", null()), "n", "thing", ARROW_FLAG_NULLABLE);
  // With nullable = false
  TestPrimitive(field("thing", null(), /*nullable=*/false), "n", "thing", 0);
  // With metadata
  auto f = field("thing", null(), /*nullable=*/false);
  f = f->WithMetadata(key_value_metadata(kMetadataKeys1, kMetadataValues1));
  TestPrimitive(f, "n", "thing", 0, kEncodedMetadata1);
}

TEST_F(TestSchemaExport, ExportSchema) {
  // A schema is exported as an equivalent struct type (+ top-level metadata)
  auto f1 = field("nulls", null(), /*nullable=*/false);
  auto f2 = field("lists", list(int64()));
  auto schema = ::arrow::schema({f1, f2});
  TestNested(schema, {"+s", "n", "+l", "l"}, {"", "nulls", "lists", "item"},
             {0, 0, ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE});

  // With field metadata
  f2 = f2->WithMetadata(key_value_metadata(kMetadataKeys1, kMetadataValues1));
  schema = ::arrow::schema({f1, f2});
  TestNested(schema, {"+s", "n", "+l", "l"}, {"", "nulls", "lists", "item"},
             {0, 0, ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE},
             {"", "", kEncodedMetadata1, ""});

  // With field metadata and schema metadata
  schema = schema->WithMetadata(key_value_metadata(kMetadataKeys2, kMetadataValues2));
  TestNested(schema, {"+s", "n", "+l", "l"}, {"", "nulls", "lists", "item"},
             {0, 0, ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE},
             {kEncodedMetadata2, "", kEncodedMetadata1, ""});
}

////////////////////////////////////////////////////////////////////////////
// Array export tests

struct ArrayExportChecker {
  void operator()(struct ArrowArray* c_export, const ArrayData& expected_data) {
    ASSERT_EQ(c_export->length, expected_data.length);
    ASSERT_EQ(c_export->null_count, expected_data.null_count);
    ASSERT_EQ(c_export->offset, expected_data.offset);

    auto expected_n_buffers = static_cast<int64_t>(expected_data.buffers.size());
    auto expected_buffers = expected_data.buffers.data();
    if (!internal::HasValidityBitmap(expected_data.type->id())) {
      --expected_n_buffers;
      ++expected_buffers;
    }
    ASSERT_EQ(c_export->n_buffers, expected_n_buffers);
    ASSERT_NE(c_export->buffers, nullptr);
    for (int64_t i = 0; i < c_export->n_buffers; ++i) {
      auto expected_ptr = expected_buffers[i] ? expected_buffers[i]->data() : nullptr;
      ASSERT_EQ(c_export->buffers[i], expected_ptr);
    }

    if (expected_data.dictionary != nullptr) {
      // Recurse into dictionary
      ASSERT_NE(c_export->dictionary, nullptr);
      operator()(c_export->dictionary, *expected_data.dictionary);
    } else {
      ASSERT_EQ(c_export->dictionary, nullptr);
    }

    ASSERT_EQ(c_export->n_children,
              static_cast<int64_t>(expected_data.child_data.size()));
    if (c_export->n_children > 0) {
      ASSERT_NE(c_export->children, nullptr);
      // Recurse into children
      for (int64_t i = 0; i < c_export->n_children; ++i) {
        ASSERT_NE(c_export->children[i], nullptr);
        operator()(c_export->children[i], *expected_data.child_data[i]);
      }
    } else {
      ASSERT_EQ(c_export->children, nullptr);
    }
  }
};

struct RecordBatchExportChecker {
  void operator()(struct ArrowArray* c_export, const RecordBatch& expected_batch) {
    ASSERT_EQ(c_export->length, expected_batch.num_rows());
    ASSERT_EQ(c_export->null_count, 0);
    ASSERT_EQ(c_export->offset, 0);

    ASSERT_EQ(c_export->n_buffers, 1);  // Like a struct array
    ASSERT_NE(c_export->buffers, nullptr);
    ASSERT_EQ(c_export->buffers[0], nullptr);  // No null bitmap
    ASSERT_EQ(c_export->dictionary, nullptr);

    ASSERT_EQ(c_export->n_children, expected_batch.num_columns());
    if (c_export->n_children > 0) {
      ArrayExportChecker array_checker{};

      ASSERT_NE(c_export->children, nullptr);
      // Recurse into children
      for (int i = 0; i < expected_batch.num_columns(); ++i) {
        ASSERT_NE(c_export->children[i], nullptr);
        array_checker(c_export->children[i], *expected_batch.column(i)->data());
      }
    } else {
      ASSERT_EQ(c_export->children, nullptr);
    }
  }
};

class TestArrayExport : public ::testing::Test {
 public:
  void SetUp() override { pool_ = default_memory_pool(); }

  static std::function<Result<std::shared_ptr<Array>>()> JSONArrayFactory(
      std::shared_ptr<DataType> type, const char* json) {
    return [=]() { return ArrayFromJSON(type, json); };
  }

  template <typename ArrayFactory, typename ExportCheckFunc>
  void TestWithArrayFactory(ArrayFactory&& factory, ExportCheckFunc&& check_func) {
    auto orig_bytes = pool_->bytes_allocated();

    std::shared_ptr<Array> arr;
    ASSERT_OK_AND_ASSIGN(arr, ToResult(factory()));
    ARROW_SCOPED_TRACE("type = ", arr->type()->ToString(),
                       ", array data = ", arr->ToString());
    const ArrayData& data = *arr->data();  // non-owning reference
    struct ArrowArray c_export;
    ASSERT_OK(ExportArray(*arr, &c_export));

    ArrayExportGuard guard(&c_export);
    auto new_bytes = pool_->bytes_allocated();
    ASSERT_GT(new_bytes, orig_bytes);

    // Release the shared_ptr<Array>, underlying data should be held alive
    arr.reset();
    ASSERT_EQ(pool_->bytes_allocated(), new_bytes);
    check_func(&c_export, data);

    // Release the ArrowArray, underlying data should be destroyed
    guard.Release();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename ArrayFactory>
  void TestNested(ArrayFactory&& factory) {
    ArrayExportChecker checker;
    TestWithArrayFactory(std::forward<ArrayFactory>(factory), checker);
  }

  void TestNested(const std::shared_ptr<DataType>& type, const char* json) {
    TestNested(JSONArrayFactory(type, json));
  }

  template <typename ArrayFactory>
  void TestPrimitive(ArrayFactory&& factory) {
    TestNested(std::forward<ArrayFactory>(factory));
  }

  void TestPrimitive(const std::shared_ptr<DataType>& type, const char* json) {
    TestNested(type, json);
  }

  template <typename ArrayFactory, typename ExportCheckFunc>
  void TestMoveWithArrayFactory(ArrayFactory&& factory, ExportCheckFunc&& check_func) {
    auto orig_bytes = pool_->bytes_allocated();

    std::shared_ptr<Array> arr;
    ASSERT_OK_AND_ASSIGN(arr, ToResult(factory()));
    const ArrayData& data = *arr->data();  // non-owning reference
    struct ArrowArray c_export_temp, c_export_final;
    ASSERT_OK(ExportArray(*arr, &c_export_temp));

    // Move the ArrowArray to its final location
    ArrowArrayMove(&c_export_temp, &c_export_final);
    ASSERT_TRUE(ArrowArrayIsReleased(&c_export_temp));

    ArrayExportGuard guard(&c_export_final);
    auto new_bytes = pool_->bytes_allocated();
    ASSERT_GT(new_bytes, orig_bytes);
    check_func(&c_export_final, data);

    // Release the shared_ptr<Array>, underlying data should be held alive
    arr.reset();
    ASSERT_EQ(pool_->bytes_allocated(), new_bytes);
    check_func(&c_export_final, data);

    // Release the ArrowArray, underlying data should be destroyed
    guard.Release();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename ArrayFactory>
  void TestMoveNested(ArrayFactory&& factory) {
    ArrayExportChecker checker;

    TestMoveWithArrayFactory(std::forward<ArrayFactory>(factory), checker);
  }

  void TestMoveNested(const std::shared_ptr<DataType>& type, const char* json) {
    TestMoveNested(JSONArrayFactory(type, json));
  }

  void TestMovePrimitive(const std::shared_ptr<DataType>& type, const char* json) {
    TestMoveNested(type, json);
  }

  template <typename ArrayFactory, typename ExportCheckFunc>
  void TestMoveChildWithArrayFactory(ArrayFactory&& factory, int64_t child_id,
                                     ExportCheckFunc&& check_func) {
    auto orig_bytes = pool_->bytes_allocated();

    std::shared_ptr<Array> arr;
    ASSERT_OK_AND_ASSIGN(arr, ToResult(factory()));
    struct ArrowArray c_export_parent, c_export_child;
    ASSERT_OK(ExportArray(*arr, &c_export_parent));

    auto bytes_with_parent = pool_->bytes_allocated();
    ASSERT_GT(bytes_with_parent, orig_bytes);

    // Move the child ArrowArray to its final location
    {
      ArrayExportGuard parent_guard(&c_export_parent);
      ASSERT_LT(child_id, c_export_parent.n_children);
      ArrowArrayMove(c_export_parent.children[child_id], &c_export_child);
    }
    ArrayExportGuard child_guard(&c_export_child);

    // Now parent is released
    ASSERT_TRUE(ArrowArrayIsReleased(&c_export_parent));
    auto bytes_with_child = pool_->bytes_allocated();
    ASSERT_LT(bytes_with_child, bytes_with_parent);
    ASSERT_GT(bytes_with_child, orig_bytes);

    const ArrayData& data = *arr->data()->child_data[child_id];  // non-owning reference
    check_func(&c_export_child, data);

    // Release the shared_ptr<Array>, some underlying data should be held alive
    arr.reset();
    ASSERT_LT(pool_->bytes_allocated(), bytes_with_child);
    ASSERT_GT(pool_->bytes_allocated(), orig_bytes);
    check_func(&c_export_child, data);

    // Release the ArrowArray, underlying data should be destroyed
    child_guard.Release();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename ArrayFactory>
  void TestMoveChild(ArrayFactory&& factory, int64_t child_id) {
    ArrayExportChecker checker;

    TestMoveChildWithArrayFactory(std::forward<ArrayFactory>(factory), child_id, checker);
  }

  void TestMoveChild(const std::shared_ptr<DataType>& type, const char* json,
                     int64_t child_id) {
    TestMoveChild(JSONArrayFactory(type, json), child_id);
  }

  template <typename ArrayFactory, typename ExportCheckFunc>
  void TestMoveChildrenWithArrayFactory(ArrayFactory&& factory,
                                        const std::vector<int64_t> children_ids,
                                        ExportCheckFunc&& check_func) {
    auto orig_bytes = pool_->bytes_allocated();

    std::shared_ptr<Array> arr;
    ASSERT_OK_AND_ASSIGN(arr, ToResult(factory()));
    struct ArrowArray c_export_parent;
    ASSERT_OK(ExportArray(*arr, &c_export_parent));

    auto bytes_with_parent = pool_->bytes_allocated();
    ASSERT_GT(bytes_with_parent, orig_bytes);

    // Move the children ArrowArrays to their final locations
    std::vector<struct ArrowArray> c_export_children(children_ids.size());
    std::vector<ArrayExportGuard> child_guards;
    std::vector<const ArrayData*> child_data;
    {
      ArrayExportGuard parent_guard(&c_export_parent);
      for (size_t i = 0; i < children_ids.size(); ++i) {
        const auto child_id = children_ids[i];
        ASSERT_LT(child_id, c_export_parent.n_children);
        ArrowArrayMove(c_export_parent.children[child_id], &c_export_children[i]);
        child_guards.emplace_back(&c_export_children[i]);
        // Keep non-owning pointer to the child ArrayData
        child_data.push_back(arr->data()->child_data[child_id].get());
      }
    }

    // Now parent is released
    ASSERT_TRUE(ArrowArrayIsReleased(&c_export_parent));
    auto bytes_with_child = pool_->bytes_allocated();
    ASSERT_LT(bytes_with_child, bytes_with_parent);
    ASSERT_GT(bytes_with_child, orig_bytes);
    for (size_t i = 0; i < children_ids.size(); ++i) {
      check_func(&c_export_children[i], *child_data[i]);
    }

    // Release the shared_ptr<Array>, the children data should be held alive
    arr.reset();
    ASSERT_LT(pool_->bytes_allocated(), bytes_with_child);
    ASSERT_GT(pool_->bytes_allocated(), orig_bytes);
    for (size_t i = 0; i < children_ids.size(); ++i) {
      check_func(&c_export_children[i], *child_data[i]);
    }

    // Release the ArrowArrays, underlying data should be destroyed
    for (auto& child_guard : child_guards) {
      child_guard.Release();
    }
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename ArrayFactory>
  void TestMoveChildren(ArrayFactory&& factory, const std::vector<int64_t> children_ids) {
    ArrayExportChecker checker;

    TestMoveChildrenWithArrayFactory(std::forward<ArrayFactory>(factory), children_ids,
                                     checker);
  }

  void TestMoveChildren(const std::shared_ptr<DataType>& type, const char* json,
                        const std::vector<int64_t> children_ids) {
    TestMoveChildren(JSONArrayFactory(type, json), children_ids);
  }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestArrayExport, Primitive) {
  TestPrimitive(int8(), "[1, 2, null, -3]");
  TestPrimitive(int16(), "[1, 2, -3]");
  TestPrimitive(int32(), "[1, 2, null, -3]");
  TestPrimitive(int64(), "[1, 2, -3]");
  TestPrimitive(uint8(), "[1, 2, 3]");
  TestPrimitive(uint16(), "[1, 2, null, 3]");
  TestPrimitive(uint32(), "[1, 2, 3]");
  TestPrimitive(uint64(), "[1, 2, null, 3]");

  TestPrimitive(boolean(), "[true, false, null]");
  TestPrimitive(null(), "[null, null]");

  TestPrimitive(float32(), "[1.5, null]");
  TestPrimitive(float64(), "[1.5, null]");

  TestPrimitive(fixed_size_binary(3), R"(["foo", "bar", null])");
  TestPrimitive(binary(), R"(["foo", "bar", null])");
  TestPrimitive(large_binary(), R"(["foo", "bar", null])");
  TestPrimitive(utf8(), R"(["foo", "bar", null])");
  TestPrimitive(large_utf8(), R"(["foo", "bar", null])");

  TestPrimitive(decimal(16, 4), R"(["1234.5670", null])");
  TestPrimitive(decimal256(16, 4), R"(["1234.5670", null])");

  TestPrimitive(month_day_nano_interval(), R"([[-1, 5, 20], null])");
}

TEST_F(TestArrayExport, PrimitiveSliced) {
  auto factory = []() { return ArrayFromJSON(int16(), "[1, 2, null, -3]")->Slice(1, 2); };

  TestPrimitive(factory);
}

TEST_F(TestArrayExport, Null) {
  TestPrimitive(null(), "[null, null, null]");
  TestPrimitive(null(), "[]");
}

TEST_F(TestArrayExport, Temporal) {
  const char* json = "[1, 2, null, 42]";
  TestPrimitive(date32(), json);
  TestPrimitive(date64(), json);
  TestPrimitive(time32(TimeUnit::SECOND), json);
  TestPrimitive(time32(TimeUnit::MILLI), json);
  TestPrimitive(time64(TimeUnit::MICRO), json);
  TestPrimitive(time64(TimeUnit::NANO), json);
  TestPrimitive(duration(TimeUnit::SECOND), json);
  TestPrimitive(duration(TimeUnit::MILLI), json);
  TestPrimitive(duration(TimeUnit::MICRO), json);
  TestPrimitive(duration(TimeUnit::NANO), json);
  TestPrimitive(month_interval(), json);

  TestPrimitive(day_time_interval(), "[[7, 600], null]");

  json = R"(["1970-01-01","2000-02-29","1900-02-28"])";
  TestPrimitive(timestamp(TimeUnit::SECOND), json);
  TestPrimitive(timestamp(TimeUnit::SECOND, "Europe/Paris"), json);
  TestPrimitive(timestamp(TimeUnit::MILLI), json);
  TestPrimitive(timestamp(TimeUnit::MILLI, "Europe/Paris"), json);
  TestPrimitive(timestamp(TimeUnit::MICRO), json);
  TestPrimitive(timestamp(TimeUnit::MICRO, "Europe/Paris"), json);
  TestPrimitive(timestamp(TimeUnit::NANO), json);
  TestPrimitive(timestamp(TimeUnit::NANO, "Europe/Paris"), json);
}

TEST_F(TestArrayExport, List) {
  TestNested(list(int8()), "[[1, 2], [3, null], null]");
  TestNested(large_list(uint16()), "[[1, 2], [3, null], null]");
  TestNested(fixed_size_list(int64(), 2), "[[1, 2], [3, null], null]");

  TestNested(list(large_list(int32())), "[[[1, 2], [3], null], null]");
}

TEST_F(TestArrayExport, ListSliced) {
  {
    auto factory = []() {
      return ArrayFromJSON(list(int8()), "[[1, 2], [3, null], [4, 5, 6], null]")
          ->Slice(1, 2);
    };
    TestNested(factory);
  }
  {
    auto factory = []() {
      auto values = ArrayFromJSON(int16(), "[1, 2, 3, 4, null, 5, 6, 7, 8]")->Slice(1, 6);
      auto offsets = ArrayFromJSON(int32(), "[0, 2, 3, 5, 6]")->Slice(2, 4);
      return ListArray::FromArrays(*offsets, *values);
    };
    TestNested(factory);
  }
}

TEST_F(TestArrayExport, Struct) {
  const char* data = R"([[1, "foo"], [2, null]])";
  auto type = struct_({field("a", int8()), field("b", utf8())});
  TestNested(type, data);
}

TEST_F(TestArrayExport, Map) {
  const char* json = R"([[[1, "foo"], [2, null]], [[3, "bar"]]])";
  TestNested(map(int8(), utf8()), json);
  TestNested(map(int8(), utf8(), /*keys_sorted=*/true), json);
}

TEST_F(TestArrayExport, Union) {
  const char* data = "[null, [42, 1], [43, true], [42, null], [42, 2]]";
  // Dense
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean(), /*nullable=*/false);
  auto type = dense_union({field_a, field_b}, {42, 43});
  TestNested(type, data);
  // Sparse
  field_a = field("a", int8(), /*nullable=*/false);
  field_b = field("b", boolean());
  type = sparse_union({field_a, field_b}, {42, 43});
  TestNested(type, data);
}

TEST_F(TestArrayExport, Dictionary) {
  {
    auto factory = []() {
      auto values = ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])");
      auto indices = ArrayFromJSON(uint16(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                         indices, values);
    };
    TestNested(factory);
  }
  {
    auto factory = []() {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(
          dictionary(indices->type(), values->type(), /*ordered=*/true), indices, values);
    };
    TestNested(factory);
  }
  {
    auto factory = []() -> Result<std::shared_ptr<Array>> {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      ARROW_ASSIGN_OR_RAISE(
          auto dict_array,
          DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                      indices, values));
      auto offsets = ArrayFromJSON(int64(), "[0, 2, 5]");
      ARROW_ASSIGN_OR_RAISE(auto arr, LargeListArray::FromArrays(*offsets, *dict_array));
      RETURN_NOT_OK(arr->ValidateFull());
      return arr;
    };
    TestNested(factory);
  }
}

TEST_F(TestArrayExport, Extension) {
  TestPrimitive(ExampleUuid);
  TestPrimitive(ExampleSmallint);
  TestPrimitive(ExampleComplex128);
}

TEST_F(TestArrayExport, MovePrimitive) {
  TestMovePrimitive(int8(), "[1, 2, null, -3]");
  TestMovePrimitive(fixed_size_binary(3), R"(["foo", "bar", null])");
  TestMovePrimitive(binary(), R"(["foo", "bar", null])");
}

TEST_F(TestArrayExport, MoveNested) {
  TestMoveNested(list(int8()), "[[1, 2], [3, null], null]");
  TestMoveNested(list(large_list(int32())), "[[[1, 2], [3], null], null]");
  TestMoveNested(struct_({field("a", int8()), field("b", utf8())}),
                 R"([[1, "foo"], [2, null]])");
}

TEST_F(TestArrayExport, MoveDictionary) {
  {
    auto factory = []() {
      auto values = ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                         indices, values);
    };
    TestMoveNested(factory);
  }
  {
    auto factory = []() -> Result<std::shared_ptr<Array>> {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      ARROW_ASSIGN_OR_RAISE(
          auto dict_array,
          DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                      indices, values));
      auto offsets = ArrayFromJSON(int64(), "[0, 2, 5]");
      ARROW_ASSIGN_OR_RAISE(auto arr, LargeListArray::FromArrays(*offsets, *dict_array));
      RETURN_NOT_OK(arr->ValidateFull());
      return arr;
    };
    TestMoveNested(factory);
  }
}

TEST_F(TestArrayExport, MoveChild) {
  TestMoveChild(list(int8()), "[[1, 2], [3, null], null]", /*child_id=*/0);
  TestMoveChild(list(large_list(int32())), "[[[1, 2], [3], null], null]",
                /*child_id=*/0);
  TestMoveChild(struct_({field("ints", int8()), field("strs", utf8())}),
                R"([[1, "foo"], [2, null]])",
                /*child_id=*/0);
  TestMoveChild(struct_({field("ints", int8()), field("strs", utf8())}),
                R"([[1, "foo"], [2, null]])",
                /*child_id=*/1);
  {
    auto factory = []() -> Result<std::shared_ptr<Array>> {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      ARROW_ASSIGN_OR_RAISE(
          auto dict_array,
          DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                      indices, values));
      auto offsets = ArrayFromJSON(int64(), "[0, 2, 5]");
      ARROW_ASSIGN_OR_RAISE(auto arr, LargeListArray::FromArrays(*offsets, *dict_array));
      RETURN_NOT_OK(arr->ValidateFull());
      return arr;
    };
    TestMoveChild(factory, /*child_id=*/0);
  }
}

TEST_F(TestArrayExport, MoveSeveralChildren) {
  TestMoveChildren(
      struct_({field("ints", int8()), field("floats", float64()), field("strs", utf8())}),
      R"([[1, 1.5, "foo"], [2, 0.0, null]])", /*children_ids=*/{0, 2});
}

TEST_F(TestArrayExport, ExportArrayAndType) {
  struct ArrowSchema c_schema {};
  struct ArrowArray c_array {};
  SchemaExportGuard schema_guard(&c_schema);
  ArrayExportGuard array_guard(&c_array);

  auto array = ArrayFromJSON(int8(), "[1, 2, 3]");
  ASSERT_OK(ExportArray(*array, &c_array, &c_schema));
  const ArrayData& data = *array->data();
  array.reset();
  ASSERT_FALSE(ArrowSchemaIsReleased(&c_schema));
  ASSERT_FALSE(ArrowArrayIsReleased(&c_array));
  ASSERT_EQ(c_schema.format, std::string("c"));
  ASSERT_EQ(c_schema.n_children, 0);
  ArrayExportChecker checker{};
  checker(&c_array, data);
}

TEST_F(TestArrayExport, ExportRecordBatch) {
  struct ArrowSchema c_schema {};
  struct ArrowArray c_array {};

  auto schema = ::arrow::schema(
      {field("ints", int16()), field("bools", boolean(), /*nullable=*/false)});
  schema = schema->WithMetadata(key_value_metadata(kMetadataKeys2, kMetadataValues2));
  auto arr0 = ArrayFromJSON(int16(), "[1, 2, null]");
  auto arr1 = ArrayFromJSON(boolean(), "[false, true, false]");

  auto batch_factory = [&]() { return RecordBatch::Make(schema, 3, {arr0, arr1}); };

  {
    auto batch = batch_factory();

    ASSERT_OK(ExportRecordBatch(*batch, &c_array));
    ArrayExportGuard array_guard(&c_array);
    RecordBatchExportChecker checker{};
    checker(&c_array, *batch);

    // Create batch anew, with the same buffer pointers
    batch = batch_factory();
    checker(&c_array, *batch);
  }
  {
    // Check one can export both schema and record batch at once
    auto batch = batch_factory();

    ASSERT_OK(ExportRecordBatch(*batch, &c_array, &c_schema));
    SchemaExportGuard schema_guard(&c_schema);
    ArrayExportGuard array_guard(&c_array);
    ASSERT_EQ(c_schema.format, std::string("+s"));
    ASSERT_EQ(c_schema.n_children, 2);
    ASSERT_NE(c_schema.metadata, nullptr);
    ASSERT_EQ(kEncodedMetadata2,
              std::string(c_schema.metadata, kEncodedMetadata2.size()));
    RecordBatchExportChecker checker{};
    checker(&c_array, *batch);

    // Create batch anew, with the same buffer pointers
    batch = batch_factory();
    checker(&c_array, *batch);
  }
}

////////////////////////////////////////////////////////////////////////////
// Schema import tests

void NoOpSchemaRelease(struct ArrowSchema* schema) { ArrowSchemaMarkReleased(schema); }

class SchemaStructBuilder {
 public:
  SchemaStructBuilder() { Reset(); }

  void Reset() {
    memset(&c_struct_, 0, sizeof(c_struct_));
    c_struct_.release = NoOpSchemaRelease;
    nested_structs_.clear();
    children_arrays_.clear();
  }

  // Create a new ArrowSchema struct with a stable C pointer
  struct ArrowSchema* AddChild() {
    nested_structs_.emplace_back();
    struct ArrowSchema* result = &nested_structs_.back();
    memset(result, 0, sizeof(*result));
    result->release = NoOpSchemaRelease;
    return result;
  }

  // Create a stable C pointer to the N last structs in nested_structs_
  struct ArrowSchema** NLastChildren(int64_t n_children, struct ArrowSchema* parent) {
    children_arrays_.emplace_back(n_children);
    struct ArrowSchema** children = children_arrays_.back().data();
    int64_t nested_offset;
    // If parent is itself at the end of nested_structs_, skip it
    if (parent != nullptr && &nested_structs_.back() == parent) {
      nested_offset = static_cast<int64_t>(nested_structs_.size()) - n_children - 1;
    } else {
      nested_offset = static_cast<int64_t>(nested_structs_.size()) - n_children;
    }
    for (int64_t i = 0; i < n_children; ++i) {
      children[i] = &nested_structs_[nested_offset + i];
    }
    return children;
  }

  struct ArrowSchema* LastChild(struct ArrowSchema* parent = nullptr) {
    return *NLastChildren(1, parent);
  }

  void FillPrimitive(struct ArrowSchema* c, const char* format,
                     const char* name = nullptr, int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->name = name;
  }

  void FillDictionary(struct ArrowSchema* c) { c->dictionary = LastChild(c); }

  void FillListLike(struct ArrowSchema* c, const char* format, const char* name = nullptr,
                    int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->name = name;
    c->n_children = 1;
    c->children = NLastChildren(1, c);
    c->children[0]->name = "item";
  }

  void FillStructLike(struct ArrowSchema* c, const char* format, int64_t n_children,
                      const char* name = nullptr, int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->name = name;
    c->n_children = n_children;
    c->children = NLastChildren(c->n_children, c);
  }

  void FillPrimitive(const char* format, const char* name = nullptr,
                     int64_t flags = kDefaultFlags) {
    FillPrimitive(&c_struct_, format, name, flags);
  }

  void FillDictionary() { FillDictionary(&c_struct_); }

  void FillListLike(const char* format, const char* name = nullptr,
                    int64_t flags = kDefaultFlags) {
    FillListLike(&c_struct_, format, name, flags);
  }

  void FillStructLike(const char* format, int64_t n_children, const char* name = nullptr,
                      int64_t flags = kDefaultFlags) {
    FillStructLike(&c_struct_, format, n_children, name, flags);
  }

  struct ArrowSchema c_struct_;
  // Deque elements don't move when the deque is appended to, which allows taking
  // stable C pointers to them.
  std::deque<struct ArrowSchema> nested_structs_;
  std::deque<std::vector<struct ArrowSchema*>> children_arrays_;
};

class TestSchemaImport : public ::testing::Test, public SchemaStructBuilder {
 public:
  void SetUp() override { Reset(); }

  void CheckImport(const std::shared_ptr<DataType>& expected) {
    SchemaReleaseCallback cb(&c_struct_);

    ASSERT_OK_AND_ASSIGN(auto type, ImportType(&c_struct_));
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_struct_));
    Reset();            // for further tests
    cb.AssertCalled();  // was released
    AssertTypeEqual(*expected, *type);
  }

  void CheckImport(const std::shared_ptr<Field>& expected) {
    SchemaReleaseCallback cb(&c_struct_);

    ASSERT_OK_AND_ASSIGN(auto field, ImportField(&c_struct_));
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_struct_));
    Reset();            // for further tests
    cb.AssertCalled();  // was released
    AssertFieldEqual(*expected, *field);
  }

  void CheckImport(const std::shared_ptr<Schema>& expected) {
    SchemaReleaseCallback cb(&c_struct_);

    ASSERT_OK_AND_ASSIGN(auto schema, ImportSchema(&c_struct_));
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_struct_));
    Reset();            // for further tests
    cb.AssertCalled();  // was released
    AssertSchemaEqual(*expected, *schema);
  }

  void CheckImportError() {
    SchemaReleaseCallback cb(&c_struct_);

    ASSERT_RAISES(Invalid, ImportField(&c_struct_));
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_struct_));
    cb.AssertCalled();  // was released
  }

  void CheckSchemaImportError() {
    SchemaReleaseCallback cb(&c_struct_);

    ASSERT_RAISES(Invalid, ImportSchema(&c_struct_));
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_struct_));
    cb.AssertCalled();  // was released
  }
};

TEST_F(TestSchemaImport, Primitive) {
  FillPrimitive("c");
  CheckImport(int8());
  FillPrimitive("c");
  CheckImport(field("", int8()));
  FillPrimitive("C");
  CheckImport(field("", uint8()));
  FillPrimitive("s");
  CheckImport(field("", int16()));
  FillPrimitive("S");
  CheckImport(field("", uint16()));
  FillPrimitive("i");
  CheckImport(field("", int32()));
  FillPrimitive("I");
  CheckImport(field("", uint32()));
  FillPrimitive("l");
  CheckImport(field("", int64()));
  FillPrimitive("L");
  CheckImport(field("", uint64()));

  FillPrimitive("b");
  CheckImport(field("", boolean()));
  FillPrimitive("e");
  CheckImport(field("", float16()));
  FillPrimitive("f");
  CheckImport(field("", float32()));
  FillPrimitive("g");
  CheckImport(field("", float64()));

  FillPrimitive("d:16,4");
  CheckImport(field("", decimal128(16, 4)));
  FillPrimitive("d:16,4,128");
  CheckImport(field("", decimal128(16, 4)));
  FillPrimitive("d:16,4,256");
  CheckImport(field("", decimal256(16, 4)));

  FillPrimitive("d:16,0");
  CheckImport(field("", decimal128(16, 0)));
  FillPrimitive("d:16,0,128");
  CheckImport(field("", decimal128(16, 0)));
  FillPrimitive("d:16,0,256");
  CheckImport(field("", decimal256(16, 0)));

  FillPrimitive("d:16,-4");
  CheckImport(field("", decimal128(16, -4)));
  FillPrimitive("d:16,-4,128");
  CheckImport(field("", decimal128(16, -4)));
  FillPrimitive("d:16,-4,256");
  CheckImport(field("", decimal256(16, -4)));
}

TEST_F(TestSchemaImport, Temporal) {
  FillPrimitive("tdD");
  CheckImport(date32());
  FillPrimitive("tdm");
  CheckImport(date64());

  FillPrimitive("tts");
  CheckImport(time32(TimeUnit::SECOND));
  FillPrimitive("ttm");
  CheckImport(time32(TimeUnit::MILLI));
  FillPrimitive("ttu");
  CheckImport(time64(TimeUnit::MICRO));
  FillPrimitive("ttn");
  CheckImport(time64(TimeUnit::NANO));

  FillPrimitive("tDs");
  CheckImport(duration(TimeUnit::SECOND));
  FillPrimitive("tDm");
  CheckImport(duration(TimeUnit::MILLI));
  FillPrimitive("tDu");
  CheckImport(duration(TimeUnit::MICRO));
  FillPrimitive("tDn");
  CheckImport(duration(TimeUnit::NANO));

  FillPrimitive("tiM");
  CheckImport(month_interval());
  FillPrimitive("tiD");
  CheckImport(day_time_interval());
  FillPrimitive("tin");
  CheckImport(month_day_nano_interval());

  FillPrimitive("tss:");
  CheckImport(timestamp(TimeUnit::SECOND));
  FillPrimitive("tsm:");
  CheckImport(timestamp(TimeUnit::MILLI));
  FillPrimitive("tsu:");
  CheckImport(timestamp(TimeUnit::MICRO));
  FillPrimitive("tsn:");
  CheckImport(timestamp(TimeUnit::NANO));

  FillPrimitive("tss:Europe/Paris");
  CheckImport(timestamp(TimeUnit::SECOND, "Europe/Paris"));
  FillPrimitive("tsm:Europe/Paris");
  CheckImport(timestamp(TimeUnit::MILLI, "Europe/Paris"));
  FillPrimitive("tsu:Europe/Paris");
  CheckImport(timestamp(TimeUnit::MICRO, "Europe/Paris"));
  FillPrimitive("tsn:Europe/Paris");
  CheckImport(timestamp(TimeUnit::NANO, "Europe/Paris"));
}

TEST_F(TestSchemaImport, String) {
  FillPrimitive("u");
  CheckImport(utf8());
  FillPrimitive("z");
  CheckImport(binary());
  FillPrimitive("U");
  CheckImport(large_utf8());
  FillPrimitive("Z");
  CheckImport(large_binary());

  FillPrimitive("w:3");
  CheckImport(fixed_size_binary(3));
  FillPrimitive("d:15,4");
  CheckImport(decimal(15, 4));
}

TEST_F(TestSchemaImport, List) {
  FillPrimitive(AddChild(), "c");
  FillListLike("+l");
  CheckImport(list(int8()));

  FillPrimitive(AddChild(), "s", "item", 0);
  FillListLike("+l");
  CheckImport(list(field("item", int16(), /*nullable=*/false)));

  // Large list
  FillPrimitive(AddChild(), "s");
  FillListLike("+L");
  CheckImport(large_list(int16()));

  // Fixed-size list
  FillPrimitive(AddChild(), "c");
  FillListLike("+w:3");
  CheckImport(fixed_size_list(int8(), 3));
}

TEST_F(TestSchemaImport, NestedList) {
  FillPrimitive(AddChild(), "c");
  FillListLike(AddChild(), "+l");
  FillListLike("+L");
  CheckImport(large_list(list(int8())));

  FillPrimitive(AddChild(), "c");
  FillListLike(AddChild(), "+w:3");
  FillListLike("+l");
  CheckImport(list(fixed_size_list(int8(), 3)));
}

TEST_F(TestSchemaImport, Struct) {
  FillPrimitive(AddChild(), "u", "strs");
  FillPrimitive(AddChild(), "S", "ints");
  FillStructLike("+s", 2);
  auto expected = struct_({field("strs", utf8()), field("ints", uint16())});
  CheckImport(expected);

  FillPrimitive(AddChild(), "u", "strs", 0);
  FillPrimitive(AddChild(), "S", "ints", kDefaultFlags);
  FillStructLike("+s", 2);
  expected =
      struct_({field("strs", utf8(), /*nullable=*/false), field("ints", uint16())});
  CheckImport(expected);

  // With metadata
  auto c = AddChild();
  FillPrimitive(c, "u", "strs", 0);
  c->metadata = kEncodedMetadata2.c_str();
  FillPrimitive(AddChild(), "S", "ints", kDefaultFlags);
  FillStructLike("+s", 2);
  expected = struct_({field("strs", utf8(), /*nullable=*/false,
                            key_value_metadata(kMetadataKeys2, kMetadataValues2)),
                      field("ints", uint16())});
  CheckImport(expected);
}

TEST_F(TestSchemaImport, Union) {
  // Sparse
  FillPrimitive(AddChild(), "u", "strs");
  FillPrimitive(AddChild(), "c", "ints");
  FillStructLike("+us:43,42", 2);
  auto expected = sparse_union({field("strs", utf8()), field("ints", int8())}, {43, 42});
  CheckImport(expected);

  // Dense
  FillPrimitive(AddChild(), "u", "strs");
  FillPrimitive(AddChild(), "c", "ints");
  FillStructLike("+ud:43,42", 2);
  expected = dense_union({field("strs", utf8()), field("ints", int8())}, {43, 42});
  CheckImport(expected);
}

TEST_F(TestSchemaImport, Map) {
  FillPrimitive(AddChild(), "u", "key");
  FillPrimitive(AddChild(), "i", "value");
  FillStructLike(AddChild(), "+s", 2, "entries");
  FillListLike("+m");
  auto expected = map(utf8(), int32());
  CheckImport(expected);

  FillPrimitive(AddChild(), "u", "key");
  FillPrimitive(AddChild(), "i", "value");
  FillStructLike(AddChild(), "+s", 2, "entries");
  FillListLike("+m", "", ARROW_FLAG_MAP_KEYS_SORTED);
  expected = map(utf8(), int32(), /*keys_sorted=*/true);
  CheckImport(expected);
}

TEST_F(TestSchemaImport, Dictionary) {
  FillPrimitive(AddChild(), "u");
  FillPrimitive("c");
  FillDictionary();
  auto expected = dictionary(int8(), utf8());
  CheckImport(expected);

  FillPrimitive(AddChild(), "u");
  FillPrimitive("c", "", ARROW_FLAG_NULLABLE | ARROW_FLAG_DICTIONARY_ORDERED);
  FillDictionary();
  expected = dictionary(int8(), utf8(), /*ordered=*/true);
  CheckImport(expected);

  FillPrimitive(AddChild(), "u");
  FillListLike(AddChild(), "+L");
  FillPrimitive("c");
  FillDictionary();
  expected = dictionary(int8(), large_list(utf8()));
  CheckImport(expected);

  FillPrimitive(AddChild(), "u");
  FillPrimitive(AddChild(), "c");
  FillDictionary(LastChild());
  FillListLike("+l");
  expected = list(dictionary(int8(), utf8()));
  CheckImport(expected);
}

TEST_F(TestSchemaImport, UnregisteredExtension) {
  FillPrimitive("w:16");
  c_struct_.metadata = kEncodedUuidMetadata.c_str();
  auto expected = fixed_size_binary(16);
  CheckImport(expected);
}

TEST_F(TestSchemaImport, RegisteredExtension) {
  {
    ExtensionTypeGuard guard(uuid());
    FillPrimitive("w:16");
    c_struct_.metadata = kEncodedUuidMetadata.c_str();
    auto expected = uuid();
    CheckImport(expected);
  }
  {
    ExtensionTypeGuard guard(dict_extension_type());
    FillPrimitive(AddChild(), "u");
    FillPrimitive("c");
    FillDictionary();
    c_struct_.metadata = kEncodedDictExtensionMetadata.c_str();
    auto expected = dict_extension_type();
    CheckImport(expected);
  }
}

TEST_F(TestSchemaImport, FormatStringError) {
  FillPrimitive("");
  CheckImportError();
  FillPrimitive("cc");
  CheckImportError();
  FillPrimitive("w3");
  CheckImportError();
  FillPrimitive("w:three");
  CheckImportError();
  FillPrimitive("w:3,5");
  CheckImportError();
  FillPrimitive("d:15");
  CheckImportError();
  FillPrimitive("d:15.4");
  CheckImportError();
  FillPrimitive("d:15,z");
  CheckImportError();
  FillPrimitive("t");
  CheckImportError();
  FillPrimitive("td");
  CheckImportError();
  FillPrimitive("tz");
  CheckImportError();
  FillPrimitive("tdd");
  CheckImportError();
  FillPrimitive("tdDd");
  CheckImportError();
  FillPrimitive("tss");
  CheckImportError();
  FillPrimitive("tss;UTC");
  CheckImportError();
  FillPrimitive("+");
  CheckImportError();
  FillPrimitive("+mm");
  CheckImportError();
  FillPrimitive("+u");
  CheckImportError();
}

TEST_F(TestSchemaImport, UnionError) {
  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+uz", 1);
  CheckImportError();

  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+uz:", 1);
  CheckImportError();

  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+uz:1", 1);
  CheckImportError();

  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+us:1.2", 1);
  CheckImportError();

  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+ud:-1", 1);
  CheckImportError();

  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+ud:1,2", 1);
  CheckImportError();
}

TEST_F(TestSchemaImport, DictionaryError) {
  // Bad index type
  FillPrimitive(AddChild(), "c");
  FillPrimitive("u");
  FillDictionary();
  CheckImportError();

  // Nested dictionary
  FillPrimitive(AddChild(), "c");
  FillPrimitive(AddChild(), "u");
  FillDictionary(LastChild());
  FillPrimitive("u");
  FillDictionary();
  CheckImportError();
}

TEST_F(TestSchemaImport, ExtensionError) {
  ExtensionTypeGuard guard(uuid());

  // Storage type doesn't match
  FillPrimitive("w:15");
  c_struct_.metadata = kEncodedUuidMetadata.c_str();
  CheckImportError();

  // Invalid serialization
  std::string bogus_metadata = kEncodedUuidMetadata;
  bogus_metadata[bogus_metadata.size() - 5] += 1;
  FillPrimitive("w:16");
  c_struct_.metadata = bogus_metadata.c_str();
  CheckImportError();
}

TEST_F(TestSchemaImport, RecursionError) {
  FillPrimitive(AddChild(), "c", "unused");
  auto c = AddChild();
  FillStructLike(c, "+s", 1, "child");
  FillStructLike("+s", 1, "parent");
  c->children[0] = &c_struct_;
  CheckImportError();
}

TEST_F(TestSchemaImport, ImportField) {
  FillPrimitive("c", "thing", kDefaultFlags);
  CheckImport(field("thing", int8()));
  FillPrimitive("c", "thing", 0);
  CheckImport(field("thing", int8(), /*nullable=*/false));
  // With metadata
  FillPrimitive("c", "thing", kDefaultFlags);
  c_struct_.metadata = kEncodedMetadata1.c_str();
  CheckImport(field("thing", int8(), /*nullable=*/true,
                    key_value_metadata(kMetadataKeys1, kMetadataValues1)));
}

TEST_F(TestSchemaImport, ImportSchema) {
  FillPrimitive(AddChild(), "l");
  FillListLike(AddChild(), "+l", "int_lists");
  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+s", 2);
  auto f1 = field("int_lists", list(int64()));
  auto f2 = field("strs", utf8());
  auto expected = schema({f1, f2});
  CheckImport(expected);

  // With metadata
  FillPrimitive(AddChild(), "l");
  FillListLike(AddChild(), "+l", "int_lists");
  LastChild()->metadata = kEncodedMetadata2.c_str();
  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+s", 2);
  c_struct_.metadata = kEncodedMetadata1.c_str();
  f1 = f1->WithMetadata(key_value_metadata(kMetadataKeys2, kMetadataValues2));
  expected = schema({f1, f2}, key_value_metadata(kMetadataKeys1, kMetadataValues1));
  CheckImport(expected);
}

TEST_F(TestSchemaImport, ImportSchemaError) {
  // Not a struct type
  FillPrimitive("n");
  CheckSchemaImportError();

  FillPrimitive(AddChild(), "l", "ints");
  FillPrimitive(AddChild(), "u", "strs");
  FillStructLike("+us:43,42", 2);
  CheckSchemaImportError();
}

////////////////////////////////////////////////////////////////////////////
// Data import tests

// [true, false, true, true, false, true, true, true] * 2
static const uint8_t bits_buffer1[] = {0xed, 0xed};

static const void* buffers_no_nulls_no_data[1] = {nullptr};
static const void* buffers_nulls_no_data1[1] = {bits_buffer1};

static const void* all_buffers_omitted[3] = {nullptr, nullptr, nullptr};

static const uint8_t data_buffer1[] = {1, 2,  3,  4,  5,  6,  7,  8,
                                       9, 10, 11, 12, 13, 14, 15, 16};
static const uint8_t data_buffer2[] = "abcdefghijklmnopqrstuvwxyz";
#if ARROW_LITTLE_ENDIAN
static const uint64_t data_buffer3[] = {123456789, 0, 987654321, 0};
#else
static const uint64_t data_buffer3[] = {0, 123456789, 0, 987654321};
#endif
static const uint8_t data_buffer4[] = {1, 2, 0, 1, 3, 0};
static const float data_buffer5[] = {0.0f, 1.5f, -2.0f, 3.0f, 4.0f, 5.0f};
static const double data_buffer6[] = {0.0, 1.5, -2.0, 3.0, 4.0, 5.0};
static const int32_t data_buffer7[] = {1234, 5678, 9012, 3456};
static const int64_t data_buffer8[] = {123456789, 987654321, -123456789, -987654321};
static const int64_t date64_data_buffer8[] = {86400000, 172800000, -86400000, -172800000};
#if ARROW_LITTLE_ENDIAN
static const void* primitive_buffers_no_nulls1_8[2] = {nullptr, data_buffer1};
static const void* primitive_buffers_no_nulls1_16[2] = {nullptr, data_buffer1};
static const void* primitive_buffers_no_nulls1_32[2] = {nullptr, data_buffer1};
static const void* primitive_buffers_no_nulls1_64[2] = {nullptr, data_buffer1};
static const void* primitive_buffers_nulls1_8[2] = {bits_buffer1, data_buffer1};
static const void* primitive_buffers_nulls1_16[2] = {bits_buffer1, data_buffer1};
#else
static const uint8_t data_buffer1_16[] = {2,  1, 4,  3,  6,  5,  8,  7,
                                          10, 9, 12, 11, 14, 13, 16, 15};
static const uint8_t data_buffer1_32[] = {4,  3,  2,  1, 8,  7,  6,  5,
                                          12, 11, 10, 9, 16, 15, 14, 13};
static const uint8_t data_buffer1_64[] = {8,  7,  6,  5,  4,  3,  2,  1,
                                          16, 15, 14, 13, 12, 11, 10, 9};
static const void* primitive_buffers_no_nulls1_8[2] = {nullptr, data_buffer1};
static const void* primitive_buffers_no_nulls1_16[2] = {nullptr, data_buffer1_16};
static const void* primitive_buffers_no_nulls1_32[2] = {nullptr, data_buffer1_32};
static const void* primitive_buffers_no_nulls1_64[2] = {nullptr, data_buffer1_64};
static const void* primitive_buffers_nulls1_8[2] = {bits_buffer1, data_buffer1};
static const void* primitive_buffers_nulls1_16[2] = {bits_buffer1, data_buffer1_16};
#endif
static const void* primitive_buffers_no_nulls2[2] = {nullptr, data_buffer2};
static const void* primitive_buffers_no_nulls3[2] = {nullptr, data_buffer3};
static const void* primitive_buffers_no_nulls4[2] = {nullptr, data_buffer4};
static const void* primitive_buffers_no_nulls5[2] = {nullptr, data_buffer5};
static const void* primitive_buffers_no_nulls6[2] = {nullptr, data_buffer6};
static const void* primitive_buffers_no_nulls7[2] = {nullptr, data_buffer7};
static const void* primitive_buffers_nulls7[2] = {bits_buffer1, data_buffer7};
static const void* primitive_buffers_no_nulls8[2] = {nullptr, data_buffer8};
static const void* primitive_buffers_nulls8[2] = {bits_buffer1, data_buffer8};

static const void* date64_buffers_no_nulls8[2] = {nullptr, date64_data_buffer8};
static const void* date64_buffers_nulls8[2] = {bits_buffer1, date64_data_buffer8};

static const int64_t timestamp_data_buffer1[] = {0, 951782400, -2203977600LL};
static const int64_t timestamp_data_buffer2[] = {0, 951782400000LL, -2203977600000LL};
static const int64_t timestamp_data_buffer3[] = {0, 951782400000000LL,
                                                 -2203977600000000LL};
static const int64_t timestamp_data_buffer4[] = {0, 951782400000000000LL,
                                                 -2203977600000000000LL};
static const void* timestamp_buffers_no_nulls1[2] = {nullptr, timestamp_data_buffer1};
static const void* timestamp_buffers_nulls1[2] = {bits_buffer1, timestamp_data_buffer1};
static const void* timestamp_buffers_no_nulls2[2] = {nullptr, timestamp_data_buffer2};
static const void* timestamp_buffers_no_nulls3[2] = {nullptr, timestamp_data_buffer3};
static const void* timestamp_buffers_no_nulls4[2] = {nullptr, timestamp_data_buffer4};

static const uint8_t string_data_buffer1[] = "foobarquuxxyzzy";

static const int32_t string_offsets_buffer1[] = {0, 3, 3, 6, 10, 15};
static const void* string_buffers_no_nulls1[3] = {nullptr, string_offsets_buffer1,
                                                  string_data_buffer1};
static const void* string_buffers_omitted[3] = {nullptr, string_offsets_buffer1, nullptr};

static const int64_t large_string_offsets_buffer1[] = {0, 3, 3, 6, 10};
static const void* large_string_buffers_no_nulls1[3] = {
    nullptr, large_string_offsets_buffer1, string_data_buffer1};
static const void* large_string_buffers_omitted[3] = {
    nullptr, large_string_offsets_buffer1, nullptr};

static const int32_t list_offsets_buffer1[] = {0, 2, 2, 5, 6, 8};
static const void* list_buffers_no_nulls1[2] = {nullptr, list_offsets_buffer1};
static const void* list_buffers_nulls1[2] = {bits_buffer1, list_offsets_buffer1};

static const int64_t large_list_offsets_buffer1[] = {0, 2, 2, 5, 6, 8};
static const void* large_list_buffers_no_nulls1[2] = {nullptr,
                                                      large_list_offsets_buffer1};

static const int8_t type_codes_buffer1[] = {42, 42, 43, 43, 42};
static const int32_t union_offsets_buffer1[] = {0, 1, 0, 1, 2};
static const void* sparse_union_buffers1_legacy[2] = {nullptr, type_codes_buffer1};
static const void* dense_union_buffers1_legacy[3] = {nullptr, type_codes_buffer1,
                                                     union_offsets_buffer1};
static const void* sparse_union_buffers1[1] = {type_codes_buffer1};
static const void* dense_union_buffers1[2] = {type_codes_buffer1, union_offsets_buffer1};

void NoOpArrayRelease(struct ArrowArray* schema) { ArrowArrayMarkReleased(schema); }

class TestArrayImport : public ::testing::Test {
 public:
  void SetUp() override { Reset(); }

  void Reset() {
    memset(&c_struct_, 0, sizeof(c_struct_));
    c_struct_.release = NoOpArrayRelease;
    nested_structs_.clear();
    children_arrays_.clear();
  }

  // Create a new ArrowArray struct with a stable C pointer
  struct ArrowArray* AddChild() {
    nested_structs_.emplace_back();
    struct ArrowArray* result = &nested_structs_.back();
    memset(result, 0, sizeof(*result));
    result->release = NoOpArrayRelease;
    return result;
  }

  // Create a stable C pointer to the N last structs in nested_structs_
  struct ArrowArray** NLastChildren(int64_t n_children, struct ArrowArray* parent) {
    children_arrays_.emplace_back(n_children);
    struct ArrowArray** children = children_arrays_.back().data();
    int64_t nested_offset;
    // If parent is itself at the end of nested_structs_, skip it
    if (parent != nullptr && &nested_structs_.back() == parent) {
      nested_offset = static_cast<int64_t>(nested_structs_.size()) - n_children - 1;
    } else {
      nested_offset = static_cast<int64_t>(nested_structs_.size()) - n_children;
    }
    for (int64_t i = 0; i < n_children; ++i) {
      children[i] = &nested_structs_[nested_offset + i];
    }
    return children;
  }

  struct ArrowArray* LastChild(struct ArrowArray* parent = nullptr) {
    return *NLastChildren(1, parent);
  }

  void FillPrimitive(struct ArrowArray* c, int64_t length, int64_t null_count,
                     int64_t offset, const void** buffers) {
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 2;
    c->buffers = buffers;
  }

  void FillDictionary(struct ArrowArray* c) { c->dictionary = LastChild(c); }

  void FillStringLike(struct ArrowArray* c, int64_t length, int64_t null_count,
                      int64_t offset, const void** buffers) {
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 3;
    c->buffers = buffers;
  }

  void FillListLike(struct ArrowArray* c, int64_t length, int64_t null_count,
                    int64_t offset, const void** buffers) {
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 2;
    c->buffers = buffers;
    c->n_children = 1;
    c->children = NLastChildren(1, c);
  }

  void FillFixedSizeListLike(struct ArrowArray* c, int64_t length, int64_t null_count,
                             int64_t offset, const void** buffers) {
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 1;
    c->buffers = buffers;
    c->n_children = 1;
    c->children = NLastChildren(1, c);
  }

  void FillStructLike(struct ArrowArray* c, int64_t length, int64_t null_count,
                      int64_t offset, int64_t n_children, const void** buffers) {
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 1;
    c->buffers = buffers;
    c->n_children = n_children;
    c->children = NLastChildren(c->n_children, c);
  }

  // `legacy` selects pre-ARROW-14179 behaviour
  void FillUnionLike(struct ArrowArray* c, UnionMode::type mode, int64_t length,
                     int64_t null_count, int64_t offset, int64_t n_children,
                     const void** buffers, bool legacy) {
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    if (mode == UnionMode::SPARSE) {
      c->n_buffers = legacy ? 2 : 1;
    } else {
      c->n_buffers = legacy ? 3 : 2;
    }
    c->buffers = buffers;
    c->n_children = n_children;
    c->children = NLastChildren(c->n_children, c);
  }

  void FillPrimitive(int64_t length, int64_t null_count, int64_t offset,
                     const void** buffers) {
    FillPrimitive(&c_struct_, length, null_count, offset, buffers);
  }

  void FillDictionary() { FillDictionary(&c_struct_); }

  void FillStringLike(int64_t length, int64_t null_count, int64_t offset,
                      const void** buffers) {
    FillStringLike(&c_struct_, length, null_count, offset, buffers);
  }

  void FillListLike(int64_t length, int64_t null_count, int64_t offset,
                    const void** buffers) {
    FillListLike(&c_struct_, length, null_count, offset, buffers);
  }

  void FillFixedSizeListLike(int64_t length, int64_t null_count, int64_t offset,
                             const void** buffers) {
    FillFixedSizeListLike(&c_struct_, length, null_count, offset, buffers);
  }

  void FillStructLike(int64_t length, int64_t null_count, int64_t offset,
                      int64_t n_children, const void** buffers) {
    FillStructLike(&c_struct_, length, null_count, offset, n_children, buffers);
  }

  void FillUnionLike(UnionMode::type mode, int64_t length, int64_t null_count,
                     int64_t offset, int64_t n_children, const void** buffers,
                     bool legacy) {
    FillUnionLike(&c_struct_, mode, length, null_count, offset, n_children, buffers,
                  legacy);
  }

  void CheckImport(const std::shared_ptr<Array>& expected) {
    ArrayReleaseCallback cb(&c_struct_);

    auto type = expected->type();
    ASSERT_OK_AND_ASSIGN(auto array, ImportArray(&c_struct_, type));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_struct_));  // was moved
    Reset();                                        // for further tests

    ASSERT_OK(array->ValidateFull());
    // Special case: arrays without data (such as Null arrays) needn't keep
    // the ArrowArray struct alive.
    if (HasData(&c_struct_)) {
      cb.AssertNotCalled();
    }
    AssertArraysEqual(*expected, *array, true);
    array.reset();
    cb.AssertCalled();
  }

  void CheckImport(const std::shared_ptr<RecordBatch>& expected) {
    ArrayReleaseCallback cb(&c_struct_);

    auto schema = expected->schema();
    ASSERT_OK_AND_ASSIGN(auto batch, ImportRecordBatch(&c_struct_, schema));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_struct_));  // was moved
    Reset();                                        // for further tests

    ASSERT_OK(batch->ValidateFull());
    AssertBatchesEqual(*expected, *batch);
    cb.AssertNotCalled();
    batch.reset();
    cb.AssertCalled();
  }

  void CheckImportError(const std::shared_ptr<DataType>& type) {
    ArrayReleaseCallback cb(&c_struct_);

    ASSERT_RAISES(Invalid, ImportArray(&c_struct_, type));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_struct_));
    Reset();            // for further tests
    cb.AssertCalled();  // was released
  }

  void CheckImportError(const std::shared_ptr<Schema>& schema) {
    ArrayReleaseCallback cb(&c_struct_);

    ASSERT_RAISES(Invalid, ImportRecordBatch(&c_struct_, schema));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_struct_));
    Reset();            // for further tests
    cb.AssertCalled();  // was released
  }

 protected:
  struct ArrowArray c_struct_;
  // Deque elements don't move when the deque is appended to, which allows taking
  // stable C pointers to them.
  std::deque<struct ArrowArray> nested_structs_;
  std::deque<std::vector<struct ArrowArray*>> children_arrays_;
};

TEST_F(TestArrayImport, Primitive) {
  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls1_8);
  CheckImport(ArrayFromJSON(int8(), "[1, 2, 3]"));
  FillPrimitive(5, 0, 0, primitive_buffers_no_nulls1_8);
  CheckImport(ArrayFromJSON(uint8(), "[1, 2, 3, 4, 5]"));
  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls1_16);
  CheckImport(ArrayFromJSON(int16(), "[513, 1027, 1541]"));
  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls1_16);
  CheckImport(ArrayFromJSON(uint16(), "[513, 1027, 1541]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls1_32);
  CheckImport(ArrayFromJSON(int32(), "[67305985, 134678021]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls1_32);
  CheckImport(ArrayFromJSON(uint32(), "[67305985, 134678021]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls1_64);
  CheckImport(ArrayFromJSON(int64(), "[578437695752307201, 1157159078456920585]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls1_64);
  CheckImport(ArrayFromJSON(uint64(), "[578437695752307201, 1157159078456920585]"));

  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls1_8);
  CheckImport(ArrayFromJSON(boolean(), "[true, false, false]"));
  FillPrimitive(6, 0, 0, primitive_buffers_no_nulls5);
  CheckImport(ArrayFromJSON(float32(), "[0.0, 1.5, -2.0, 3.0, 4.0, 5.0]"));
  FillPrimitive(6, 0, 0, primitive_buffers_no_nulls6);
  CheckImport(ArrayFromJSON(float64(), "[0.0, 1.5, -2.0, 3.0, 4.0, 5.0]"));

  // With nulls
  FillPrimitive(9, -1, 0, primitive_buffers_nulls1_8);
  CheckImport(ArrayFromJSON(int8(), "[1, null, 3, 4, null, 6, 7, 8, 9]"));
  FillPrimitive(9, 2, 0, primitive_buffers_nulls1_8);
  CheckImport(ArrayFromJSON(int8(), "[1, null, 3, 4, null, 6, 7, 8, 9]"));
  FillPrimitive(3, -1, 0, primitive_buffers_nulls1_16);
  CheckImport(ArrayFromJSON(int16(), "[513, null, 1541]"));
  FillPrimitive(3, 1, 0, primitive_buffers_nulls1_16);
  CheckImport(ArrayFromJSON(int16(), "[513, null, 1541]"));
  FillPrimitive(3, -1, 0, primitive_buffers_nulls1_8);
  CheckImport(ArrayFromJSON(boolean(), "[true, null, false]"));
  FillPrimitive(3, 1, 0, primitive_buffers_nulls1_8);
  CheckImport(ArrayFromJSON(boolean(), "[true, null, false]"));

  // Empty array with null data pointers
  FillPrimitive(0, 0, 0, all_buffers_omitted);
  CheckImport(ArrayFromJSON(int32(), "[]"));
}

TEST_F(TestArrayImport, Temporal) {
  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(date32(), "[1234, 5678, 9012]"));
  FillPrimitive(3, 0, 0, date64_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(date64(), "[86400000, 172800000, -86400000]"));

  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(time32(TimeUnit::SECOND), "[1234, 5678]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(time32(TimeUnit::MILLI), "[1234, 5678]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(time64(TimeUnit::MICRO), "[123456789, 987654321]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(time64(TimeUnit::NANO), "[123456789, 987654321]"));

  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::SECOND), "[123456789, 987654321]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::MILLI), "[123456789, 987654321]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::MICRO), "[123456789, 987654321]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::NANO), "[123456789, 987654321]"));

  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(month_interval(), "[1234, 5678, 9012]"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(day_time_interval(), "[[1234, 5678], [9012, 3456]]"));

  const char* json = R"(["1970-01-01","2000-02-29","1900-02-28"])";
  FillPrimitive(3, 0, 0, timestamp_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::SECOND), json));
  FillPrimitive(3, 0, 0, timestamp_buffers_no_nulls2);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::MILLI), json));
  FillPrimitive(3, 0, 0, timestamp_buffers_no_nulls3);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::MICRO), json));
  FillPrimitive(3, 0, 0, timestamp_buffers_no_nulls4);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::NANO), json));

  // With nulls
  FillPrimitive(3, -1, 0, primitive_buffers_nulls7);
  CheckImport(ArrayFromJSON(date32(), "[1234, null, 9012]"));
  FillPrimitive(3, -1, 0, date64_buffers_nulls8);
  CheckImport(ArrayFromJSON(date64(), "[86400000, null, -86400000]"));
  FillPrimitive(2, -1, 0, primitive_buffers_nulls8);
  CheckImport(ArrayFromJSON(time64(TimeUnit::NANO), "[123456789, null]"));
  FillPrimitive(2, -1, 0, primitive_buffers_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::NANO), "[123456789, null]"));
  FillPrimitive(3, -1, 0, primitive_buffers_nulls7);
  CheckImport(ArrayFromJSON(month_interval(), "[1234, null, 9012]"));
  FillPrimitive(2, -1, 0, primitive_buffers_nulls7);
  CheckImport(ArrayFromJSON(day_time_interval(), "[[1234, 5678], null]"));
  FillPrimitive(3, -1, 0, timestamp_buffers_nulls1);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC+2"),
                            R"(["1970-01-01",null,"1900-02-28"])"));
}

TEST_F(TestArrayImport, Null) {
  // Arrow C++ used to export null arrays with a null bitmap buffer
  for (const int64_t n_buffers : {0, 1}) {
    const void* buffers[] = {nullptr};
    c_struct_.length = 3;
    c_struct_.null_count = 3;
    c_struct_.offset = 0;
    c_struct_.buffers = buffers;
    c_struct_.n_buffers = n_buffers;
    CheckImport(ArrayFromJSON(null(), "[null, null, null]"));
  }
}

TEST_F(TestArrayImport, PrimitiveWithOffset) {
  FillPrimitive(3, 0, 2, primitive_buffers_no_nulls1_8);
  CheckImport(ArrayFromJSON(int8(), "[3, 4, 5]"));
  FillPrimitive(3, 0, 1, primitive_buffers_no_nulls1_16);
  CheckImport(ArrayFromJSON(uint16(), "[1027, 1541, 2055]"));

  FillPrimitive(4, 0, 7, primitive_buffers_no_nulls1_8);
  CheckImport(ArrayFromJSON(boolean(), "[false, false, true, false]"));

  // Empty array with null data pointers
  FillPrimitive(0, 0, 2, all_buffers_omitted);
  CheckImport(ArrayFromJSON(int32(), "[]"));
  FillPrimitive(0, 0, 3, all_buffers_omitted);
  CheckImport(ArrayFromJSON(boolean(), "[]"));
}

TEST_F(TestArrayImport, NullWithOffset) {
  const void* buffers[] = {nullptr};
  c_struct_.length = 3;
  c_struct_.null_count = 3;
  c_struct_.offset = 5;
  c_struct_.n_buffers = 1;
  c_struct_.buffers = buffers;
  CheckImport(ArrayFromJSON(null(), "[null, null, null]"));
}

TEST_F(TestArrayImport, String) {
  FillStringLike(4, 0, 0, string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(utf8(), R"(["foo", "", "bar", "quux"])"));
  FillStringLike(4, 0, 0, string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(binary(), R"(["foo", "", "bar", "quux"])"));
  FillStringLike(4, 0, 0, large_string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(large_utf8(), R"(["foo", "", "bar", "quux"])"));
  FillStringLike(4, 0, 0, large_string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(large_binary(), R"(["foo", "", "bar", "quux"])"));

  // Empty array with null data pointers
  FillStringLike(0, 0, 0, string_buffers_omitted);
  CheckImport(ArrayFromJSON(utf8(), "[]"));
  FillStringLike(0, 0, 0, large_string_buffers_omitted);
  CheckImport(ArrayFromJSON(large_binary(), "[]"));
}

TEST_F(TestArrayImport, StringWithOffset) {
  FillStringLike(3, 0, 1, string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(utf8(), R"(["", "bar", "quux"])"));
  FillStringLike(2, 0, 2, large_string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(large_utf8(), R"(["bar", "quux"])"));

  // Empty array with null data pointers
  FillStringLike(0, 0, 1, string_buffers_omitted);
  CheckImport(ArrayFromJSON(utf8(), "[]"));
}

TEST_F(TestArrayImport, FixedSizeBinary) {
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls2);
  CheckImport(ArrayFromJSON(fixed_size_binary(3), R"(["abc", "def"])"));
  FillPrimitive(2, 0, 0, primitive_buffers_no_nulls3);
  CheckImport(ArrayFromJSON(decimal(15, 4), R"(["12345.6789", "98765.4321"])"));

  // Empty array with null data pointers
  FillPrimitive(0, 0, 0, all_buffers_omitted);
  CheckImport(ArrayFromJSON(fixed_size_binary(3), "[]"));
  FillPrimitive(0, 0, 0, all_buffers_omitted);
  CheckImport(ArrayFromJSON(decimal(15, 4), "[]"));
}

TEST_F(TestArrayImport, FixedSizeBinaryWithOffset) {
  FillPrimitive(1, 0, 1, primitive_buffers_no_nulls2);
  CheckImport(ArrayFromJSON(fixed_size_binary(3), R"(["def"])"));
  FillPrimitive(1, 0, 1, primitive_buffers_no_nulls3);
  CheckImport(ArrayFromJSON(decimal(15, 4), R"(["98765.4321"])"));

  // Empty array with null data pointers
  FillPrimitive(0, 0, 1, all_buffers_omitted);
  CheckImport(ArrayFromJSON(fixed_size_binary(3), "[]"));
  FillPrimitive(0, 0, 1, all_buffers_omitted);
  CheckImport(ArrayFromJSON(decimal(15, 4), "[]"));
}

TEST_F(TestArrayImport, List) {
  FillPrimitive(AddChild(), 8, 0, 0, primitive_buffers_no_nulls1_8);
  FillListLike(5, 0, 0, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int8()), "[[1, 2], [], [3, 4, 5], [6], [7, 8]]"));
  FillPrimitive(AddChild(), 5, 0, 0, primitive_buffers_no_nulls1_16);
  FillListLike(3, 1, 0, list_buffers_nulls1);
  CheckImport(ArrayFromJSON(list(int16()), "[[513, 1027], null, [1541, 2055, 2569]]"));

  // Large list
  FillPrimitive(AddChild(), 5, 0, 0, primitive_buffers_no_nulls1_16);
  FillListLike(3, 0, 0, large_list_buffers_no_nulls1);
  CheckImport(
      ArrayFromJSON(large_list(int16()), "[[513, 1027], [], [1541, 2055, 2569]]"));

  // Fixed-size list
  FillPrimitive(AddChild(), 9, 0, 0, primitive_buffers_no_nulls1_8);
  FillFixedSizeListLike(3, 0, 0, buffers_no_nulls_no_data);
  CheckImport(
      ArrayFromJSON(fixed_size_list(int8(), 3), "[[1, 2, 3], [4, 5, 6], [7, 8, 9]]"));

  // Empty child array with null data pointers
  FillPrimitive(AddChild(), 0, 0, 0, all_buffers_omitted);
  FillFixedSizeListLike(0, 0, 0, buffers_no_nulls_no_data);
  CheckImport(ArrayFromJSON(fixed_size_list(int8(), 3), "[]"));
}

TEST_F(TestArrayImport, NestedList) {
  FillPrimitive(AddChild(), 8, 0, 0, primitive_buffers_no_nulls1_8);
  FillListLike(AddChild(), 5, 0, 0, list_buffers_no_nulls1);
  FillListLike(3, 0, 0, large_list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(large_list(list(int8())),
                            "[[[1, 2], []], [], [[3, 4, 5], [6], [7, 8]]]"));

  FillPrimitive(AddChild(), 6, 0, 0, primitive_buffers_no_nulls1_8);
  FillFixedSizeListLike(AddChild(), 2, 0, 0, buffers_no_nulls_no_data);
  FillListLike(2, 0, 0, list_buffers_no_nulls1);
  CheckImport(
      ArrayFromJSON(list(fixed_size_list(int8(), 3)), "[[[1, 2, 3], [4, 5, 6]], []]"));
}

TEST_F(TestArrayImport, ListWithOffset) {
  // Offset in child
  FillPrimitive(AddChild(), 8, 0, 1, primitive_buffers_no_nulls1_8);
  FillListLike(5, 0, 0, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int8()), "[[2, 3], [], [4, 5, 6], [7], [8, 9]]"));

  FillPrimitive(AddChild(), 9, 0, 1, primitive_buffers_no_nulls1_8);
  FillFixedSizeListLike(3, 0, 0, buffers_no_nulls_no_data);
  CheckImport(
      ArrayFromJSON(fixed_size_list(int8(), 3), "[[2, 3, 4], [5, 6, 7], [8, 9, 10]]"));

  // Offset in parent
  FillPrimitive(AddChild(), 8, 0, 0, primitive_buffers_no_nulls1_8);
  FillListLike(4, 0, 1, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int8()), "[[], [3, 4, 5], [6], [7, 8]]"));

  FillPrimitive(AddChild(), 9, 0, 0, primitive_buffers_no_nulls1_8);
  FillFixedSizeListLike(3, 0, 1, buffers_no_nulls_no_data);
  CheckImport(
      ArrayFromJSON(fixed_size_list(int8(), 3), "[[4, 5, 6], [7, 8, 9], [10, 11, 12]]"));

  // Both
  FillPrimitive(AddChild(), 8, 0, 2, primitive_buffers_no_nulls1_8);
  FillListLike(4, 0, 1, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int8()), "[[], [5, 6, 7], [8], [9, 10]]"));

  FillPrimitive(AddChild(), 9, 0, 2, primitive_buffers_no_nulls1_8);
  FillFixedSizeListLike(3, 0, 1, buffers_no_nulls_no_data);
  CheckImport(ArrayFromJSON(fixed_size_list(int8(), 3),
                            "[[6, 7, 8], [9, 10, 11], [12, 13, 14]]"));
}

TEST_F(TestArrayImport, Struct) {
  FillStringLike(AddChild(), 3, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, -1, 0, primitive_buffers_nulls1_16);
  FillStructLike(3, 0, 0, 2, buffers_no_nulls_no_data);
  auto expected = ArrayFromJSON(struct_({field("strs", utf8()), field("ints", uint16())}),
                                R"([["foo", 513], ["", null], ["bar", 1541]])");
  CheckImport(expected);

  FillStringLike(AddChild(), 3, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, 0, 0, primitive_buffers_no_nulls1_16);
  FillStructLike(3, -1, 0, 2, buffers_nulls_no_data1);
  expected = ArrayFromJSON(struct_({field("strs", utf8()), field("ints", uint16())}),
                           R"([["foo", 513], null, ["bar", 1541]])");
  CheckImport(expected);

  FillStringLike(AddChild(), 3, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, 0, 0, primitive_buffers_no_nulls1_16);
  FillStructLike(3, -1, 0, 2, buffers_nulls_no_data1);
  expected = ArrayFromJSON(
      struct_({field("strs", utf8(), /*nullable=*/false), field("ints", uint16())}),
      R"([["foo", 513], null, ["bar", 1541]])");
  CheckImport(expected);
}

TEST_F(TestArrayImport, SparseUnion) {
  auto type = sparse_union({field("strs", utf8()), field("ints", int8())}, {43, 42});
  auto expected =
      ArrayFromJSON(type, R"([[42, 1], [42, null], [43, "bar"], [43, "quux"]])");

  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 4, -1, 0, primitive_buffers_nulls1_8);
  FillUnionLike(UnionMode::SPARSE, 4, 0, 0, 2, sparse_union_buffers1, /*legacy=*/false);
  CheckImport(expected);

  // Legacy format with null bitmap (ARROW-14179)
  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 4, -1, 0, primitive_buffers_nulls1_8);
  FillUnionLike(UnionMode::SPARSE, 4, 0, 0, 2, sparse_union_buffers1_legacy,
                /*legacy=*/true);
  CheckImport(expected);

  // Empty array with null data pointers
  expected = ArrayFromJSON(type, "[]");
  FillStringLike(AddChild(), 0, 0, 0, string_buffers_omitted);
  FillPrimitive(AddChild(), 0, 0, 0, all_buffers_omitted);
  FillUnionLike(UnionMode::SPARSE, 0, 0, 0, 2, all_buffers_omitted, /*legacy=*/false);
  FillStringLike(AddChild(), 0, 0, 0, string_buffers_omitted);
  FillPrimitive(AddChild(), 0, 0, 0, all_buffers_omitted);
  FillUnionLike(UnionMode::SPARSE, 0, 0, 3, 2, all_buffers_omitted, /*legacy=*/false);
}

TEST_F(TestArrayImport, DenseUnion) {
  auto type = dense_union({field("strs", utf8()), field("ints", int8())}, {43, 42});
  auto expected =
      ArrayFromJSON(type, R"([[42, 1], [42, null], [43, "foo"], [43, ""], [42, 3]])");

  FillStringLike(AddChild(), 2, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, -1, 0, primitive_buffers_nulls1_8);
  FillUnionLike(UnionMode::DENSE, 5, 0, 0, 2, dense_union_buffers1, /*legacy=*/false);
  CheckImport(expected);

  // Legacy format with null bitmap (ARROW-14179)
  FillStringLike(AddChild(), 2, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, -1, 0, primitive_buffers_nulls1_8);
  FillUnionLike(UnionMode::DENSE, 5, 0, 0, 2, dense_union_buffers1_legacy,
                /*legacy=*/true);
  CheckImport(expected);

  // Empty array with null data pointers
  expected = ArrayFromJSON(type, "[]");
  FillStringLike(AddChild(), 0, 0, 0, string_buffers_omitted);
  FillPrimitive(AddChild(), 0, 0, 0, all_buffers_omitted);
  FillUnionLike(UnionMode::DENSE, 0, 0, 0, 2, all_buffers_omitted, /*legacy=*/false);
  FillStringLike(AddChild(), 0, 0, 0, string_buffers_omitted);
  FillPrimitive(AddChild(), 0, 0, 0, all_buffers_omitted);
  FillUnionLike(UnionMode::DENSE, 0, 0, 3, 2, all_buffers_omitted, /*legacy=*/false);
}

TEST_F(TestArrayImport, StructWithOffset) {
  // Child
  FillStringLike(AddChild(), 3, 0, 1, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, 0, 2, primitive_buffers_no_nulls1_8);
  FillStructLike(3, 0, 0, 2, buffers_no_nulls_no_data);
  auto expected = ArrayFromJSON(struct_({field("strs", utf8()), field("ints", int8())}),
                                R"([["", 3], ["bar", 4], ["quux", 5]])");
  CheckImport(expected);

  // Parent and child
  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 4, 0, 2, primitive_buffers_no_nulls1_8);
  FillStructLike(3, 0, 1, 2, buffers_no_nulls_no_data);
  expected = ArrayFromJSON(struct_({field("strs", utf8()), field("ints", int8())}),
                           R"([["", 4], ["bar", 5], ["quux", 6]])");
  CheckImport(expected);
}

TEST_F(TestArrayImport, Map) {
  FillStringLike(AddChild(), 5, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 5, 0, 0, primitive_buffers_no_nulls1_8);
  FillStructLike(AddChild(), 5, 0, 0, 2, buffers_no_nulls_no_data);
  FillListLike(3, 1, 0, list_buffers_nulls1);
  auto expected = ArrayFromJSON(
      map(utf8(), uint8()),
      R"([[["foo", 1], ["", 2]], null, [["bar", 3], ["quux", 4], ["xyzzy", 5]]])");
  CheckImport(expected);
}

TEST_F(TestArrayImport, Dictionary) {
  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(6, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary();

  auto dict_values = ArrayFromJSON(utf8(), R"(["foo", "", "bar", "quux"])");
  auto indices = ArrayFromJSON(int8(), "[1, 2, 0, 1, 3, 0]");
  ASSERT_OK_AND_ASSIGN(
      auto expected,
      DictionaryArray::FromArrays(dictionary(int8(), utf8()), indices, dict_values));
  CheckImport(expected);

  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(6, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary();

  ASSERT_OK_AND_ASSIGN(
      expected, DictionaryArray::FromArrays(dictionary(int8(), utf8(), /*ordered=*/true),
                                            indices, dict_values));
  CheckImport(expected);
}

TEST_F(TestArrayImport, NestedDictionary) {
  FillPrimitive(AddChild(), 6, 0, 0, primitive_buffers_no_nulls1_8);
  FillListLike(AddChild(), 4, 0, 0, list_buffers_no_nulls1);
  FillPrimitive(6, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary();

  auto dict_values = ArrayFromJSON(list(int8()), "[[1, 2], [], [3, 4, 5], [6]]");
  auto indices = ArrayFromJSON(int8(), "[1, 2, 0, 1, 3, 0]");
  ASSERT_OK_AND_ASSIGN(auto expected,
                       DictionaryArray::FromArrays(dictionary(int8(), list(int8())),
                                                   indices, dict_values));
  CheckImport(expected);

  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 6, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary(LastChild());
  FillListLike(3, 0, 0, list_buffers_no_nulls1);

  dict_values = ArrayFromJSON(utf8(), R"(["foo", "", "bar", "quux"])");
  indices = ArrayFromJSON(int8(), "[1, 2, 0, 1, 3, 0]");
  ASSERT_OK_AND_ASSIGN(
      auto dict_array,
      DictionaryArray::FromArrays(dictionary(int8(), utf8()), indices, dict_values));
  auto offsets = ArrayFromJSON(int32(), "[0, 2, 2, 5]");
  ASSERT_OK_AND_ASSIGN(expected, ListArray::FromArrays(*offsets, *dict_array));
  CheckImport(expected);
}

TEST_F(TestArrayImport, DictionaryWithOffset) {
  FillStringLike(AddChild(), 3, 0, 1, string_buffers_no_nulls1);
  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary();

  auto expected = DictArrayFromJSON(dictionary(int8(), utf8()), "[1, 2, 0]",
                                    R"(["", "bar", "quux"])");
  CheckImport(expected);

  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(4, 0, 2, primitive_buffers_no_nulls4);
  FillDictionary();

  expected = DictArrayFromJSON(dictionary(int8(), utf8()), "[0, 1, 3, 0]",
                               R"(["foo", "", "bar", "quux"])");
  CheckImport(expected);
}

TEST_F(TestArrayImport, RegisteredExtension) {
  ExtensionTypeGuard guard({smallint(), dict_extension_type(), complex128()});

  // smallint
  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls1_16);
  auto expected =
      ExtensionType::WrapArray(smallint(), ArrayFromJSON(int16(), "[513, 1027, 1541]"));
  CheckImport(expected);

  // dict_extension_type
  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(6, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary();

  auto storage = DictArrayFromJSON(dictionary(int8(), utf8()), "[1, 2, 0, 1, 3, 0]",
                                   R"(["foo", "", "bar", "quux"])");
  expected = ExtensionType::WrapArray(dict_extension_type(), storage);
  CheckImport(expected);

  // complex128
  FillPrimitive(AddChild(), 3, 0, /*offset=*/0, primitive_buffers_no_nulls6);
  FillPrimitive(AddChild(), 3, 0, /*offset=*/3, primitive_buffers_no_nulls6);
  FillStructLike(3, 0, 0, 2, buffers_no_nulls_no_data);
  expected = MakeComplex128(ArrayFromJSON(float64(), "[0.0, 1.5, -2.0]"),
                            ArrayFromJSON(float64(), "[3.0, 4.0, 5.0]"));
  CheckImport(expected);
}

TEST_F(TestArrayImport, PrimitiveError) {
  // Bad number of buffers
  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls1_8);
  c_struct_.n_buffers = 1;
  CheckImportError(int8());

  // Zero null bitmap but non-zero null_count
  FillPrimitive(3, 1, 0, primitive_buffers_no_nulls1_8);
  CheckImportError(int8());

  // Null data pointers with non-zero length
  FillPrimitive(1, 0, 0, all_buffers_omitted);
  CheckImportError(int8());
  FillPrimitive(1, 0, 0, all_buffers_omitted);
  CheckImportError(boolean());
  FillPrimitive(1, 0, 0, all_buffers_omitted);
  CheckImportError(fixed_size_binary(3));
}

TEST_F(TestArrayImport, StringError) {
  // Bad number of buffers
  FillStringLike(4, 0, 0, string_buffers_no_nulls1);
  c_struct_.n_buffers = 2;
  CheckImportError(utf8());

  // Null data pointers with non-zero length
  FillStringLike(4, 0, 0, string_buffers_omitted);
  CheckImportError(utf8());

  // Null offsets pointer
  FillStringLike(0, 0, 0, all_buffers_omitted);
  CheckImportError(utf8());
}

TEST_F(TestArrayImport, StructError) {
  // Bad number of children
  FillStringLike(AddChild(), 3, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, -1, 0, primitive_buffers_nulls1_8);
  FillStructLike(3, 0, 0, 2, buffers_no_nulls_no_data);
  CheckImportError(struct_({field("strs", utf8())}));
}

TEST_F(TestArrayImport, ListError) {
  // Null offsets pointer
  FillPrimitive(AddChild(), 0, 0, 0, primitive_buffers_no_nulls1_8);
  FillListLike(0, 0, 0, all_buffers_omitted);
  CheckImportError(list(int8()));
}

TEST_F(TestArrayImport, MapError) {
  // Bad number of (struct) children in map child
  FillStringLike(AddChild(), 5, 0, 0, string_buffers_no_nulls1);
  FillStructLike(AddChild(), 5, 0, 0, 1, buffers_no_nulls_no_data);
  FillListLike(3, 1, 0, list_buffers_nulls1);
  CheckImportError(map(utf8(), uint8()));
}

TEST_F(TestArrayImport, UnionError) {
  // Non-zero null count
  auto type = sparse_union({field("strs", utf8()), field("ints", int8())}, {43, 42});
  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 4, -1, 0, primitive_buffers_nulls1_8);
  FillUnionLike(UnionMode::SPARSE, 4, -1, 0, 2, sparse_union_buffers1, /*legacy=*/false);
  CheckImportError(type);

  type = dense_union({field("strs", utf8()), field("ints", int8())}, {43, 42});
  FillStringLike(AddChild(), 2, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, -1, 0, primitive_buffers_nulls1_8);
  FillUnionLike(UnionMode::DENSE, 5, -1, 0, 2, dense_union_buffers1, /*legacy=*/false);
  CheckImportError(type);
}

TEST_F(TestArrayImport, DictionaryError) {
  // Missing dictionary field
  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls4);
  CheckImportError(dictionary(int8(), utf8()));

  // Unexpected dictionary field
  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(6, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary();
  CheckImportError(int8());
}

TEST_F(TestArrayImport, RecursionError) {
  // Infinite loop through children
  FillStringLike(AddChild(), 3, 0, 0, string_buffers_no_nulls1);
  FillStructLike(AddChild(), 3, 0, 0, 1, buffers_no_nulls_no_data);
  FillStructLike(3, 0, 0, 1, buffers_no_nulls_no_data);
  c_struct_.children[0] = &c_struct_;
  CheckImportError(struct_({field("ints", struct_({field("ints", int8())}))}));
}

TEST_F(TestArrayImport, ImportRecordBatch) {
  auto schema = ::arrow::schema(
      {field("strs", utf8(), /*nullable=*/false), field("ints", uint16())});
  auto expected_strs = ArrayFromJSON(utf8(), R"(["", "bar", "quux"])");
  auto expected_ints = ArrayFromJSON(uint16(), "[513, null, 1541]");

  FillStringLike(AddChild(), 3, 0, 1, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, -1, 0, primitive_buffers_nulls1_16);
  FillStructLike(3, 0, 0, 2, buffers_no_nulls_no_data);

  auto expected = RecordBatch::Make(schema, 3, {expected_strs, expected_ints});
  CheckImport(expected);
}

TEST_F(TestArrayImport, ImportRecordBatchError) {
  // Struct with non-zero parent offset
  FillStringLike(AddChild(), 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 4, 0, 0, primitive_buffers_no_nulls1_16);
  FillStructLike(3, 0, 1, 2, buffers_no_nulls_no_data);
  auto schema = ::arrow::schema({field("strs", utf8()), field("ints", uint16())});
  CheckImportError(schema);

  // Struct with nulls in parent
  FillStringLike(AddChild(), 3, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, 0, 0, primitive_buffers_no_nulls1_8);
  FillStructLike(3, 1, 0, 2, buffers_nulls_no_data1);
  CheckImportError(schema);
}

TEST_F(TestArrayImport, ImportArrayAndType) {
  // Test importing both array and its type at the same time
  SchemaStructBuilder schema_builder;
  schema_builder.FillPrimitive("c");
  SchemaReleaseCallback schema_cb(&schema_builder.c_struct_);

  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls1_8);
  ArrayReleaseCallback array_cb(&c_struct_);

  ASSERT_OK_AND_ASSIGN(auto array, ImportArray(&c_struct_, &schema_builder.c_struct_));
  AssertArraysEqual(*array, *ArrayFromJSON(int8(), "[1, 2, 3]"));
  schema_cb.AssertCalled();  // was released
  array_cb.AssertNotCalled();
  ASSERT_TRUE(ArrowArrayIsReleased(&c_struct_));  // was moved
  array.reset();
  array_cb.AssertCalled();
}

TEST_F(TestArrayImport, ImportArrayAndTypeError) {
  // On error, both structs are released
  SchemaStructBuilder schema_builder;
  schema_builder.FillPrimitive("cc");
  SchemaReleaseCallback schema_cb(&schema_builder.c_struct_);

  FillPrimitive(3, 0, 0, primitive_buffers_no_nulls1_8);
  ArrayReleaseCallback array_cb(&c_struct_);

  ASSERT_RAISES(Invalid, ImportArray(&c_struct_, &schema_builder.c_struct_));
  schema_cb.AssertCalled();
  array_cb.AssertCalled();
}

TEST_F(TestArrayImport, ImportRecordBatchAndSchema) {
  // Test importing both record batch and its schema at the same time
  auto schema = ::arrow::schema({field("strs", utf8()), field("ints", uint16())});
  auto expected_strs = ArrayFromJSON(utf8(), R"(["", "bar", "quux"])");
  auto expected_ints = ArrayFromJSON(uint16(), "[513, null, 1541]");

  SchemaStructBuilder schema_builder;
  schema_builder.FillPrimitive(schema_builder.AddChild(), "u", "strs");
  schema_builder.FillPrimitive(schema_builder.AddChild(), "S", "ints");
  schema_builder.FillStructLike("+s", 2);
  SchemaReleaseCallback schema_cb(&schema_builder.c_struct_);

  FillStringLike(AddChild(), 3, 0, 1, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, -1, 0, primitive_buffers_nulls1_16);
  FillStructLike(3, 0, 0, 2, buffers_no_nulls_no_data);
  ArrayReleaseCallback array_cb(&c_struct_);

  ASSERT_OK_AND_ASSIGN(auto batch,
                       ImportRecordBatch(&c_struct_, &schema_builder.c_struct_));
  auto expected = RecordBatch::Make(schema, 3, {expected_strs, expected_ints});
  AssertBatchesEqual(*batch, *expected);
  schema_cb.AssertCalled();  // was released
  array_cb.AssertNotCalled();
  ASSERT_TRUE(ArrowArrayIsReleased(&c_struct_));  // was moved
  batch.reset();
  array_cb.AssertCalled();
}

TEST_F(TestArrayImport, ImportRecordBatchAndSchemaError) {
  // On error, both structs are released
  SchemaStructBuilder schema_builder;
  schema_builder.FillPrimitive("cc");
  SchemaReleaseCallback schema_cb(&schema_builder.c_struct_);

  FillStringLike(AddChild(), 3, 0, 1, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), 3, -1, 0, primitive_buffers_nulls1_8);
  FillStructLike(3, 0, 0, 2, buffers_no_nulls_no_data);
  ArrayReleaseCallback array_cb(&c_struct_);

  ASSERT_RAISES(Invalid, ImportRecordBatch(&c_struct_, &schema_builder.c_struct_));
  schema_cb.AssertCalled();
  array_cb.AssertCalled();
}

////////////////////////////////////////////////////////////////////////////
// C++ -> C -> C++ schema roundtripping tests

class TestSchemaRoundtrip : public ::testing::Test {
 public:
  void SetUp() override { pool_ = default_memory_pool(); }

  template <typename TypeFactory, typename ExpectedTypeFactory>
  void TestWithTypeFactory(TypeFactory&& factory,
                           ExpectedTypeFactory&& factory_expected) {
    std::shared_ptr<DataType> type, actual;
    struct ArrowSchema c_schema {};  // zeroed
    SchemaExportGuard schema_guard(&c_schema);

    auto orig_bytes = pool_->bytes_allocated();

    type = factory();
    auto type_use_count = type.use_count();
    ASSERT_OK(ExportType(*type, &c_schema));
    ASSERT_GT(pool_->bytes_allocated(), orig_bytes);
    // Export stores no reference to the type
    ASSERT_EQ(type_use_count, type.use_count());
    type.reset();

    // Recreate the type
    ASSERT_OK_AND_ASSIGN(actual, ImportType(&c_schema));
    type = factory_expected();
    AssertTypeEqual(*type, *actual);
    type.reset();
    actual.reset();

    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename TypeFactory>
  void TestWithTypeFactory(TypeFactory&& factory) {
    TestWithTypeFactory(factory, factory);
  }

  template <typename SchemaFactory>
  void TestWithSchemaFactory(SchemaFactory&& factory) {
    std::shared_ptr<Schema> schema, actual;
    struct ArrowSchema c_schema {};  // zeroed
    SchemaExportGuard schema_guard(&c_schema);

    auto orig_bytes = pool_->bytes_allocated();

    schema = factory();
    auto schema_use_count = schema.use_count();
    ASSERT_OK(ExportSchema(*schema, &c_schema));
    ASSERT_GT(pool_->bytes_allocated(), orig_bytes);
    // Export stores no reference to the schema
    ASSERT_EQ(schema_use_count, schema.use_count());
    schema.reset();

    // Recreate the schema
    ASSERT_OK_AND_ASSIGN(actual, ImportSchema(&c_schema));
    schema = factory();
    AssertSchemaEqual(*schema, *actual);
    schema.reset();
    actual.reset();

    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestSchemaRoundtrip, Null) { TestWithTypeFactory(null); }

TEST_F(TestSchemaRoundtrip, Primitive) {
  TestWithTypeFactory(int32);
  TestWithTypeFactory(boolean);
  TestWithTypeFactory(float16);

  TestWithTypeFactory(std::bind(decimal128, 19, 4));
  TestWithTypeFactory(std::bind(decimal256, 19, 4));
  TestWithTypeFactory(std::bind(decimal128, 19, 0));
  TestWithTypeFactory(std::bind(decimal256, 19, 0));
  TestWithTypeFactory(std::bind(decimal128, 19, -5));
  TestWithTypeFactory(std::bind(decimal256, 19, -5));
  TestWithTypeFactory(std::bind(fixed_size_binary, 3));
  TestWithTypeFactory(binary);
  TestWithTypeFactory(large_utf8);
}

TEST_F(TestSchemaRoundtrip, Temporal) {
  TestWithTypeFactory(date32);
  TestWithTypeFactory(day_time_interval);
  TestWithTypeFactory(month_interval);
  TestWithTypeFactory(month_day_nano_interval);
  TestWithTypeFactory(std::bind(time64, TimeUnit::NANO));
  TestWithTypeFactory(std::bind(duration, TimeUnit::MICRO));
  TestWithTypeFactory([]() { return arrow::timestamp(TimeUnit::MICRO, "Europe/Paris"); });
}

TEST_F(TestSchemaRoundtrip, List) {
  TestWithTypeFactory([]() { return list(utf8()); });
  TestWithTypeFactory([]() { return large_list(list(utf8())); });
  TestWithTypeFactory([]() { return fixed_size_list(utf8(), 5); });
  TestWithTypeFactory([]() { return list(fixed_size_list(utf8(), 5)); });
}

TEST_F(TestSchemaRoundtrip, Struct) {
  auto f1 = field("f1", utf8(), /*nullable=*/false);
  auto f2 = field("f2", list(decimal(19, 4)));

  TestWithTypeFactory([&]() { return struct_({f1, f2}); });
  f2 = f2->WithMetadata(key_value_metadata(kMetadataKeys2, kMetadataValues2));
  TestWithTypeFactory([&]() { return struct_({f1, f2}); });
  TestWithTypeFactory([&]() { return struct_(arrow::FieldVector{}); });
}

TEST_F(TestSchemaRoundtrip, Union) {
  auto f1 = field("f1", utf8(), /*nullable=*/false);
  auto f2 = field("f2", list(decimal(19, 4)));
  auto type_codes = std::vector<int8_t>{42, 43};

  TestWithTypeFactory(
      [&]() { return dense_union(arrow::FieldVector{}, std::vector<int8_t>{}); });
  TestWithTypeFactory(
      [&]() { return sparse_union(arrow::FieldVector{}, std::vector<int8_t>{}); });
  TestWithTypeFactory([&]() { return sparse_union({f1, f2}, type_codes); });
  f2 = f2->WithMetadata(key_value_metadata(kMetadataKeys2, kMetadataValues2));
  TestWithTypeFactory([&]() { return dense_union({f1, f2}, type_codes); });
}

TEST_F(TestSchemaRoundtrip, Dictionary) {
  for (auto index_ty : all_dictionary_index_types()) {
    TestWithTypeFactory([&]() { return dictionary(index_ty, utf8()); });
    TestWithTypeFactory([&]() { return dictionary(index_ty, utf8(), /*ordered=*/true); });
    TestWithTypeFactory([&]() { return dictionary(index_ty, list(utf8())); });
    TestWithTypeFactory([&]() { return list(dictionary(index_ty, list(utf8()))); });
  }
}

TEST_F(TestSchemaRoundtrip, UnregisteredExtension) {
  TestWithTypeFactory(uuid, []() { return fixed_size_binary(16); });
  TestWithTypeFactory(dict_extension_type, []() { return dictionary(int8(), utf8()); });

  // Inside nested type
  TestWithTypeFactory([]() { return list(dict_extension_type()); },
                      []() { return list(dictionary(int8(), utf8())); });
}

TEST_F(TestSchemaRoundtrip, RegisteredExtension) {
  ExtensionTypeGuard guard({uuid(), dict_extension_type(), complex128()});
  TestWithTypeFactory(uuid);
  TestWithTypeFactory(dict_extension_type);
  TestWithTypeFactory(complex128);

  // Inside nested type
  TestWithTypeFactory([]() { return list(uuid()); });
  TestWithTypeFactory([]() { return list(dict_extension_type()); });
  TestWithTypeFactory([]() { return list(complex128()); });
}

TEST_F(TestSchemaRoundtrip, Map) {
  TestWithTypeFactory([&]() { return map(utf8(), int32()); });
  TestWithTypeFactory([&]() { return map(utf8(), field("value", int32(), false)); });
  // Field names are brought in line with the spec on import.
  TestWithTypeFactory(
      [&]() {
        return MapType::Make(field("some_entries",
                                   struct_({field("some_key", utf8(), false),
                                            field("some_value", int32())}),
                                   false))
            .ValueOrDie();
      },
      [&]() { return map(utf8(), int32()); });
  TestWithTypeFactory([&]() { return map(list(utf8()), int32()); });
  TestWithTypeFactory([&]() { return list(map(list(utf8()), int32())); });
}

TEST_F(TestSchemaRoundtrip, Schema) {
  auto f1 = field("f1", utf8(), /*nullable=*/false);
  auto f2 = field("f2", list(decimal256(19, 4)));
  auto md1 = key_value_metadata(kMetadataKeys1, kMetadataValues1);
  auto md2 = key_value_metadata(kMetadataKeys2, kMetadataValues2);

  TestWithSchemaFactory([&]() { return schema({f1, f2}); });
  f2 = f2->WithMetadata(md2);
  TestWithSchemaFactory([&]() { return schema({f1, f2}); });
  TestWithSchemaFactory([&]() { return schema({f1, f2}, md1); });
}

////////////////////////////////////////////////////////////////////////////
// C++ -> C -> C++ data roundtripping tests

class TestArrayRoundtrip : public ::testing::Test {
 public:
  using ArrayFactory = std::function<Result<std::shared_ptr<Array>>()>;

  void SetUp() override { pool_ = default_memory_pool(); }

  static ArrayFactory JSONArrayFactory(std::shared_ptr<DataType> type, const char* json) {
    return [=]() { return ArrayFromJSON(type, json); };
  }

  static ArrayFactory SlicedArrayFactory(ArrayFactory factory) {
    return [=]() -> Result<std::shared_ptr<Array>> {
      ARROW_ASSIGN_OR_RAISE(auto arr, factory());
      DCHECK_GE(arr->length(), 2);
      return arr->Slice(1, arr->length() - 2);
    };
  }

  template <typename ArrayFactory>
  void TestWithArrayFactory(ArrayFactory&& factory) {
    TestWithArrayFactory(factory, factory);
  }

  template <typename ArrayFactory, typename ExpectedArrayFactory>
  void TestWithArrayFactory(ArrayFactory&& factory,
                            ExpectedArrayFactory&& factory_expected) {
    std::shared_ptr<Array> array;
    struct ArrowArray c_array {};
    struct ArrowSchema c_schema {};
    ArrayExportGuard array_guard(&c_array);
    SchemaExportGuard schema_guard(&c_schema);

    auto orig_bytes = pool_->bytes_allocated();

    ASSERT_OK_AND_ASSIGN(array, ToResult(factory()));
    ASSERT_OK(ExportType(*array->type(), &c_schema));
    ASSERT_OK(ExportArray(*array, &c_array));

    auto new_bytes = pool_->bytes_allocated();
    if (array->type_id() != Type::NA) {
      ASSERT_GT(new_bytes, orig_bytes);
    }

    array.reset();
    ASSERT_EQ(pool_->bytes_allocated(), new_bytes);
    ASSERT_OK_AND_ASSIGN(array, ImportArray(&c_array, &c_schema));
    ASSERT_OK(array->ValidateFull());
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_schema));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_array));

    // Re-export and re-import, now both at once
    ASSERT_OK(ExportArray(*array, &c_array, &c_schema));
    array.reset();
    ASSERT_OK_AND_ASSIGN(array, ImportArray(&c_array, &c_schema));
    ASSERT_OK(array->ValidateFull());
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_schema));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_array));

    // Check value of imported array
    {
      std::shared_ptr<Array> expected;
      ASSERT_OK_AND_ASSIGN(expected, ToResult(factory_expected()));
      AssertTypeEqual(*expected->type(), *array->type());
      AssertArraysEqual(*expected, *array, true);
    }
    array.reset();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename BatchFactory>
  void TestWithBatchFactory(BatchFactory&& factory) {
    std::shared_ptr<RecordBatch> batch;
    struct ArrowArray c_array {};
    struct ArrowSchema c_schema {};
    ArrayExportGuard array_guard(&c_array);
    SchemaExportGuard schema_guard(&c_schema);

    auto orig_bytes = pool_->bytes_allocated();
    ASSERT_OK_AND_ASSIGN(batch, ToResult(factory()));
    ASSERT_OK(ExportSchema(*batch->schema(), &c_schema));
    ASSERT_OK(ExportRecordBatch(*batch, &c_array));

    auto new_bytes = pool_->bytes_allocated();
    batch.reset();
    ASSERT_EQ(pool_->bytes_allocated(), new_bytes);
    ASSERT_OK_AND_ASSIGN(batch, ImportRecordBatch(&c_array, &c_schema));
    ASSERT_OK(batch->ValidateFull());
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_schema));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_array));

    // Re-export and re-import, now both at once
    ASSERT_OK(ExportRecordBatch(*batch, &c_array, &c_schema));
    batch.reset();
    ASSERT_OK_AND_ASSIGN(batch, ImportRecordBatch(&c_array, &c_schema));
    ASSERT_OK(batch->ValidateFull());
    ASSERT_TRUE(ArrowSchemaIsReleased(&c_schema));
    ASSERT_TRUE(ArrowArrayIsReleased(&c_array));

    // Check value of imported record batch
    {
      std::shared_ptr<RecordBatch> expected;
      ASSERT_OK_AND_ASSIGN(expected, ToResult(factory()));
      AssertSchemaEqual(*expected->schema(), *batch->schema());
      AssertBatchesEqual(*expected, *batch);
    }
    batch.reset();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  void TestWithJSON(std::shared_ptr<DataType> type, const char* json) {
    TestWithArrayFactory(JSONArrayFactory(type, json));
  }

  void TestWithJSONSliced(std::shared_ptr<DataType> type, const char* json) {
    TestWithArrayFactory(SlicedArrayFactory(JSONArrayFactory(type, json)));
  }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestArrayRoundtrip, Null) {
  TestWithJSON(null(), "[]");
  TestWithJSON(null(), "[null, null]");

  TestWithJSONSliced(null(), "[null, null]");
  TestWithJSONSliced(null(), "[null, null, null]");
}

TEST_F(TestArrayRoundtrip, Primitive) {
  TestWithJSON(int32(), "[]");
  TestWithJSON(int32(), "[4, 5, null]");

  TestWithJSON(decimal128(16, 4), R"(["0.4759", "1234.5670", null])");
  TestWithJSON(decimal256(16, 4), R"(["0.4759", "1234.5670", null])");

  TestWithJSON(month_day_nano_interval(), R"([[1, -600, 5000], null])");

  TestWithJSONSliced(int32(), "[4, 5]");
  TestWithJSONSliced(int32(), "[4, 5, 6, null]");
  TestWithJSONSliced(decimal128(16, 4), R"(["0.4759", "1234.5670", null])");
  TestWithJSONSliced(decimal256(16, 4), R"(["0.4759", "1234.5670", null])");
  TestWithJSONSliced(month_day_nano_interval(),
                     R"([[4, 5, 6], [1, -600, 5000], null, null])");
}

TEST_F(TestArrayRoundtrip, UnknownNullCount) {
  TestWithArrayFactory([]() -> Result<std::shared_ptr<Array>> {
    auto arr = ArrayFromJSON(int32(), "[0, 1, 2]");
    if (arr->null_bitmap()) {
      return Status::Invalid(
          "Failed precondition: "
          "the array shouldn't have a null bitmap.");
    }
    arr->data()->SetNullCount(kUnknownNullCount);
    return arr;
  });
}

TEST_F(TestArrayRoundtrip, List) {
  TestWithJSON(list(int32()), "[]");
  TestWithJSON(list(int32()), "[[4, 5], [6, null], null]");
  TestWithJSON(fixed_size_list(int32(), 3), "[[4, 5, 6], null, [7, 8, null]]");

  TestWithJSONSliced(list(int32()), "[[4, 5], [6, null], null]");
  TestWithJSONSliced(fixed_size_list(int32(), 3), "[[4, 5, 6], null, [7, 8, null]]");
}

TEST_F(TestArrayRoundtrip, Struct) {
  auto type = struct_({field("ints", int16()), field("bools", boolean())});
  TestWithJSON(type, "[]");
  TestWithJSON(type, "[[4, true], [5, false]]");
  TestWithJSON(type, "[[4, null], null, [5, false]]");

  TestWithJSONSliced(type, "[[4, null], null, [5, false]]");

  // With nullable = false and metadata
  auto f0 = field("ints", int16(), /*nullable=*/false);
  auto f1 = field("bools", boolean(), /*nullable=*/true,
                  key_value_metadata(kMetadataKeys1, kMetadataValues1));
  type = struct_({f0, f1});
  TestWithJSON(type, "[]");
  TestWithJSON(type, "[[4, true], [5, null]]");

  TestWithJSONSliced(type, "[[4, true], [5, null], [6, false]]");

  // With no fields
  type = struct_({});
  TestWithJSON(type, "[]");
  TestWithJSON(type, "[[], null, [], null, []]");
  TestWithJSONSliced(type, "[[], null, [], null, []]");
}

TEST_F(TestArrayRoundtrip, Map) {
  // Map type
  auto type = map(utf8(), int32());
  const char* json = R"([[["foo", 123], ["bar", -456]], null,
                        [["foo", null]], []])";
  TestWithJSON(type, json);
  TestWithJSONSliced(type, json);

  type = map(utf8(), int32(), /*keys_sorted=*/true);
  TestWithJSON(type, json);
  TestWithJSONSliced(type, json);
}

TEST_F(TestArrayRoundtrip, Union) {
  FieldVector fields = {field("strs", utf8()), field("ints", int8())};
  std::vector<int8_t> type_codes = {43, 42};
  DataTypeVector union_types = {sparse_union(fields, type_codes),
                                dense_union(fields, type_codes)};
  const char* json = R"([[42, 1], [42, null], [43, "foo"], [43, ""], [42, 3]])";

  for (const auto& type : union_types) {
    TestWithJSON(type, "[]");
    TestWithJSON(type, json);
    TestWithJSONSliced(type, json);
  }

  // With no fields
  fields = {};
  type_codes = {};
  union_types = {sparse_union(fields, type_codes), dense_union(fields, type_codes)};

  for (const auto& type : union_types) {
    TestWithJSON(type, "[]");
  }
}

TEST_F(TestArrayRoundtrip, Dictionary) {
  {
    auto factory = []() {
      auto values = ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                         indices, values);
    };
    TestWithArrayFactory(factory);
    TestWithArrayFactory(SlicedArrayFactory(factory));
  }
  {
    auto factory = []() {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(
          dictionary(indices->type(), values->type(), /*ordered=*/true), indices, values);
    };
    TestWithArrayFactory(factory);
    TestWithArrayFactory(SlicedArrayFactory(factory));
  }
}

TEST_F(TestArrayRoundtrip, RegisteredExtension) {
  ExtensionTypeGuard guard({smallint(), complex128(), dict_extension_type(), uuid()});

  TestWithArrayFactory(ExampleSmallint);
  TestWithArrayFactory(ExampleUuid);
  TestWithArrayFactory(ExampleComplex128);
  TestWithArrayFactory(ExampleDictExtension);

  // Nested inside outer array
  auto NestedFactory = [](ArrayFactory factory) {
    return [factory]() -> Result<std::shared_ptr<Array>> {
      ARROW_ASSIGN_OR_RAISE(auto arr, ToResult(factory()));
      return FixedSizeListArray::FromArrays(arr, /*list_size=*/1);
    };
  };
  TestWithArrayFactory(NestedFactory(ExampleSmallint));
  TestWithArrayFactory(NestedFactory(ExampleUuid));
  TestWithArrayFactory(NestedFactory(ExampleComplex128));
  TestWithArrayFactory(NestedFactory(ExampleDictExtension));
}

TEST_F(TestArrayRoundtrip, UnregisteredExtension) {
  auto StorageExtractor = [](ArrayFactory factory) {
    return [factory]() -> Result<std::shared_ptr<Array>> {
      ARROW_ASSIGN_OR_RAISE(auto arr, ToResult(factory()));
      return checked_cast<const ExtensionArray&>(*arr).storage();
    };
  };

  TestWithArrayFactory(ExampleSmallint, StorageExtractor(ExampleSmallint));
  TestWithArrayFactory(ExampleUuid, StorageExtractor(ExampleUuid));
  TestWithArrayFactory(ExampleComplex128, StorageExtractor(ExampleComplex128));
  TestWithArrayFactory(ExampleDictExtension, StorageExtractor(ExampleDictExtension));
}

TEST_F(TestArrayRoundtrip, RecordBatch) {
  auto schema = ::arrow::schema(
      {field("ints", int16()), field("bools", boolean(), /*nullable=*/false)});
  auto arr0 = ArrayFromJSON(int16(), "[1, 2, null]");
  auto arr1 = ArrayFromJSON(boolean(), "[false, true, false]");

  {
    auto factory = [&]() { return RecordBatch::Make(schema, 3, {arr0, arr1}); };
    TestWithBatchFactory(factory);
  }
  {
    // With schema and field metadata
    auto factory = [&]() {
      auto f0 = schema->field(0);
      auto f1 = schema->field(1);
      f1 = f1->WithMetadata(key_value_metadata(kMetadataKeys1, kMetadataValues1));
      auto schema_with_md =
          ::arrow::schema({f0, f1}, key_value_metadata(kMetadataKeys2, kMetadataValues2));
      return RecordBatch::Make(schema_with_md, 3, {arr0, arr1});
    };
    TestWithBatchFactory(factory);
  }
}

// TODO C -> C++ -> C roundtripping tests?

////////////////////////////////////////////////////////////////////////////
// Array stream export tests

class FailingRecordBatchReader : public RecordBatchReader {
 public:
  explicit FailingRecordBatchReader(Status error) : error_(std::move(error)) {}

  static std::shared_ptr<Schema> expected_schema() { return arrow::schema({}); }

  std::shared_ptr<Schema> schema() const override { return expected_schema(); }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override { return error_; }

 protected:
  Status error_;
};

class BaseArrayStreamTest : public ::testing::Test {
 public:
  void SetUp() override {
    pool_ = default_memory_pool();
    orig_allocated_ = pool_->bytes_allocated();
  }

  void TearDown() override { ASSERT_EQ(pool_->bytes_allocated(), orig_allocated_); }

  RecordBatchVector MakeBatches(std::shared_ptr<Schema> schema, ArrayVector arrays) {
    DCHECK_EQ(schema->num_fields(), 1);
    RecordBatchVector batches;
    for (const auto& array : arrays) {
      batches.push_back(RecordBatch::Make(schema, array->length(), {array}));
    }
    return batches;
  }

 protected:
  MemoryPool* pool_;
  int64_t orig_allocated_;
};

class TestArrayStreamExport : public BaseArrayStreamTest {
 public:
  void AssertStreamSchema(struct ArrowArrayStream* c_stream, const Schema& expected) {
    struct ArrowSchema c_schema;
    ASSERT_EQ(0, c_stream->get_schema(c_stream, &c_schema));

    SchemaExportGuard schema_guard(&c_schema);
    ASSERT_FALSE(ArrowSchemaIsReleased(&c_schema));
    ASSERT_OK_AND_ASSIGN(auto schema, ImportSchema(&c_schema));
    AssertSchemaEqual(expected, *schema);
  }

  void AssertStreamEnd(struct ArrowArrayStream* c_stream) {
    struct ArrowArray c_array;
    ASSERT_EQ(0, c_stream->get_next(c_stream, &c_array));

    ArrayExportGuard guard(&c_array);
    ASSERT_TRUE(ArrowArrayIsReleased(&c_array));
  }

  void AssertStreamNext(struct ArrowArrayStream* c_stream, const RecordBatch& expected) {
    struct ArrowArray c_array;
    ASSERT_EQ(0, c_stream->get_next(c_stream, &c_array));

    ArrayExportGuard guard(&c_array);
    ASSERT_FALSE(ArrowArrayIsReleased(&c_array));

    ASSERT_OK_AND_ASSIGN(auto batch, ImportRecordBatch(&c_array, expected.schema()));
    AssertBatchesEqual(expected, *batch);
  }
};

TEST_F(TestArrayStreamExport, Empty) {
  auto schema = arrow::schema({field("ints", int32())});
  auto batches = MakeBatches(schema, {});
  ASSERT_OK_AND_ASSIGN(auto reader, RecordBatchReader::Make(batches, schema));

  struct ArrowArrayStream c_stream;

  ASSERT_OK(ExportRecordBatchReader(reader, &c_stream));
  ArrayStreamExportGuard guard(&c_stream);

  ASSERT_FALSE(ArrowArrayStreamIsReleased(&c_stream));
  AssertStreamSchema(&c_stream, *schema);
  AssertStreamEnd(&c_stream);
  AssertStreamEnd(&c_stream);
}

TEST_F(TestArrayStreamExport, Simple) {
  auto schema = arrow::schema({field("ints", int32())});
  auto batches = MakeBatches(
      schema, {ArrayFromJSON(int32(), "[1, 2]"), ArrayFromJSON(int32(), "[4, 5, null]")});
  ASSERT_OK_AND_ASSIGN(auto reader, RecordBatchReader::Make(batches, schema));

  struct ArrowArrayStream c_stream;

  ASSERT_OK(ExportRecordBatchReader(reader, &c_stream));
  ArrayStreamExportGuard guard(&c_stream);

  ASSERT_FALSE(ArrowArrayStreamIsReleased(&c_stream));
  AssertStreamSchema(&c_stream, *schema);
  AssertStreamNext(&c_stream, *batches[0]);
  AssertStreamNext(&c_stream, *batches[1]);
  AssertStreamEnd(&c_stream);
  AssertStreamEnd(&c_stream);
}

TEST_F(TestArrayStreamExport, ArrayLifetime) {
  auto schema = arrow::schema({field("ints", int32())});
  auto batches = MakeBatches(
      schema, {ArrayFromJSON(int32(), "[1, 2]"), ArrayFromJSON(int32(), "[4, 5, null]")});
  ASSERT_OK_AND_ASSIGN(auto reader, RecordBatchReader::Make(batches, schema));

  struct ArrowArrayStream c_stream;
  struct ArrowSchema c_schema;
  struct ArrowArray c_array0, c_array1;

  ASSERT_OK(ExportRecordBatchReader(reader, &c_stream));
  {
    ArrayStreamExportGuard guard(&c_stream);
    ASSERT_FALSE(ArrowArrayStreamIsReleased(&c_stream));

    ASSERT_EQ(0, c_stream.get_schema(&c_stream, &c_schema));
    ASSERT_EQ(0, c_stream.get_next(&c_stream, &c_array0));
    ASSERT_EQ(0, c_stream.get_next(&c_stream, &c_array1));
    AssertStreamEnd(&c_stream);
  }

  ArrayExportGuard guard0(&c_array0), guard1(&c_array1);

  {
    SchemaExportGuard schema_guard(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto got_schema, ImportSchema(&c_schema));
    AssertSchemaEqual(*schema, *got_schema);
  }

  ASSERT_GT(pool_->bytes_allocated(), orig_allocated_);
  ASSERT_OK_AND_ASSIGN(auto batch, ImportRecordBatch(&c_array1, schema));
  AssertBatchesEqual(*batches[1], *batch);
  ASSERT_OK_AND_ASSIGN(batch, ImportRecordBatch(&c_array0, schema));
  AssertBatchesEqual(*batches[0], *batch);
}

TEST_F(TestArrayStreamExport, Errors) {
  auto reader =
      std::make_shared<FailingRecordBatchReader>(Status::Invalid("some example error"));

  struct ArrowArrayStream c_stream;

  ASSERT_OK(ExportRecordBatchReader(reader, &c_stream));
  ArrayStreamExportGuard guard(&c_stream);

  struct ArrowSchema c_schema;
  ASSERT_EQ(0, c_stream.get_schema(&c_stream, &c_schema));
  ASSERT_FALSE(ArrowSchemaIsReleased(&c_schema));
  {
    SchemaExportGuard schema_guard(&c_schema);
    ASSERT_OK_AND_ASSIGN(auto schema, ImportSchema(&c_schema));
    AssertSchemaEqual(schema, arrow::schema({}));
  }

  struct ArrowArray c_array;
  ASSERT_EQ(EINVAL, c_stream.get_next(&c_stream, &c_array));
}

////////////////////////////////////////////////////////////////////////////
// Array stream roundtrip tests

class TestArrayStreamRoundtrip : public BaseArrayStreamTest {
 public:
  void Roundtrip(std::shared_ptr<RecordBatchReader>* reader,
                 struct ArrowArrayStream* c_stream) {
    ASSERT_OK(ExportRecordBatchReader(*reader, c_stream));
    ASSERT_FALSE(ArrowArrayStreamIsReleased(c_stream));

    ASSERT_OK_AND_ASSIGN(auto got_reader, ImportRecordBatchReader(c_stream));
    *reader = std::move(got_reader);
  }

  void Roundtrip(
      std::shared_ptr<RecordBatchReader> reader,
      std::function<void(const std::shared_ptr<RecordBatchReader>&)> check_func) {
    ArrowArrayStream c_stream;

    // NOTE: ReleaseCallback<> is not immediately usable with ArrowArrayStream,
    // because get_next and get_schema need the original private_data.
    std::weak_ptr<RecordBatchReader> weak_reader(reader);
    ASSERT_EQ(weak_reader.use_count(), 1);  // Expiration check will fail otherwise

    ASSERT_OK(ExportRecordBatchReader(std::move(reader), &c_stream));
    ASSERT_FALSE(ArrowArrayStreamIsReleased(&c_stream));

    {
      ASSERT_OK_AND_ASSIGN(auto new_reader, ImportRecordBatchReader(&c_stream));
      // Stream was moved
      ASSERT_TRUE(ArrowArrayStreamIsReleased(&c_stream));
      ASSERT_FALSE(weak_reader.expired());

      check_func(new_reader);
    }
    // Stream was released when `new_reader` was destroyed
    ASSERT_TRUE(weak_reader.expired());
  }

  void AssertReaderNext(const std::shared_ptr<RecordBatchReader>& reader,
                        const RecordBatch& expected) {
    ASSERT_OK_AND_ASSIGN(auto batch, reader->Next());
    ASSERT_NE(batch, nullptr);
    AssertBatchesEqual(expected, *batch);
  }

  void AssertReaderEnd(const std::shared_ptr<RecordBatchReader>& reader) {
    ASSERT_OK_AND_ASSIGN(auto batch, reader->Next());
    ASSERT_EQ(batch, nullptr);
  }

  void AssertReaderClosed(const std::shared_ptr<RecordBatchReader>& reader) {
    ASSERT_THAT(reader->Next(),
                Raises(StatusCode::Invalid, ::testing::HasSubstr("already been closed")));
  }

  void AssertReaderClose(const std::shared_ptr<RecordBatchReader>& reader) {
    ASSERT_OK(reader->Close());
    AssertReaderClosed(reader);
  }
};

TEST_F(TestArrayStreamRoundtrip, Simple) {
  auto orig_schema = arrow::schema({field("ints", int32())});
  auto batches = MakeBatches(orig_schema, {ArrayFromJSON(int32(), "[1, 2]"),
                                           ArrayFromJSON(int32(), "[4, 5, null]")});

  ASSERT_OK_AND_ASSIGN(auto reader, RecordBatchReader::Make(batches, orig_schema));

  Roundtrip(std::move(reader), [&](const std::shared_ptr<RecordBatchReader>& reader) {
    AssertSchemaEqual(*orig_schema, *reader->schema());
    AssertReaderNext(reader, *batches[0]);
    AssertReaderNext(reader, *batches[1]);
    AssertReaderEnd(reader);
    AssertReaderEnd(reader);
    AssertReaderClose(reader);
  });
}

TEST_F(TestArrayStreamRoundtrip, CloseEarly) {
  auto orig_schema = arrow::schema({field("ints", int32())});
  auto batches = MakeBatches(orig_schema, {ArrayFromJSON(int32(), "[1, 2]"),
                                           ArrayFromJSON(int32(), "[4, 5, null]")});

  ASSERT_OK_AND_ASSIGN(auto reader, RecordBatchReader::Make(batches, orig_schema));

  Roundtrip(std::move(reader), [&](const std::shared_ptr<RecordBatchReader>& reader) {
    AssertReaderNext(reader, *batches[0]);
    AssertReaderClose(reader);
  });
}

TEST_F(TestArrayStreamRoundtrip, Errors) {
  auto reader = std::make_shared<FailingRecordBatchReader>(
      Status::Invalid("roundtrip error example"));

  Roundtrip(std::move(reader), [&](const std::shared_ptr<RecordBatchReader>& reader) {
    auto status = reader->Next().status();
    ASSERT_RAISES(Invalid, status);
    ASSERT_THAT(status.message(), ::testing::HasSubstr("roundtrip error example"));
  });
}

TEST_F(TestArrayStreamRoundtrip, SchemaError) {
  struct StreamState {
    bool released = false;

    static const char* GetLastError(struct ArrowArrayStream* stream) {
      return "Expected error";
    }

    static int GetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* schema) {
      return EIO;
    }

    static int GetNext(struct ArrowArrayStream* stream, struct ArrowArray* array) {
      return EINVAL;
    }

    static void Release(struct ArrowArrayStream* stream) {
      reinterpret_cast<StreamState*>(stream->private_data)->released = true;
      std::memset(stream, 0, sizeof(*stream));
    }
  } state;
  struct ArrowArrayStream stream = {};
  stream.get_last_error = &StreamState::GetLastError;
  stream.get_schema = &StreamState::GetSchema;
  stream.get_next = &StreamState::GetNext;
  stream.release = &StreamState::Release;
  stream.private_data = &state;

  EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, ::testing::HasSubstr("Expected error"),
                                  ImportRecordBatchReader(&stream));
  ASSERT_TRUE(state.released);
}

}  // namespace arrow
