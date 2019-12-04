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

#include <deque>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"

namespace arrow {

class ExportGuard {
 public:
  explicit ExportGuard(struct ArrowArray* c_export) : c_export_(c_export) {}

  ~ExportGuard() { Release(); }

  void Release() {
    if (c_export_) {
      ArrowReleaseArray(c_export_);
      c_export_ = nullptr;
    }
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ExportGuard);

  struct ArrowArray* c_export_;
};

class ReleaseCallback {
 public:
  explicit ReleaseCallback(struct ArrowArray* c_struct) : called_(false) {
    orig_release_ = c_struct->release;
    orig_private_data_ = c_struct->private_data;
    c_struct->release = ReleaseUnbound;
    c_struct->private_data = this;
  }

  static void ReleaseUnbound(struct ArrowArray* c_struct) {
    reinterpret_cast<ReleaseCallback*>(c_struct->private_data)->Release(c_struct);
  }

  void Release(struct ArrowArray* c_struct) {
    ASSERT_FALSE(called_) << "ReleaseCallback called twice";
    called_ = true;
    ASSERT_NE(c_struct->format, nullptr)
        << "ReleaseCallback called with released ArrowArray";
    // Call original release callback
    c_struct->release = orig_release_;
    c_struct->private_data = orig_private_data_;
    ArrowReleaseArray(c_struct);
  }

  void AssertCalled() { ASSERT_TRUE(called_) << "ReleaseCallback was not called"; }

  void AssertNotCalled() { ASSERT_FALSE(called_) << "ReleaseCallback was called"; }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ReleaseCallback);

  bool called_;
  void (*orig_release_)(struct ArrowArray*);
  void* orig_private_data_;
};

static const std::vector<std::string> kMetadataKeys1{"key1", "key2"};
static const std::vector<std::string> kMetadataValues1{"", "bar"};
// clang-format off
static const std::string kEncodedMetadata1{  // NOLINT: runtime/string
    2, 0, 0, 0,
    4, 0, 0, 0, 'k', 'e', 'y', '1', 0, 0, 0, 0,
    4, 0, 0, 0, 'k', 'e', 'y', '2', 3, 0, 0, 0, 'b', 'a', 'r'};
// clang-format off

static const std::vector<std::string> kMetadataKeys2{"key"};
static const std::vector<std::string> kMetadataValues2{"abcde"};
// clang-format off
static const std::string kEncodedMetadata2{  // NOLINT: runtime/string
    1, 0, 0, 0,
    3, 0, 0, 0, 'k', 'e', 'y', 5, 0, 0, 0, 'a', 'b', 'c', 'd', 'e'};
// clang-format off

////////////////////////////////////////////////////////////////////////////
// Export tests

static constexpr int64_t kDefaultFlags = ARROW_FLAG_NULLABLE;

struct ExportChecker {
  ExportChecker(std::vector<std::string> flattened_formats,
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

  void operator()(struct ArrowArray* c_export, const ArrayData& expected_data,
                  bool inner = false) {
    ASSERT_LT(flattened_index_, flattened_formats_.size());
    ASSERT_LT(flattened_index_, flattened_names_.size());
    ASSERT_LT(flattened_index_, flattened_flags_.size());
    ASSERT_EQ(std::string(c_export->format), flattened_formats_[flattened_index_]);
    ASSERT_EQ(std::string(c_export->name), flattened_names_[flattened_index_]);
    if (!flattened_metadata_.empty()) {
      const auto& expected_md = flattened_metadata_[flattened_index_];
      ASSERT_EQ(std::string(c_export->metadata, expected_md.size()), expected_md);
    } else {
      ASSERT_EQ(c_export->metadata, nullptr);
    }
    ASSERT_EQ(c_export->flags, flattened_flags_[flattened_index_]);
    ++flattened_index_;

    ASSERT_EQ(c_export->length, expected_data.length);
    ASSERT_EQ(c_export->null_count, expected_data.null_count);
    ASSERT_EQ(c_export->offset, expected_data.offset);

    ASSERT_EQ(c_export->n_buffers, static_cast<int64_t>(expected_data.buffers.size()));
    ASSERT_EQ(c_export->n_children,
              static_cast<int64_t>(expected_data.child_data.size()));
    ASSERT_NE(c_export->buffers, nullptr);
    for (int64_t i = 0; i < c_export->n_buffers; ++i) {
      auto expected_ptr =
          expected_data.buffers[i] ? expected_data.buffers[i]->data() : nullptr;
      ASSERT_EQ(c_export->buffers[i], expected_ptr);
    }

    if (expected_data.dictionary != nullptr) {
      // Recurse into dictionary
      ASSERT_NE(c_export->dictionary, nullptr);
      operator()(c_export->dictionary, *expected_data.dictionary->data(), true);
    } else {
      ASSERT_EQ(c_export->dictionary, nullptr);
    }

    if (c_export->n_children > 0) {
      ASSERT_NE(c_export->children, nullptr);
      // Recurse into children
      for (int64_t i = 0; i < c_export->n_children; ++i) {
        ASSERT_NE(c_export->children[i], nullptr);
        operator()(c_export->children[i], *expected_data.child_data[i], true);
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

class TestExport : public ::testing::Test {
 public:
  void SetUp() override { pool_ = default_memory_pool(); }

  static std::function<Status(std::shared_ptr<Array>*)> JSONArrayFactory(
      std::shared_ptr<DataType> type, const char* json) {
    return [=](std::shared_ptr<Array>* out) -> Status {
      return ::arrow::ipc::internal::json::ArrayFromJSON(type, json, out);
    };
  }

  template <typename ArrayFactory, typename ExportCheckFunc>
  void TestWithArrayFactory(ArrayFactory&& factory, ExportCheckFunc&& func) {
    auto orig_bytes = pool_->bytes_allocated();

    std::shared_ptr<Array> arr;
    ASSERT_OK(factory(&arr));
    const ArrayData& data = *arr->data();  // non-owning reference
    struct ArrowArray c_export;
    ASSERT_OK(ExportArray(*arr, &c_export));

    ExportGuard guard(&c_export);
    auto new_bytes = pool_->bytes_allocated();
    ASSERT_GT(new_bytes, orig_bytes);

    // Release the shared_ptr<Array>, underlying data should be held alive
    arr.reset();
    ASSERT_EQ(pool_->bytes_allocated(), new_bytes);
    func(&c_export, data);

    // Release the ArrowArray, underlying data should be destroyed
    guard.Release();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename ArrayFactory>
  void TestNested(ArrayFactory&& factory, std::vector<std::string> flattened_formats,
                  std::vector<std::string> flattened_names,
                  std::vector<int64_t> flattened_flags = {},
                  std::vector<std::string> flattened_metadata = {}) {
    ExportChecker checker(std::move(flattened_formats), std::move(flattened_names),
                          std::move(flattened_flags), std::move(flattened_metadata));

    TestWithArrayFactory(std::move(factory), checker);
  }

  void TestNested(const std::shared_ptr<DataType>& type, const char* json,
                  std::vector<std::string> flattened_formats,
                  std::vector<std::string> flattened_names,
                  std::vector<int64_t> flattened_flags = {},
                  std::vector<std::string> flattened_metadata = {}) {
    TestNested(JSONArrayFactory(type, json), std::move(flattened_formats),
               std::move(flattened_names), std::move(flattened_flags),
               std::move(flattened_metadata));
  }

  template <typename ArrayFactory>
  void TestPrimitive(ArrayFactory&& factory, const char* format) {
    TestNested(std::forward<ArrayFactory>(factory), {format}, {""});
  }

  void TestPrimitive(const std::shared_ptr<DataType>& type, const char* json,
                     const char* format) {
    TestNested(type, json, {format}, {""});
  }

  template <typename ArrayFactory, typename ExportCheckFunc>
  void TestMoveWithArrayFactory(ArrayFactory&& factory, ExportCheckFunc&& func) {
    auto orig_bytes = pool_->bytes_allocated();

    std::shared_ptr<Array> arr;
    ASSERT_OK(factory(&arr));
    const ArrayData& data = *arr->data();  // non-owning reference
    struct ArrowArray c_export_temp, c_export_final;
    ASSERT_OK(ExportArray(*arr, &c_export_temp));

    // Move the ArrowArray to its final location
    ArrowMoveArray(&c_export_temp, &c_export_final);
    ASSERT_EQ(c_export_temp.format, nullptr);  // released

    ExportGuard guard(&c_export_final);
    auto new_bytes = pool_->bytes_allocated();
    ASSERT_GT(new_bytes, orig_bytes);

    // Release the shared_ptr<Array>, underlying data should be held alive
    arr.reset();
    ASSERT_EQ(pool_->bytes_allocated(), new_bytes);
    func(&c_export_final, data);

    // Release the ArrowArray, underlying data should be destroyed
    guard.Release();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename ArrayFactory>
  void TestMoveNested(ArrayFactory&& factory, std::vector<std::string> flattened_formats,
                      std::vector<std::string> flattened_names) {
    ExportChecker checker(std::move(flattened_formats), std::move(flattened_names));

    TestMoveWithArrayFactory(std::move(factory), checker);
  }

  void TestMoveNested(const std::shared_ptr<DataType>& type, const char* json,
                      std::vector<std::string> flattened_formats,
                      std::vector<std::string> flattened_names) {
    TestMoveNested(JSONArrayFactory(type, json), std::move(flattened_formats),
                   std::move(flattened_names));
  }

  void TestMovePrimitive(const std::shared_ptr<DataType>& type, const char* json,
                         const char* format) {
    TestMoveNested(type, json, {format}, {""});
  }

  template <typename ArrayFactory, typename ExportCheckFunc>
  void TestMoveChildWithArrayFactory(ArrayFactory&& factory, int64_t child_id,
                                     ExportCheckFunc&& func) {
    auto orig_bytes = pool_->bytes_allocated();

    std::shared_ptr<Array> arr;
    ASSERT_OK(factory(&arr));
    struct ArrowArray c_export_parent, c_export_child;
    ASSERT_OK(ExportArray(*arr, &c_export_parent));

    auto bytes_with_parent = pool_->bytes_allocated();
    ASSERT_GT(bytes_with_parent, orig_bytes);

    // Move the child ArrowArray to its final location
    {
      ExportGuard parent_guard(&c_export_parent);
      ASSERT_LT(child_id, c_export_parent.n_children);
      ArrowMoveArray(c_export_parent.children[child_id], &c_export_child);
    }
    ExportGuard child_guard(&c_export_child);

    // Now parent is released
    ASSERT_EQ(c_export_parent.format, nullptr);
    auto bytes_with_child = pool_->bytes_allocated();
    ASSERT_LT(bytes_with_child, bytes_with_parent);
    ASSERT_GT(bytes_with_child, orig_bytes);

    // Release the shared_ptr<Array>, some underlying data should be held alive
    const ArrayData& data = *arr->data()->child_data[child_id];  // non-owning reference
    arr.reset();
    ASSERT_LT(pool_->bytes_allocated(), bytes_with_child);
    ASSERT_GT(pool_->bytes_allocated(), orig_bytes);
    func(&c_export_child, data);

    // Release the ArrowArray, underlying data should be destroyed
    child_guard.Release();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename ArrayFactory>
  void TestMoveChild(ArrayFactory&& factory, int64_t child_id,
                     std::vector<std::string> flattened_formats,
                     std::vector<std::string> flattened_names) {
    ExportChecker checker(std::move(flattened_formats), std::move(flattened_names));

    TestMoveChildWithArrayFactory(std::move(factory), child_id, checker);
  }

  void TestMoveChild(const std::shared_ptr<DataType>& type, const char* json,
                     int64_t child_id, std::vector<std::string> flattened_formats,
                     std::vector<std::string> flattened_names) {
    TestMoveChild(JSONArrayFactory(type, json), child_id, std::move(flattened_formats),
                  std::move(flattened_names));
  }

 protected:
  MemoryPool* pool_;
};

TEST_F(TestExport, Primitive) {
  TestPrimitive(int8(), "[1, 2, null, -3]", "c");
  TestPrimitive(int16(), "[1, 2, -3]", "s");
  TestPrimitive(int32(), "[1, 2, null, -3]", "i");
  TestPrimitive(int64(), "[1, 2, -3]", "l");
  TestPrimitive(uint8(), "[1, 2, 3]", "C");
  TestPrimitive(uint16(), "[1, 2, null, 3]", "S");
  TestPrimitive(uint32(), "[1, 2, 3]", "I");
  TestPrimitive(uint64(), "[1, 2, null, 3]", "L");

  TestPrimitive(boolean(), "[true, false, null]", "b");
  TestPrimitive(null(), "[null, null]", "n");

  TestPrimitive(float32(), "[1.5, null]", "f");
  TestPrimitive(float64(), "[1.5, null]", "g");

  TestPrimitive(fixed_size_binary(3), R"(["foo", "bar", null])", "w:3");
  TestPrimitive(binary(), R"(["foo", "bar", null])", "z");
  TestPrimitive(large_binary(), R"(["foo", "bar", null])", "Z");
  TestPrimitive(utf8(), R"(["foo", "bar", null])", "u");
  TestPrimitive(large_utf8(), R"(["foo", "bar", null])", "U");

  TestPrimitive(decimal(16, 4), R"(["1234.5670", null])", "d:16,4");
}

TEST_F(TestExport, PrimitiveSliced) {
  auto factory = [](std::shared_ptr<Array>* out) -> Status {
    *out = ArrayFromJSON(int16(), "[1, 2, null, -3]")->Slice(1, 2);
    return Status::OK();
  };

  TestPrimitive(factory, "s");
}

TEST_F(TestExport, Null) {
  TestPrimitive(null(), "[null, null, null]", "n");
  TestPrimitive(null(), "[]", "n");
}

TEST_F(TestExport, Temporal) {
  const char* json = "[1, 2, null, 42]";
  TestPrimitive(date32(), json, "tdD");
  TestPrimitive(date64(), json, "tdm");
  TestPrimitive(time32(TimeUnit::SECOND), json, "tts");
  TestPrimitive(time32(TimeUnit::MILLI), json, "ttm");
  TestPrimitive(time64(TimeUnit::MICRO), json, "ttu");
  TestPrimitive(time64(TimeUnit::NANO), json, "ttn");
  TestPrimitive(duration(TimeUnit::SECOND), json, "tDs");
  TestPrimitive(duration(TimeUnit::MILLI), json, "tDm");
  TestPrimitive(duration(TimeUnit::MICRO), json, "tDu");
  TestPrimitive(duration(TimeUnit::NANO), json, "tDn");
  TestPrimitive(month_interval(), json, "tiM");

  TestPrimitive(day_time_interval(), "[[7, 600], null]", "tiD");

  json = R"(["1970-01-01","2000-02-29","1900-02-28"])";
  TestPrimitive(timestamp(TimeUnit::SECOND), json, "tss:");
  TestPrimitive(timestamp(TimeUnit::SECOND, "Europe/Paris"), json, "tss:Europe/Paris");
  TestPrimitive(timestamp(TimeUnit::MILLI), json, "tsm:");
  TestPrimitive(timestamp(TimeUnit::MILLI, "Europe/Paris"), json, "tsm:Europe/Paris");
  TestPrimitive(timestamp(TimeUnit::MICRO), json, "tsu:");
  TestPrimitive(timestamp(TimeUnit::MICRO, "Europe/Paris"), json, "tsu:Europe/Paris");
  TestPrimitive(timestamp(TimeUnit::NANO), json, "tsn:");
  TestPrimitive(timestamp(TimeUnit::NANO, "Europe/Paris"), json, "tsn:Europe/Paris");
}

TEST_F(TestExport, List) {
  TestNested(list(int8()), "[[1, 2], [3, null], null]", {"+l", "c"}, {"", "item"});
  TestNested(large_list(uint16()), "[[1, 2], [3, null], null]", {"+L", "S"},
             {"", "item"});
  TestNested(fixed_size_list(int64(), 2), "[[1, 2], [3, null], null]", {"+w:2", "l"},
             {"", "item"});

  TestNested(list(large_list(int32())), "[[[1, 2], [3], null], null]", {"+l", "+L", "i"},
             {"", "item", "item"});
}

TEST_F(TestExport, ListSliced) {
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      *out = ArrayFromJSON(list(int8()), "[[1, 2], [3, null], [4, 5, 6], null]")
                 ->Slice(1, 2);
      return Status::OK();
    };
    TestNested(factory, {"+l", "c"}, {"", "item"});
  }
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(int16(), "[1, 2, 3, 4, null, 5, 6, 7, 8]")->Slice(1, 6);
      auto offsets = ArrayFromJSON(int32(), "[0, 2, 3, 5, 6]")->Slice(2, 4);
      return ListArray::FromArrays(*offsets, *values, default_memory_pool(), out);
    };
    TestNested(factory, {"+l", "s"}, {"", "item"});
  }
}

TEST_F(TestExport, Struct) {
  const char* data = R"([[1, "foo"], [2, null]])";
  auto type = struct_({field("a", int8()), field("b", utf8())});
  TestNested(type, data, {"+s", "c", "u"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE});

  // With nullable = false
  type = struct_({field("a", int8(), /*nullable=*/false), field("b", utf8())});
  TestNested(type, data, {"+s", "c", "u"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, 0, ARROW_FLAG_NULLABLE});

  // With metadata
  auto f0 = type->child(0);
  auto f1 = type->child(1)->WithMetadata(
      key_value_metadata(kMetadataKeys1, kMetadataValues1));
  type = struct_({f0, f1});
  TestNested(type, data, {"+s", "c", "u"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, 0, ARROW_FLAG_NULLABLE},
             {"", "", kEncodedMetadata1});
}

TEST_F(TestExport, Map) {
  const char* json = R"([[[1, "foo"], [2, null]], [[3, "bar"]]])";
  TestNested(map(int8(), utf8()), json,
             {"+m", "+s", "c", "u"}, {"", "entries", "key", "value"},
             {ARROW_FLAG_NULLABLE, 0, 0, ARROW_FLAG_NULLABLE});
  TestNested(map(int8(), utf8(), /*keys_sorted=*/ true), json,
             {"+m", "+s", "c", "u"}, {"", "entries", "key", "value"},
             {ARROW_FLAG_NULLABLE | ARROW_FLAG_MAP_KEYS_SORTED, 0, 0,
              ARROW_FLAG_NULLABLE});
}

TEST_F(TestExport, Union) {
  const char* data = "[null, [42, 1], [43, true], [42, null], [42, 2]]";
  // Dense
  auto field_a = field("a", int8());
  auto field_b = field("b", boolean(), /*nullable=*/false);
  auto type = union_({field_a, field_b}, {42, 43}, UnionMode::DENSE);
  TestNested(type, data, {"+ud:42,43", "c", "b"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE, 0});
  // Sparse
  field_a = field("a", int8(), /*nullable=*/false);
  field_b = field("b", boolean());
  type = union_({field_a, field_b}, {42, 43}, UnionMode::SPARSE);
  TestNested(type, data, {"+us:42,43", "c", "b"}, {"", "a", "b"},
             {ARROW_FLAG_NULLABLE, 0, ARROW_FLAG_NULLABLE});
}

TEST_F(TestExport, Dictionary) {
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                         indices, values, out);
    };
    TestNested(factory, {"i", "u"}, {"", ""});
  }
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(
          dictionary(indices->type(), values->type(), /*ordered=*/true), indices, values,
          out);
    };
    TestNested(factory, {"i", "+l", "u"}, {"", "", "item"},
               {ARROW_FLAG_NULLABLE | ARROW_FLAG_DICTIONARY_ORDERED, ARROW_FLAG_NULLABLE,
                ARROW_FLAG_NULLABLE});
  }
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      std::shared_ptr<Array> dict_array;
      RETURN_NOT_OK(DictionaryArray::FromArrays(
          dictionary(indices->type(), values->type()), indices, values, &dict_array));
      auto offsets = ArrayFromJSON(int64(), "[0, 2, 5]");
      RETURN_NOT_OK(
          LargeListArray::FromArrays(*offsets, *dict_array, default_memory_pool(), out));
      return (*out)->ValidateFull();
    };
    TestNested(factory, {"+L", "i", "+l", "u"}, {"", "item", "", "item"});
  }
}

TEST_F(TestExport, MovePrimitive) {
  TestMovePrimitive(int8(), "[1, 2, null, -3]", "c");
  TestMovePrimitive(fixed_size_binary(3), R"(["foo", "bar", null])", "w:3");
  TestMovePrimitive(binary(), R"(["foo", "bar", null])", "z");
}

TEST_F(TestExport, MoveNested) {
  TestMoveNested(list(int8()), "[[1, 2], [3, null], null]", {"+l", "c"}, {"", "item"});
  TestMoveNested(list(large_list(int32())), "[[[1, 2], [3], null], null]",
                 {"+l", "+L", "i"}, {"", "item", "item"});
  TestMoveNested(struct_({field("a", int8()), field("b", utf8())}),
                 R"([[1, "foo"], [2, null]])", {"+s", "c", "u"}, {"", "a", "b"});
}

TEST_F(TestExport, MoveDictionary) {
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                         indices, values, out);
    };
    TestMoveNested(factory, {"i", "u"}, {"", ""});
  }
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      std::shared_ptr<Array> dict_array;
      RETURN_NOT_OK(DictionaryArray::FromArrays(
          dictionary(indices->type(), values->type()), indices, values, &dict_array));
      auto offsets = ArrayFromJSON(int64(), "[0, 2, 5]");
      RETURN_NOT_OK(
          LargeListArray::FromArrays(*offsets, *dict_array, default_memory_pool(), out));
      return (*out)->ValidateFull();
    };
    TestMoveNested(factory, {"+L", "i", "+l", "u"}, {"", "item", "", "item"});
  }
}

TEST_F(TestExport, MoveChild) {
  TestMoveChild(list(int8()), "[[1, 2], [3, null], null]", /*child_id=*/0, {"c"},
                {"item"});
  TestMoveChild(list(large_list(int32())), "[[[1, 2], [3], null], null]",
                /*child_id=*/0, {"+L", "i"}, {"item", "item"});
  TestMoveChild(struct_({field("ints", int8()), field("strs", utf8())}),
                R"([[1, "foo"], [2, null]])",
                /*child_id=*/0, {"c"}, {"ints"});
  TestMoveChild(struct_({field("ints", int8()), field("strs", utf8())}),
                R"([[1, "foo"], [2, null]])",
                /*child_id=*/1, {"u"}, {"strs"});
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      std::shared_ptr<Array> dict_array;
      RETURN_NOT_OK(DictionaryArray::FromArrays(
          dictionary(indices->type(), values->type()), indices, values, &dict_array));
      auto offsets = ArrayFromJSON(int64(), "[0, 2, 5]");
      RETURN_NOT_OK(
          LargeListArray::FromArrays(*offsets, *dict_array, default_memory_pool(), out));
      return (*out)->ValidateFull();
    };
    TestMoveChild(factory, /*child_id=*/0, {"i", "+l", "u"}, {"item", "", "item"});
  }
}

TEST_F(TestExport, WithField) {
  struct ArrowArray c_export;
  {
    auto arr = ArrayFromJSON(null(), "[null, null, null]");
    auto f = field("thing", null());
    ASSERT_OK(ExportArray(*f, *arr, &c_export));

    ExportGuard guard(&c_export);
    const auto& data = *arr->data();
    arr.reset();
    f.reset();
    ExportChecker({"n"}, {"thing"}, {ARROW_FLAG_NULLABLE})(&c_export, data);
  }
  {
    // With nullable = false
    auto arr = ArrayFromJSON(null(), "[null, null, null]");
    auto f = field("thing", null(), /*nullable=*/false);
    ASSERT_OK(ExportArray(*f, *arr, &c_export));

    ExportGuard guard(&c_export);
    ExportChecker({"n"}, {"thing"}, {0})(&c_export, *arr->data());
  }
  {
    // With metadata
    auto arr = ArrayFromJSON(null(), "[null, null, null]");
    auto f = field("thing", null(), /*nullable=*/false);
    f = f->WithMetadata(key_value_metadata(kMetadataKeys1, kMetadataValues1));
    ASSERT_OK(ExportArray(*f, *arr, &c_export));

    ExportGuard guard(&c_export);
    ExportChecker({"n"}, {"thing"}, {0}, {kEncodedMetadata1})(&c_export, *arr->data());
  }
}

TEST_F(TestExport, AsRecordBatch) {
  struct ArrowArray c_export;

  auto schema = ::arrow::schema(
      {field("ints", int16()), field("bools", boolean(), /*nullable=*/false)});
  auto arr0 = ArrayFromJSON(int16(), "[1, 2, null]");
  auto arr1 = ArrayFromJSON(boolean(), "[false, true, false]");

  {
    auto batch = RecordBatch::Make(schema, 3, {arr0, arr1});
    ASSERT_OK(ExportRecordBatch(*batch, &c_export));
    ExportGuard guard(&c_export);

    ASSERT_EQ(c_export.null_count, 0);
    StructArray expected(struct_(schema->fields()), 3, batch->columns());
    ASSERT_EQ(expected.null_count(), 0);  // compute null count for comparison below
    ExportChecker({"+s", "s", "b"}, {"", "ints", "bools"},
                  {0, ARROW_FLAG_NULLABLE, 0})(&c_export, *expected.data());
  }
  {
    // With schema and field metadata
    auto f0 = schema->field(0);
    auto f1 = schema->field(1);
    f1 = f1->WithMetadata(key_value_metadata(kMetadataKeys1, kMetadataValues1));
    schema = ::arrow::schema({f0, f1},
        key_value_metadata(kMetadataKeys2, kMetadataValues2));
    auto batch = RecordBatch::Make(schema, 3, {arr0, arr1});
    ASSERT_OK(ExportRecordBatch(*batch, &c_export));
    ExportGuard guard(&c_export);

    ASSERT_EQ(c_export.null_count, 0);
    StructArray expected(struct_(schema->fields()), 3, batch->columns());
    ASSERT_EQ(expected.null_count(), 0);  // compute null count for comparison below
    ExportChecker(
        {"+s", "s", "b"}, {"", "ints", "bools"}, {0, ARROW_FLAG_NULLABLE, 0},
        {kEncodedMetadata2, "", kEncodedMetadata1})(&c_export, *expected.data());
  }
}

////////////////////////////////////////////////////////////////////////////
// Import tests

// [true, false, true, true, false, true, true, true] * 2
static const uint8_t bits_buffer1[] = {0xed, 0xed};

static const void* buffers_no_nulls_no_data[1] = {nullptr};
static const void* buffers_nulls_no_data1[1] = {bits_buffer1};

static const uint8_t data_buffer1[] = {1, 2,  3,  4,  5,  6,  7,  8,
                                       9, 10, 11, 12, 13, 14, 15, 16};
static const uint8_t data_buffer2[] = "abcdefghijklmnopqrstuvwxyz";
static const uint64_t data_buffer3[] = {123456789, 0, 987654321, 0};
static const uint8_t data_buffer4[] = {1, 2, 0, 1, 3, 0};
static const float data_buffer5[] = {0.0f, 1.5f, -2.0f, 3.0f, 4.0f, 5.0f};
static const double data_buffer6[] = {0.0, 1.5, -2.0, 3.0, 4.0, 5.0};
static const int32_t data_buffer7[] = {1234, 5678, 9012, 3456};
static const int64_t data_buffer8[] = {123456789, 987654321, -123456789, -987654321};
static const void* primitive_buffers_no_nulls1[2] = {nullptr, data_buffer1};
static const void* primitive_buffers_nulls1[2] = {bits_buffer1, data_buffer1};
static const void* primitive_buffers_no_nulls2[2] = {nullptr, data_buffer2};
static const void* primitive_buffers_no_nulls3[2] = {nullptr, data_buffer3};
static const void* primitive_buffers_no_nulls4[2] = {nullptr, data_buffer4};
static const void* primitive_buffers_no_nulls5[2] = {nullptr, data_buffer5};
static const void* primitive_buffers_no_nulls6[2] = {nullptr, data_buffer6};
static const void* primitive_buffers_no_nulls7[2] = {nullptr, data_buffer7};
static const void* primitive_buffers_nulls7[2] = {bits_buffer1, data_buffer7};
static const void* primitive_buffers_no_nulls8[2] = {nullptr, data_buffer8};
static const void* primitive_buffers_nulls8[2] = {bits_buffer1, data_buffer8};

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

static const uint8_t string_data_buffer1[] = "foobarquux";

static const int32_t string_offsets_buffer1[] = {0, 3, 3, 6, 10};
static const void* string_buffers_no_nulls1[3] = {nullptr, string_offsets_buffer1,
                                                  string_data_buffer1};

static const int64_t large_string_offsets_buffer1[] = {0, 3, 3, 6, 10};
static const void* large_string_buffers_no_nulls1[3] = {
    nullptr, large_string_offsets_buffer1, string_data_buffer1};

static const int32_t list_offsets_buffer1[] = {0, 2, 2, 5, 6, 8};
static const void* list_buffers_no_nulls1[2] = {nullptr, list_offsets_buffer1};

static const int64_t large_list_offsets_buffer1[] = {0, 2, 2, 5, 6, 8};
static const void* large_list_buffers_no_nulls1[2] = {nullptr,
                                                      large_list_offsets_buffer1};

static const int8_t type_codes_buffer1[] = {42, 42, 43, 43, 42};
static const int32_t union_offsets_buffer1[] = {0, 1, 0, 1, 2};
static const void* sparse_union_buffers_no_nulls1[3] = {nullptr, type_codes_buffer1,
                                                        nullptr};
static const void* dense_union_buffers_no_nulls1[3] = {nullptr, type_codes_buffer1,
                                                       union_offsets_buffer1};

class TestImport : public ::testing::Test {
 public:
  void SetUp() override {
    memset(&c_struct_, 0, sizeof(c_struct_));
    c_struct_.name = "";
  }

  // Create a new ArrowArray struct with a stable C pointer
  struct ArrowArray* AddChild() {
    nested_structs_.emplace_back();
    struct ArrowArray* result = &nested_structs_.back();
    memset(result, 0, sizeof(*result));
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

  struct ArrowArray* LastChild(struct ArrowArray* parent) {
    return *NLastChildren(1, parent);
  }

  void FillPrimitive(struct ArrowArray* c, const char* format, int64_t length,
                     int64_t null_count, int64_t offset, const void** buffers,
                     int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 2;
    c->buffers = buffers;
  }

  void FillDictionary(struct ArrowArray* c) { c->dictionary = LastChild(c); }

  void FillStringLike(struct ArrowArray* c, const char* format, int64_t length,
                      int64_t null_count, int64_t offset, const void** buffers,
                      int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 3;
    c->buffers = buffers;
  }

  void FillListLike(struct ArrowArray* c, const char* format, int64_t length,
                    int64_t null_count, int64_t offset, const void** buffers,
                    int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 2;
    c->buffers = buffers;
    c->n_children = 1;
    c->children = NLastChildren(1, c);
    c->children[0]->name = "item";
  }

  void FillFixedSizeListLike(struct ArrowArray* c, const char* format, int64_t length,
                             int64_t null_count, int64_t offset, const void** buffers,
                             int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 1;
    c->buffers = buffers;
    c->n_children = 1;
    c->children = NLastChildren(1, c);
    c->children[0]->name = "item";
  }

  void FillStructLike(struct ArrowArray* c, const char* format, int64_t length,
                      int64_t null_count, int64_t offset,
                      std::vector<std::string> child_names, const void** buffers,
                      int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 1;
    c->buffers = buffers;
    c->n_children = static_cast<int64_t>(child_names.size());
    c->children = NLastChildren(c->n_children, c);
    for (int64_t i = 0; i < c->n_children; ++i) {
      children_names_.push_back(std::move(child_names[i]));
      c->children[i]->name = children_names_.back().c_str();
    }
  }

  void FillUnionLike(struct ArrowArray* c, const char* format, int64_t length,
                     int64_t null_count, int64_t offset,
                     std::vector<std::string> child_names, const void** buffers,
                     int64_t flags = kDefaultFlags) {
    c->flags = flags;
    c->format = format;
    c->length = length;
    c->null_count = null_count;
    c->offset = offset;
    c->n_buffers = 3;
    c->buffers = buffers;
    c->n_children = static_cast<int64_t>(child_names.size());
    c->children = NLastChildren(c->n_children, c);
    for (int64_t i = 0; i < c->n_children; ++i) {
      children_names_.push_back(std::move(child_names[i]));
      c->children[i]->name = children_names_.back().c_str();
    }
  }

  void FillPrimitive(const char* format, int64_t length, int64_t null_count,
                     int64_t offset, const void** buffers,
                     int64_t flags = kDefaultFlags) {
    FillPrimitive(&c_struct_, format, length, null_count, offset, buffers, flags);
  }

  void FillDictionary() { FillDictionary(&c_struct_); }

  void FillStringLike(const char* format, int64_t length, int64_t null_count,
                      int64_t offset, const void** buffers,
                      int64_t flags = kDefaultFlags) {
    FillStringLike(&c_struct_, format, length, null_count, offset, buffers, flags);
  }

  void FillListLike(const char* format, int64_t length, int64_t null_count,
                    int64_t offset, const void** buffers, int64_t flags = kDefaultFlags) {
    FillListLike(&c_struct_, format, length, null_count, offset, buffers, flags);
  }

  void FillFixedSizeListLike(const char* format, int64_t length, int64_t null_count,
                             int64_t offset, const void** buffers,
                             int64_t flags = kDefaultFlags) {
    FillFixedSizeListLike(&c_struct_, format, length, null_count, offset, buffers, flags);
  }

  void FillStructLike(const char* format, int64_t length, int64_t null_count,
                      int64_t offset, std::vector<std::string> child_names,
                      const void** buffers, int64_t flags = kDefaultFlags) {
    FillStructLike(&c_struct_, format, length, null_count, offset, std::move(child_names),
                   buffers, flags);
  }

  void FillUnionLike(const char* format, int64_t length, int64_t null_count,
                     int64_t offset, std::vector<std::string> child_names,
                     const void** buffers, int64_t flags = kDefaultFlags) {
    FillUnionLike(&c_struct_, format, length, null_count, offset, std::move(child_names),
                  buffers, flags);
  }

  void CheckImport(const std::shared_ptr<Array>& expected) {
    ReleaseCallback cb(&c_struct_);

    std::shared_ptr<Array> array;
    ASSERT_OK(ImportArray(&c_struct_, &array));
    ASSERT_TRUE(ArrowIsReleased(&c_struct_));  // was moved
    ASSERT_OK(array->ValidateFull());
    // Special case: Null array doesn't have any data, so it needn't
    // keep the ArrowArray struct alive.
    if (expected->type_id() != Type::NA) {
      cb.AssertNotCalled();
    }
    AssertArraysEqual(*expected, *array, true);
    array.reset();
    cb.AssertCalled();
  }

  void CheckImportError() {
    ReleaseCallback cb(&c_struct_);

    std::shared_ptr<Array> array;
    ASSERT_RAISES(Invalid, ImportArray(&c_struct_, &array));
    ASSERT_TRUE(ArrowIsReleased(&c_struct_));  // was moved
    // The ArrowArray should have been released.
    cb.AssertCalled();
  }

  void CheckImportAsRecordBatchError() {
    ReleaseCallback cb(&c_struct_);

    std::shared_ptr<RecordBatch> batch;
    ASSERT_RAISES(Invalid, ImportRecordBatch(&c_struct_, &batch));
    ASSERT_TRUE(ArrowIsReleased(&c_struct_));  // was moved
    // The ArrowArray should have been released.
    cb.AssertCalled();
  }

 protected:
  struct ArrowArray c_struct_;
  // Deque elements don't move when the deque is appended to, which allows taking
  // stable C pointers to them.
  std::deque<struct ArrowArray> nested_structs_;
  std::deque<std::vector<struct ArrowArray*>> children_arrays_;
  std::deque<std::string> children_names_;
};

TEST_F(TestImport, Primitive) {
  FillPrimitive("c", 3, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(int8(), "[1, 2, 3]"));
  FillPrimitive("C", 5, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(uint8(), "[1, 2, 3, 4, 5]"));
  FillPrimitive("s", 3, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(int16(), "[513, 1027, 1541]"));
  FillPrimitive("S", 3, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(uint16(), "[513, 1027, 1541]"));
  FillPrimitive("i", 2, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(int32(), "[67305985, 134678021]"));
  FillPrimitive("I", 2, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(uint32(), "[67305985, 134678021]"));
  FillPrimitive("l", 2, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(int64(), "[578437695752307201, 1157159078456920585]"));
  FillPrimitive("L", 2, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(uint64(), "[578437695752307201, 1157159078456920585]"));

  FillPrimitive("b", 3, 0, 0, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(boolean(), "[true, false, false]"));
  FillPrimitive("f", 6, 0, 0, primitive_buffers_no_nulls5);
  CheckImport(ArrayFromJSON(float32(), "[0.0, 1.5, -2.0, 3.0, 4.0, 5.0]"));
  FillPrimitive("g", 6, 0, 0, primitive_buffers_no_nulls6);
  CheckImport(ArrayFromJSON(float64(), "[0.0, 1.5, -2.0, 3.0, 4.0, 5.0]"));

  // With nulls
  FillPrimitive("c", 9, -1, 0, primitive_buffers_nulls1);
  CheckImport(ArrayFromJSON(int8(), "[1, null, 3, 4, null, 6, 7, 8, 9]"));
  FillPrimitive("c", 9, 2, 0, primitive_buffers_nulls1);
  CheckImport(ArrayFromJSON(int8(), "[1, null, 3, 4, null, 6, 7, 8, 9]"));
  FillPrimitive("s", 3, -1, 0, primitive_buffers_nulls1);
  CheckImport(ArrayFromJSON(int16(), "[513, null, 1541]"));
  FillPrimitive("s", 3, 1, 0, primitive_buffers_nulls1);
  CheckImport(ArrayFromJSON(int16(), "[513, null, 1541]"));
  FillPrimitive("b", 3, -1, 0, primitive_buffers_nulls1);
  CheckImport(ArrayFromJSON(boolean(), "[true, null, false]"));
  FillPrimitive("b", 3, 1, 0, primitive_buffers_nulls1);
  CheckImport(ArrayFromJSON(boolean(), "[true, null, false]"));
}

TEST_F(TestImport, Temporal) {
  FillPrimitive("tdD", 3, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(date32(), "[1234, 5678, 9012]"));
  FillPrimitive("tdm", 3, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(date64(), "[123456789, 987654321, -123456789]"));

  FillPrimitive("tts", 2, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(time32(TimeUnit::SECOND), "[1234, 5678]"));
  FillPrimitive("ttm", 2, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(time32(TimeUnit::MILLI), "[1234, 5678]"));
  FillPrimitive("ttu", 2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(time64(TimeUnit::MICRO), "[123456789, 987654321]"));
  FillPrimitive("ttn", 2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(time64(TimeUnit::NANO), "[123456789, 987654321]"));

  FillPrimitive("tDs", 2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::SECOND), "[123456789, 987654321]"));
  FillPrimitive("tDm", 2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::MILLI), "[123456789, 987654321]"));
  FillPrimitive("tDu", 2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::MICRO), "[123456789, 987654321]"));
  FillPrimitive("tDn", 2, 0, 0, primitive_buffers_no_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::NANO), "[123456789, 987654321]"));

  FillPrimitive("tiM", 3, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(month_interval(), "[1234, 5678, 9012]"));
  FillPrimitive("tiD", 2, 0, 0, primitive_buffers_no_nulls7);
  CheckImport(ArrayFromJSON(day_time_interval(), "[[1234, 5678], [9012, 3456]]"));

  const char* json = R"(["1970-01-01","2000-02-29","1900-02-28"])";
  FillPrimitive("tss:", 3, 0, 0, timestamp_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::SECOND), json));
  FillPrimitive("tsm:", 3, 0, 0, timestamp_buffers_no_nulls2);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::MILLI), json));
  FillPrimitive("tsu:", 3, 0, 0, timestamp_buffers_no_nulls3);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::MICRO), json));
  FillPrimitive("tsn:", 3, 0, 0, timestamp_buffers_no_nulls4);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::NANO), json));

  // With nulls
  FillPrimitive("tdD", 3, -1, 0, primitive_buffers_nulls7);
  CheckImport(ArrayFromJSON(date32(), "[1234, null, 9012]"));
  FillPrimitive("tdm", 3, -1, 0, primitive_buffers_nulls8);
  CheckImport(ArrayFromJSON(date64(), "[123456789, null, -123456789]"));
  FillPrimitive("ttn", 2, -1, 0, primitive_buffers_nulls8);
  CheckImport(ArrayFromJSON(time64(TimeUnit::NANO), "[123456789, null]"));
  FillPrimitive("tDn", 2, -1, 0, primitive_buffers_nulls8);
  CheckImport(ArrayFromJSON(duration(TimeUnit::NANO), "[123456789, null]"));
  FillPrimitive("tiM", 3, -1, 0, primitive_buffers_nulls7);
  CheckImport(ArrayFromJSON(month_interval(), "[1234, null, 9012]"));
  FillPrimitive("tiD", 2, -1, 0, primitive_buffers_nulls7);
  CheckImport(ArrayFromJSON(day_time_interval(), "[[1234, 5678], null]"));
  FillPrimitive("tss:UTC+2", 3, -1, 0, timestamp_buffers_nulls1);
  CheckImport(ArrayFromJSON(timestamp(TimeUnit::SECOND, "UTC+2"),
                            R"(["1970-01-01",null,"1900-02-28"])"));
}

TEST_F(TestImport, Null) {
  const void* buffers[] = {nullptr};
  c_struct_.format = "n";
  c_struct_.length = 3;
  c_struct_.null_count = 3;
  c_struct_.offset = 0;
  c_struct_.n_buffers = 1;
  c_struct_.buffers = buffers;
  CheckImport(ArrayFromJSON(null(), "[null, null, null]"));
}

TEST_F(TestImport, PrimitiveWithOffset) {
  FillPrimitive("c", 3, 0, 2, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(int8(), "[3, 4, 5]"));
  FillPrimitive("S", 3, 0, 1, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(uint16(), "[1027, 1541, 2055]"));

  FillPrimitive("b", 4, 0, 7, primitive_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(boolean(), "[false, false, true, false]"));
}

TEST_F(TestImport, NullWithOffset) {
  const void* buffers[] = {nullptr};
  c_struct_.format = "n";
  c_struct_.length = 3;
  c_struct_.null_count = 3;
  c_struct_.offset = 5;
  c_struct_.n_buffers = 1;
  c_struct_.buffers = buffers;
  CheckImport(ArrayFromJSON(null(), "[null, null, null]"));
}

TEST_F(TestImport, String) {
  FillStringLike("u", 4, 0, 0, string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(utf8(), R"(["foo", "", "bar", "quux"])"));
  FillStringLike("z", 4, 0, 0, string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(binary(), R"(["foo", "", "bar", "quux"])"));
  FillStringLike("U", 4, 0, 0, large_string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(large_utf8(), R"(["foo", "", "bar", "quux"])"));
  FillStringLike("Z", 4, 0, 0, large_string_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(large_binary(), R"(["foo", "", "bar", "quux"])"));

  FillPrimitive("w:3", 2, 0, 0, primitive_buffers_no_nulls2);
  CheckImport(ArrayFromJSON(fixed_size_binary(3), R"(["abc", "def"])"));
  FillPrimitive("d:15,4", 2, 0, 0, primitive_buffers_no_nulls3);
  CheckImport(ArrayFromJSON(decimal(15, 4), R"(["12345.6789", "98765.4321"])"));
}

TEST_F(TestImport, List) {
  FillPrimitive(AddChild(), "c", 8, 0, 0, primitive_buffers_no_nulls1);
  FillListLike("+l", 5, 0, 0, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int8()), "[[1, 2], [], [3, 4, 5], [6], [7, 8]]"));
  FillPrimitive(AddChild(), "s", 5, 0, 0, primitive_buffers_no_nulls1);
  FillListLike("+l", 3, 0, 0, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int16()), "[[513, 1027], [], [1541, 2055, 2569]]"));

  // Large list
  FillPrimitive(AddChild(), "s", 5, 0, 0, primitive_buffers_no_nulls1);
  FillListLike("+L", 3, 0, 0, large_list_buffers_no_nulls1);
  CheckImport(
      ArrayFromJSON(large_list(int16()), "[[513, 1027], [], [1541, 2055, 2569]]"));

  // Fixed-size list
  FillPrimitive(AddChild(), "c", 9, 0, 0, primitive_buffers_no_nulls1);
  FillFixedSizeListLike("+w:3", 3, 0, 0, buffers_no_nulls_no_data);
  CheckImport(
      ArrayFromJSON(fixed_size_list(int8(), 3), "[[1, 2, 3], [4, 5, 6], [7, 8, 9]]"));
}

TEST_F(TestImport, NestedList) {
  FillPrimitive(AddChild(), "c", 8, 0, 0, primitive_buffers_no_nulls1);
  FillListLike(AddChild(), "+l", 5, 0, 0, list_buffers_no_nulls1);
  FillListLike("+L", 3, 0, 0, large_list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(large_list(list(int8())),
                            "[[[1, 2], []], [], [[3, 4, 5], [6], [7, 8]]]"));

  FillPrimitive(AddChild(), "c", 6, 0, 0, primitive_buffers_no_nulls1);
  FillFixedSizeListLike(AddChild(), "+w:3", 2, 0, 0, buffers_no_nulls_no_data);
  FillListLike("+l", 2, 0, 0, list_buffers_no_nulls1);
  CheckImport(
      ArrayFromJSON(list(fixed_size_list(int8(), 3)), "[[[1, 2, 3], [4, 5, 6]], []]"));
}

TEST_F(TestImport, ListWithOffset) {
  // Offset in child
  FillPrimitive(AddChild(), "c", 8, 0, 1, primitive_buffers_no_nulls1);
  FillListLike("+l", 5, 0, 0, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int8()), "[[2, 3], [], [4, 5, 6], [7], [8, 9]]"));

  FillPrimitive(AddChild(), "c", 9, 0, 1, primitive_buffers_no_nulls1);
  FillFixedSizeListLike("+w:3", 3, 0, 0, buffers_no_nulls_no_data);
  CheckImport(
      ArrayFromJSON(fixed_size_list(int8(), 3), "[[2, 3, 4], [5, 6, 7], [8, 9, 10]]"));

  // Offset in parent
  FillPrimitive(AddChild(), "c", 8, 0, 0, primitive_buffers_no_nulls1);
  FillListLike("+l", 4, 0, 1, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int8()), "[[], [3, 4, 5], [6], [7, 8]]"));

  FillPrimitive(AddChild(), "c", 9, 0, 0, primitive_buffers_no_nulls1);
  FillFixedSizeListLike("+w:3", 3, 0, 1, buffers_no_nulls_no_data);
  CheckImport(
      ArrayFromJSON(fixed_size_list(int8(), 3), "[[4, 5, 6], [7, 8, 9], [10, 11, 12]]"));

  // Both
  FillPrimitive(AddChild(), "c", 8, 0, 2, primitive_buffers_no_nulls1);
  FillListLike("+l", 4, 0, 1, list_buffers_no_nulls1);
  CheckImport(ArrayFromJSON(list(int8()), "[[], [5, 6, 7], [8], [9, 10]]"));

  FillPrimitive(AddChild(), "c", 9, 0, 2, primitive_buffers_no_nulls1);
  FillFixedSizeListLike("+w:3", 3, 0, 1, buffers_no_nulls_no_data);
  CheckImport(ArrayFromJSON(fixed_size_list(int8(), 3),
                            "[[6, 7, 8], [9, 10, 11], [12, 13, 14]]"));
}

TEST_F(TestImport, Struct) {
  FillStringLike(AddChild(), "u", 3, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), "S", 3, -1, 0, primitive_buffers_nulls1);
  FillStructLike("+s", 3, 0, 0, {"strs", "ints"}, buffers_no_nulls_no_data);
  auto expected = ArrayFromJSON(struct_({field("strs", utf8()), field("ints", uint16())}),
                                R"([["foo", 513], ["", null], ["bar", 1541]])");
  CheckImport(expected);

  FillStringLike(AddChild(), "u", 3, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), "S", 3, 0, 0, primitive_buffers_no_nulls1);
  FillStructLike("+s", 3, -1, 0, {"strs", "ints"}, buffers_nulls_no_data1);
  expected = ArrayFromJSON(struct_({field("strs", utf8()), field("ints", uint16())}),
                           R"([["foo", 513], null, ["bar", 1541]])");
  CheckImport(expected);

  FillStringLike(AddChild(), "u", 3, 0, 0, string_buffers_no_nulls1, /*flags=*/0);
  FillPrimitive(AddChild(), "S", 3, 0, 0, primitive_buffers_no_nulls1);
  FillStructLike("+s", 3, -1, 0, {"strs", "ints"}, buffers_nulls_no_data1);
  expected = ArrayFromJSON(
      struct_({field("strs", utf8(), /*nullable=*/false), field("ints", uint16())}),
      R"([["foo", 513], null, ["bar", 1541]])");
  CheckImport(expected);
}

TEST_F(TestImport, Union) {
  // Sparse
  FillStringLike(AddChild(), "u", 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), "c", 4, -1, 0, primitive_buffers_nulls1);
  FillUnionLike("+us:43,42", 4, 0, 0, {"strs", "ints"}, sparse_union_buffers_no_nulls1);
  auto type =
      union_({field("strs", utf8()), field("ints", int8())}, {43, 42}, UnionMode::SPARSE);
  auto expected =
      ArrayFromJSON(type, R"([[42, 1], [42, null], [43, "bar"], [43, "quux"]])");
  CheckImport(expected);

  // Dense
  FillStringLike(AddChild(), "u", 2, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), "c", 3, -1, 0, primitive_buffers_nulls1);
  FillUnionLike("+ud:43,42", 5, 0, 0, {"strs", "ints"}, dense_union_buffers_no_nulls1);
  type =
      union_({field("strs", utf8()), field("ints", int8())}, {43, 42}, UnionMode::DENSE);
  expected =
      ArrayFromJSON(type, R"([[42, 1], [42, null], [43, "foo"], [43, ""], [42, 3]])");
  CheckImport(expected);
}

TEST_F(TestImport, StructWithOffset) {
  // Child
  FillStringLike(AddChild(), "u", 3, 0, 1, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), "c", 3, 0, 2, primitive_buffers_no_nulls1);
  FillStructLike("+s", 3, 0, 0, {"strs", "ints"}, buffers_no_nulls_no_data);
  auto expected = ArrayFromJSON(struct_({field("strs", utf8()), field("ints", int8())}),
                                R"([["", 3], ["bar", 4], ["quux", 5]])");
  CheckImport(expected);

  // Parent and child
  FillStringLike(AddChild(), "u", 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), "c", 4, 0, 2, primitive_buffers_no_nulls1);
  FillStructLike("+s", 3, 0, 1, {"strs", "ints"}, buffers_no_nulls_no_data);
  expected = ArrayFromJSON(struct_({field("strs", utf8()), field("ints", int8())}),
                           R"([["", 4], ["bar", 5], ["quux", 6]])");
  CheckImport(expected);
}

TEST_F(TestImport, Dictionary) {
  FillStringLike(AddChild(), "u", 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive("c", 6, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary();

  auto dict_values = ArrayFromJSON(utf8(), R"(["foo", "", "bar", "quux"])");
  auto indices = ArrayFromJSON(int8(), "[1, 2, 0, 1, 3, 0]");
  std::shared_ptr<Array> expected;
  ASSERT_OK(DictionaryArray::FromArrays(dictionary(int8(), utf8()), indices, dict_values,
                                        &expected));
  CheckImport(expected);

  FillStringLike(AddChild(), "u", 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive("c", 6, 0, 0, primitive_buffers_no_nulls4,
                ARROW_FLAG_NULLABLE | ARROW_FLAG_DICTIONARY_ORDERED);
  FillDictionary();

  ASSERT_OK(DictionaryArray::FromArrays(dictionary(int8(), utf8(), /*ordered=*/true),
                                        indices, dict_values, &expected));
  CheckImport(expected);
}

TEST_F(TestImport, DictionaryWithOffset) {
  FillStringLike(AddChild(), "u", 3, 0, 1, string_buffers_no_nulls1);
  FillPrimitive("c", 3, 0, 0, primitive_buffers_no_nulls4);
  FillDictionary();

  auto dict_values = ArrayFromJSON(utf8(), R"(["", "bar", "quux"])");
  auto indices = ArrayFromJSON(int8(), "[1, 2, 0]");
  std::shared_ptr<Array> expected;
  ASSERT_OK(DictionaryArray::FromArrays(dictionary(int8(), utf8()), indices, dict_values,
                                        &expected));
  CheckImport(expected);

  FillStringLike(AddChild(), "u", 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive("c", 4, 0, 2, primitive_buffers_no_nulls4);
  FillDictionary();

  dict_values = ArrayFromJSON(utf8(), R"(["foo", "", "bar", "quux"])");
  indices = ArrayFromJSON(int8(), "[0, 1, 3, 0]");
  ASSERT_OK(DictionaryArray::FromArrays(dictionary(int8(), utf8()), indices, dict_values,
                                        &expected));
  CheckImport(expected);
}

TEST_F(TestImport, ErrorFormatString) {
  FillPrimitive("cc", 3, 0, 0, primitive_buffers_no_nulls1);
  CheckImportError();
  FillPrimitive("w3", 2, 0, 0, primitive_buffers_no_nulls2);
  CheckImportError();
  FillPrimitive("w:three", 2, 0, 0, primitive_buffers_no_nulls2);
  CheckImportError();
  FillPrimitive("w:3,5", 2, 0, 0, primitive_buffers_no_nulls2);
  CheckImportError();
  FillPrimitive("d:15", 2, 0, 0, primitive_buffers_no_nulls3);
  CheckImportError();
  FillPrimitive("d:15.4", 2, 0, 0, primitive_buffers_no_nulls3);
  CheckImportError();
  FillPrimitive("t", 3, 0, 0, primitive_buffers_no_nulls7);
  CheckImportError();
  FillPrimitive("td", 3, 0, 0, primitive_buffers_no_nulls7);
  CheckImportError();
  FillPrimitive("tz", 3, 0, 0, primitive_buffers_no_nulls7);
  CheckImportError();
  FillPrimitive("tdd", 3, 0, 0, primitive_buffers_no_nulls7);
  CheckImportError();
  FillPrimitive("tdDd", 3, 0, 0, primitive_buffers_no_nulls7);
  CheckImportError();
  FillPrimitive("tss", 3, 0, 0, timestamp_buffers_no_nulls1);
  CheckImportError();
  FillPrimitive("tss;UTC", 3, 0, 0, timestamp_buffers_no_nulls1);
  CheckImportError();
}

TEST_F(TestImport, ErrorPrimitive) {
  // Bad number of buffers
  FillPrimitive("c", 3, 0, 0, primitive_buffers_no_nulls1);
  c_struct_.n_buffers = 1;
  CheckImportError();
  // Zero null bitmap but non-zero null_count
  FillPrimitive("c", 3, 1, 0, primitive_buffers_no_nulls1);
  CheckImportError();
}

TEST_F(TestImport, ErrorDictionary) {
  // Bad index type
  FillPrimitive(AddChild(), "c", 3, 0, 0, primitive_buffers_no_nulls4);
  FillStringLike("u", 3, 0, 1, string_buffers_no_nulls1);
  FillDictionary();
  CheckImportError();
}

TEST_F(TestImport, WithField) {
  std::shared_ptr<Field> field;
  std::shared_ptr<Array> array;
  auto expected_array = ArrayFromJSON(int8(), "[1, 2, 3]");

  {
    FillPrimitive("c", 3, 0, 0, primitive_buffers_no_nulls1);
    auto expected_field = ::arrow::field("", int8());

    ReleaseCallback cb(&c_struct_);
    ASSERT_OK(ImportArray(&c_struct_, &field, &array));
    ASSERT_TRUE(ArrowIsReleased(&c_struct_));  // was moved
    ASSERT_OK(array->ValidateFull());
    AssertArraysEqual(*expected_array, *array, true);
    AssertFieldEqual(*expected_field, *field);
    array.reset();
    cb.AssertCalled();
  }
  {
    // With nullable = false and metadata
    FillPrimitive("c", 3, 0, 0, primitive_buffers_no_nulls1, 0);
    c_struct_.name = "ints";
    c_struct_.metadata = kEncodedMetadata1.c_str();
    auto expected_field = ::arrow::field(
        "ints", int8(), /*nullable=*/false,
        key_value_metadata(kMetadataKeys1, kMetadataValues1));

    ReleaseCallback cb(&c_struct_);
    ASSERT_OK(ImportArray(&c_struct_, &field, &array));
    ASSERT_TRUE(ArrowIsReleased(&c_struct_));  // was moved
    ASSERT_OK(array->ValidateFull());
    AssertArraysEqual(*expected_array, *array, true);
    AssertFieldEqual(*expected_field, *field);
    array.reset();
    cb.AssertCalled();
  }
}

TEST_F(TestImport, AsRecordBatch) {
  std::shared_ptr<RecordBatch> batch;
  auto schema = ::arrow::schema(
      {field("strs", utf8(), /*nullable=*/false), field("ints", uint16())});
  auto expected_strs = ArrayFromJSON(utf8(), R"(["", "bar", "quux"])");
  auto expected_ints = ArrayFromJSON(uint16(), "[513, null, 1541]");

  {
    FillStringLike(AddChild(), "u", 3, 0, 1, string_buffers_no_nulls1, 0);
    FillPrimitive(AddChild(), "S", 3, -1, 0, primitive_buffers_nulls1);
    FillStructLike("+s", 3, 0, 0, {"strs", "ints"}, buffers_no_nulls_no_data);

    ReleaseCallback cb(&c_struct_);
    ASSERT_OK(ImportRecordBatch(&c_struct_, &batch));
    ASSERT_TRUE(ArrowIsReleased(&c_struct_));  // was moved
    ASSERT_OK(batch->ValidateFull());
    ASSERT_EQ(batch->num_columns(), 2);
    AssertSchemaEqual(*schema, *batch->schema());
    AssertArraysEqual(*expected_strs, *batch->column(0), true);
    AssertArraysEqual(*expected_ints, *batch->column(1), true);
    batch.reset();
    cb.AssertCalled();
  }
  {
    // With schema and field metadata
    FillStringLike(AddChild(), "u", 3, 0, 1, string_buffers_no_nulls1, 0);
    FillPrimitive(AddChild(), "S", 3, -1, 0, primitive_buffers_nulls1);
    FillStructLike("+s", 3, 0, 0, {"strs", "ints"}, buffers_no_nulls_no_data);
    c_struct_.metadata = kEncodedMetadata1.c_str();
    c_struct_.children[0]->metadata = kEncodedMetadata2.c_str();
    auto f0 = schema->field(0)->WithMetadata(
        key_value_metadata(kMetadataKeys2, kMetadataValues2));
    auto f1 = schema->field(1);
    schema = ::arrow::schema(
        {f0, f1}, key_value_metadata(kMetadataKeys1, kMetadataValues1));

    ReleaseCallback cb(&c_struct_);
    ASSERT_OK(ImportRecordBatch(&c_struct_, &batch));
    ASSERT_TRUE(ArrowIsReleased(&c_struct_));  // was moved
    ASSERT_OK(batch->ValidateFull());
    ASSERT_EQ(batch->num_columns(), 2);
    AssertSchemaEqual(*schema, *batch->schema());
    AssertArraysEqual(*expected_strs, *batch->column(0), true);
    AssertArraysEqual(*expected_ints, *batch->column(1), true);
    batch.reset();
    cb.AssertCalled();
  }
}

TEST_F(TestImport, AsRecordBatchError) {
  // Not a struct
  FillStringLike("u", 3, 0, 1, string_buffers_no_nulls1);
  CheckImportAsRecordBatchError();

  // Struct with non-zero parent offset
  FillStringLike(AddChild(), "u", 4, 0, 0, string_buffers_no_nulls1);
  FillPrimitive(AddChild(), "c", 4, 0, 0, primitive_buffers_no_nulls1);
  FillStructLike("+s", 3, 0, 1, {"strs", "ints"}, buffers_no_nulls_no_data);
  CheckImportAsRecordBatchError();

  // Struct with nulls in parent
  FillStringLike(AddChild(), "u", 3, 0, 0, string_buffers_no_nulls1, /*flags=*/0);
  FillPrimitive(AddChild(), "S", 3, 0, 0, primitive_buffers_no_nulls1);
  FillStructLike("+s", 3, -1, 0, {"strs", "ints"}, buffers_nulls_no_data1);
  CheckImportAsRecordBatchError();
}

////////////////////////////////////////////////////////////////////////////
// C++ -> C -> C++ roundtripping tests

class TestRoundtrip : public ::testing::Test {
 public:
  using ArrayFactory = std::function<Status(std::shared_ptr<Array>*)>;

  void SetUp() override { pool_ = default_memory_pool(); }

  static ArrayFactory JSONArrayFactory(std::shared_ptr<DataType> type, const char* json) {
    return [=](std::shared_ptr<Array>* out) -> Status {
      return ::arrow::ipc::internal::json::ArrayFromJSON(type, json, out);
    };
  }

  static ArrayFactory SlicedArrayFactory(ArrayFactory factory) {
    return [=](std::shared_ptr<Array>* out) -> Status {
      std::shared_ptr<Array> arr;
      RETURN_NOT_OK(factory(&arr));
      DCHECK_GE(arr->length(), 2);
      *out = arr->Slice(1, arr->length() - 2);
      return Status::OK();
    };
  }

  template <typename ArrayFactory>
  void TestWithArrayFactory(ArrayFactory&& factory) {
    struct ArrowArray c_export;
    ExportGuard guard(&c_export);

    auto orig_bytes = pool_->bytes_allocated();
    std::shared_ptr<Array> arr;
    ASSERT_OK(factory(&arr));
    ASSERT_OK(ExportArray(*arr, &c_export));

    auto new_bytes = pool_->bytes_allocated();
    if (arr->type_id() != Type::NA) {
      ASSERT_GT(new_bytes, orig_bytes);
    }

    arr.reset();
    ASSERT_EQ(pool_->bytes_allocated(), new_bytes);
    ASSERT_OK(ImportArray(&c_export, &arr));
    ASSERT_OK(arr->ValidateFull());
    ASSERT_TRUE(ArrowIsReleased(&c_export));

    // Re-export and re-import
    ASSERT_OK(ExportArray(*arr, &c_export));
    arr.reset();
    ASSERT_OK(ImportArray(&c_export, &arr));
    ASSERT_OK(arr->ValidateFull());
    ASSERT_TRUE(ArrowIsReleased(&c_export));

    // Check value of imported array
    {
      std::shared_ptr<Array> expected;
      ASSERT_OK(factory(&expected));
      AssertTypeEqual(*expected->type(), *arr->type());
      AssertArraysEqual(*expected, *arr, true);
    }
    if (arr->type_id() != Type::NA) {
      ASSERT_GE(pool_->bytes_allocated(), new_bytes);
    }
    arr.reset();
    ASSERT_EQ(pool_->bytes_allocated(), orig_bytes);
  }

  template <typename BatchFactory>
  void TestWithBatchFactory(BatchFactory&& factory) {
    struct ArrowArray c_export;
    ExportGuard guard(&c_export);

    auto orig_bytes = pool_->bytes_allocated();
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(factory(&batch));
    ASSERT_OK(ExportRecordBatch(*batch, &c_export));

    auto new_bytes = pool_->bytes_allocated();
    batch.reset();
    ASSERT_EQ(pool_->bytes_allocated(), new_bytes);
    ASSERT_OK(ImportRecordBatch(&c_export, &batch));
    ASSERT_OK(batch->ValidateFull());
    ASSERT_TRUE(ArrowIsReleased(&c_export));

    // Re-export and re-import
    ASSERT_OK(ExportRecordBatch(*batch, &c_export));
    batch.reset();
    ASSERT_OK(ImportRecordBatch(&c_export, &batch));
    ASSERT_OK(batch->ValidateFull());
    ASSERT_TRUE(ArrowIsReleased(&c_export));

    // Check value of imported record batch
    {
      std::shared_ptr<RecordBatch> expected;
      ASSERT_OK(factory(&expected));
      AssertSchemaEqual(*expected->schema(), *batch->schema());
      AssertBatchesEqual(*expected, *batch);
    }
    ASSERT_GE(pool_->bytes_allocated(), new_bytes);
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

TEST_F(TestRoundtrip, Null) {
  TestWithJSON(null(), "[]");
  TestWithJSON(null(), "[null, null]");

  TestWithJSONSliced(null(), "[null, null]");
  TestWithJSONSliced(null(), "[null, null, null]");
}

TEST_F(TestRoundtrip, Primitive) {
  TestWithJSON(int32(), "[]");
  TestWithJSON(int32(), "[4, 5, null]");

  TestWithJSONSliced(int32(), "[4, 5]");
  TestWithJSONSliced(int32(), "[4, 5, 6, null]");
}

TEST_F(TestRoundtrip, Nested) {
  TestWithJSON(list(int32()), "[]");
  TestWithJSON(list(int32()), "[[4, 5], [6, null], null]");

  TestWithJSONSliced(list(int32()), "[[4, 5], [6, null], null]");

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

  // Map type
  type = map(utf8(), int32());
  const char* json = R"([[["foo", 123], ["bar", -456]], null,
                        [["foo", null]], []])";
  TestWithJSON(type, json);
  TestWithJSONSliced(type, json);

  type = map(utf8(), int32(), /*keys_sorted=*/ true);
  TestWithJSON(type, json);
  TestWithJSONSliced(type, json);
}

TEST_F(TestRoundtrip, Dictionary) {
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(dictionary(indices->type(), values->type()),
                                         indices, values, out);
    };
    TestWithArrayFactory(factory);
    TestWithArrayFactory(SlicedArrayFactory(factory));
  }
  {
    auto factory = [](std::shared_ptr<Array>* out) -> Status {
      auto values = ArrayFromJSON(list(utf8()), R"([["abc", "def"], ["efg"], []])");
      auto indices = ArrayFromJSON(int32(), "[0, 2, 1, null, 1]");
      return DictionaryArray::FromArrays(
          dictionary(indices->type(), values->type(), /*ordered=*/true), indices, values,
          out);
    };
    TestWithArrayFactory(factory);
    TestWithArrayFactory(SlicedArrayFactory(factory));
  }
}

TEST_F(TestRoundtrip, RecordBatch) {
  auto schema = ::arrow::schema(
      {field("ints", int16()), field("bools", boolean(), /*nullable=*/false)});
  auto arr0 = ArrayFromJSON(int16(), "[1, 2, null]");
  auto arr1 = ArrayFromJSON(boolean(), "[false, true, false]");

  {
    auto factory = [&](std::shared_ptr<RecordBatch>* out) -> Status {
      *out = RecordBatch::Make(schema, 3, {arr0, arr1});
      return Status::OK();
    };
    TestWithBatchFactory(factory);
  }
  {
    // With schema and field metadata
    auto factory = [&](std::shared_ptr<RecordBatch>* out) -> Status {
      auto f0 = schema->field(0);
      auto f1 = schema->field(1);
      f1 = f1->WithMetadata(key_value_metadata(kMetadataKeys1, kMetadataValues1));
      auto schema_with_md = ::arrow::schema({f0, f1},
          key_value_metadata(kMetadataKeys2, kMetadataValues2));
      *out = RecordBatch::Make(schema_with_md, 3, {arr0, arr1});
      return Status::OK();
    };
    TestWithBatchFactory(factory);
  }
}

// TODO C -> C++ -> C roundtripping tests?

}  // namespace arrow
