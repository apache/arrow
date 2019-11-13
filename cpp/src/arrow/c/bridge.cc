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

#include "arrow/c/bridge.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/c/helpers.h"
#include "arrow/memory_pool.h"
#include "arrow/stl.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/parsing.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

namespace {

//////////////////////////////////////////////////////////////////////////
// C data export

Status ExportingNotImplemented(const DataType& type) {
  return Status::NotImplemented("Exporting ", type.ToString(), " array not supported");
}

template <typename T>
using PoolVector = std::vector<T, ::arrow::stl::allocator<T>>;

struct ExportedArrayPrivateData {
  std::string format_;
  std::string name_;
  std::string metadata_;
  PoolVector<const void*> buffers_;
  struct ArrowArray dictionary_;
  PoolVector<struct ArrowArray> child_arrays_;
  PoolVector<struct ArrowArray*> child_array_pointers_;

  std::shared_ptr<ArrayData> data_;

  // Allocate ExportedArrayPrivateData instances using MemoryPool,
  // to allow accounting memory and checking for memory leaks.

  static void* operator new(size_t size) {
    DCHECK_EQ(size, sizeof(ExportedArrayPrivateData));
    uint8_t* data;
    ARROW_CHECK_OK(default_memory_pool()->Allocate(static_cast<int64_t>(size), &data));
    return data;
  }

  static void operator delete(void* ptr) {
    default_memory_pool()->Free(reinterpret_cast<uint8_t*>(ptr),
                                sizeof(ExportedArrayPrivateData));
  }
};

void ReleaseExportedArray(struct ArrowArray* array) {
  if (array->format == nullptr) {
    // Array already released
    return;
  }
  for (int64_t i = 0; i < array->n_children; ++i) {
    struct ArrowArray* child = array->children[i];
    ArrowReleaseArray(child);
    DCHECK_EQ(child->format, nullptr)
        << "Child release callback should have marked it released";
  }
  struct ArrowArray* dict = array->dictionary;
  if (dict != nullptr && dict->format != nullptr) {
    ArrowReleaseArray(dict);
    DCHECK_EQ(dict->format, nullptr)
        << "Dictionary release callback should have marked it released";
  }
  DCHECK_NE(array->private_data, nullptr);
  delete reinterpret_cast<ExportedArrayPrivateData*>(array->private_data);

  array->format = nullptr;
}

template <typename SizeType>
Status DowncastMetadataSize(SizeType size, int32_t* out) {
  *out = static_cast<int32_t>(size);
  if (*out < 0 || static_cast<SizeType>(*out) != size) {
    return Status::Invalid("Metadata too large (more than 2**31 items or bytes)");
  }
  return Status::OK();
}

Status ExportMetadata(const KeyValueMetadata& metadata, std::string* out) {
  int32_t npairs;
  RETURN_NOT_OK(DowncastMetadataSize(metadata.size(), &npairs));
  // Pre-compute total string size
  size_t total_size = 4;
  for (int32_t i = 0; i < npairs; ++i) {
    total_size += 8 + metadata.key(i).length() + metadata.value(i).length();
  }
  out->resize(total_size);

  char* data_start = &(*out)[0];
  char* data = data_start;
  auto write_int32 = [&](int32_t v) -> void {
    const int32_t le_v = BitUtil::ToLittleEndian(v);
    memcpy(data, &le_v, 4);
    data += 4;
  };
  auto write_string = [&](const std::string& s) -> Status {
    int32_t len;
    RETURN_NOT_OK(DowncastMetadataSize(s.length(), &len));
    write_int32(len);
    if (len > 0) {
      memcpy(data, s.data(), len);
      data += len;
    }
    return Status::OK();
  };

  write_int32(npairs);
  for (int32_t i = 0; i < npairs; ++i) {
    RETURN_NOT_OK(write_string(metadata.key(i)));
    RETURN_NOT_OK(write_string(metadata.value(i)));
  }
  DCHECK_EQ(static_cast<size_t>(data - data_start), total_size);
  return Status::OK();
}

struct ArrayExporter {
  explicit ArrayExporter(const std::shared_ptr<ArrayData>& data)
      : data_(data), flags_(0) {}

  Status Export(const Field* field = nullptr) {
    if (field != nullptr) {
      export_.name_ = field->name();
      flags_ = field->nullable() ? ARROW_FLAG_NULLABLE : 0;
    } else {
      flags_ = ARROW_FLAG_NULLABLE;
    }
    RETURN_NOT_OK(VisitTypeInline(*data_->type, this));
    DCHECK(!export_.format_.empty());

    // Store buffer pointers
    export_.buffers_.resize(data_->buffers.size());
    std::transform(data_->buffers.begin(), data_->buffers.end(), export_.buffers_.begin(),
                   [](const std::shared_ptr<Buffer>& buffer) -> const void* {
                     return buffer ? buffer->data() : nullptr;
                   });

    // Export dictionary
    if (data_->dictionary != nullptr) {
      if (checked_cast<const DictionaryType&>(*data_->type).ordered()) {
        flags_ |= ARROW_FLAG_ORDERED;
      }
      dict_exporter_.reset(new ArrayExporter(data_->dictionary->data()));
      RETURN_NOT_OK(dict_exporter_->Export());
    }

    // Export children
    export_.child_arrays_.resize(data_->child_data.size());
    for (size_t i = 0; i < data_->child_data.size(); ++i) {
      child_exporters_.emplace_back(data_->child_data[i]);
      RETURN_NOT_OK(
          child_exporters_.back().Export(data_->type->child(static_cast<int>(i)).get()));
    }

    // Export metadata
    export_.metadata_ = "";
    if (field != nullptr) {
      const auto metadata = field->metadata();
      if (metadata != nullptr && metadata->size() > 0) {
        RETURN_NOT_OK(ExportMetadata(*metadata, &export_.metadata_));
      }
    }

    // Store owning pointer to ArrayData
    export_.data_ = data_;

    return Status::OK();
  }

  // Finalize exporting by setting C struct fields and allocating
  // autonomous private data for each array node.
  //
  // This function can't fail, as properly reclaiming memory in case of error
  // would be too fragile.  After this function returns, memory is reclaimed
  // by calling the release() pointer in the top level ArrowArray struct.
  void Finish(struct ArrowArray* c_struct_) {
    // First, create permanent ExportedArrayPrivateData, to make sure that
    // child ArrayData pointers don't get invalidated.
    auto pdata = new ExportedArrayPrivateData(std::move(export_));

    // Second, finish dictionary and children.
    if (dict_exporter_) {
      dict_exporter_->Finish(&pdata->dictionary_);
    }
    pdata->child_array_pointers_.resize(data_->child_data.size(), nullptr);
    for (size_t i = 0; i < data_->child_data.size(); ++i) {
      auto ptr = pdata->child_array_pointers_[i] = &pdata->child_arrays_[i];
      child_exporters_[i].Finish(ptr);
    }

    // Third, fill C struct.
    DCHECK_NE(c_struct_, nullptr);
    memset(c_struct_, 0, sizeof(*c_struct_));

    c_struct_->format = pdata->format_.c_str();
    c_struct_->name = pdata->name_.c_str();
    c_struct_->metadata = pdata->metadata_.empty() ? nullptr : pdata->metadata_.c_str();
    c_struct_->flags = flags_;

    c_struct_->length = data_->length;
    c_struct_->null_count = data_->null_count;
    c_struct_->offset = data_->offset;
    c_struct_->n_buffers = static_cast<int64_t>(pdata->buffers_.size());
    c_struct_->n_children = static_cast<int64_t>(pdata->child_array_pointers_.size());
    c_struct_->buffers = pdata->buffers_.data();

    // We only initialize child_array_pointers_ here because the child_arrays_
    // vector may be resized, moved or copied around.
    std::transform(pdata->child_arrays_.begin(), pdata->child_arrays_.end(),
                   pdata->child_array_pointers_.begin(),
                   [](struct ArrowArray& array) { return &array; });
    c_struct_->children = pdata->child_array_pointers_.data();
    c_struct_->dictionary = dict_exporter_ ? &pdata->dictionary_ : nullptr;
    c_struct_->private_data = pdata;
    c_struct_->release = ReleaseExportedArray;
  }

  // Type-specific visitors

  Status Visit(const DataType& type) { return ExportingNotImplemented(type); }

  Status Visit(const NullType& type) {
    export_.format_ = "n";
    return Status::OK();
  }

  Status Visit(const BooleanType& type) {
    export_.format_ = "b";
    return Status::OK();
  }

  Status Visit(const Int8Type& type) {
    export_.format_ = "c";
    return Status::OK();
  }

  Status Visit(const UInt8Type& type) {
    export_.format_ = "C";
    return Status::OK();
  }

  Status Visit(const Int16Type& type) {
    export_.format_ = "s";
    return Status::OK();
  }

  Status Visit(const UInt16Type& type) {
    export_.format_ = "S";
    return Status::OK();
  }

  Status Visit(const Int32Type& type) {
    export_.format_ = "i";
    return Status::OK();
  }

  Status Visit(const UInt32Type& type) {
    export_.format_ = "I";
    return Status::OK();
  }

  Status Visit(const Int64Type& type) {
    export_.format_ = "l";
    return Status::OK();
  }

  Status Visit(const UInt64Type& type) {
    export_.format_ = "L";
    return Status::OK();
  }

  Status Visit(const HalfFloatType& type) {
    export_.format_ = "e";
    return Status::OK();
  }

  Status Visit(const FloatType& type) {
    export_.format_ = "f";
    return Status::OK();
  }

  Status Visit(const DoubleType& type) {
    export_.format_ = "g";
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType& type) {
    export_.format_ = "w:" + std::to_string(type.byte_width());
    return Status::OK();
  }

  Status Visit(const Decimal128Type& type) {
    export_.format_ =
        "d:" + std::to_string(type.precision()) + "," + std::to_string(type.scale());
    return Status::OK();
  }

  Status Visit(const BinaryType& type) {
    export_.format_ = "z";
    return Status::OK();
  }

  Status Visit(const LargeBinaryType& type) {
    export_.format_ = "Z";
    return Status::OK();
  }

  Status Visit(const StringType& type) {
    export_.format_ = "u";
    return Status::OK();
  }

  Status Visit(const LargeStringType& type) {
    export_.format_ = "U";
    return Status::OK();
  }

  Status Visit(const Date32Type& type) {
    export_.format_ = "tdD";
    return Status::OK();
  }

  Status Visit(const Date64Type& type) {
    export_.format_ = "tdm";
    return Status::OK();
  }

  Status Visit(const Time32Type& type) {
    switch (type.unit()) {
      case TimeUnit::SECOND:
        export_.format_ = "tts";
        break;
      case TimeUnit::MILLI:
        export_.format_ = "ttm";
        break;
      default:
        return Status::Invalid("Invalid time unit for Time32: ", type.unit());
    }
    return Status::OK();
  }

  Status Visit(const Time64Type& type) {
    switch (type.unit()) {
      case TimeUnit::MICRO:
        export_.format_ = "ttu";
        break;
      case TimeUnit::NANO:
        export_.format_ = "ttn";
        break;
      default:
        return Status::Invalid("Invalid time unit for Time64: ", type.unit());
    }
    return Status::OK();
  }

  Status Visit(const TimestampType& type) {
    switch (type.unit()) {
      case TimeUnit::SECOND:
        export_.format_ = "tss:";
        break;
      case TimeUnit::MILLI:
        export_.format_ = "tsm:";
        break;
      case TimeUnit::MICRO:
        export_.format_ = "tsu:";
        break;
      case TimeUnit::NANO:
        export_.format_ = "tsn:";
        break;
      default:
        return Status::Invalid("Invalid time unit for Timestamp: ", type.unit());
    }
    export_.format_ += type.timezone();
    return Status::OK();
  }

  Status Visit(const DurationType& type) {
    switch (type.unit()) {
      case TimeUnit::SECOND:
        export_.format_ = "tDs";
        break;
      case TimeUnit::MILLI:
        export_.format_ = "tDm";
        break;
      case TimeUnit::MICRO:
        export_.format_ = "tDu";
        break;
      case TimeUnit::NANO:
        export_.format_ = "tDn";
        break;
      default:
        return Status::Invalid("Invalid time unit for Duration: ", type.unit());
    }
    return Status::OK();
  }

  Status Visit(const MonthIntervalType& type) {
    export_.format_ = "tiM";
    return Status::OK();
  }

  Status Visit(const DayTimeIntervalType& type) {
    export_.format_ = "tiD";
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    export_.format_ = "+l";
    return Status::OK();
  }

  Status Visit(const LargeListType& type) {
    export_.format_ = "+L";
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    export_.format_ = "+w:" + std::to_string(type.list_size());
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    export_.format_ = "+s";
    return Status::OK();
  }

  Status Visit(const MapType& type) {
    export_.format_ = "+m";
    if (type.keys_sorted()) {
      flags_ |= ARROW_FLAG_KEYS_SORTED;
    }
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    std::string& s = export_.format_;
    s = "+u";
    if (type.mode() == UnionMode::DENSE) {
      s += "d:";
    } else {
      DCHECK_EQ(type.mode(), UnionMode::SPARSE);
      s += "s:";
    }
    bool first = true;
    for (const auto code : type.type_codes()) {
      if (!first) {
        s += ",";
      }
      s += std::to_string(code);
      first = false;
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    // Dictionary array is exported as its index array
    return VisitTypeInline(*type.index_type(), this);
  }

  std::shared_ptr<ArrayData> data_;
  ExportedArrayPrivateData export_;
  int64_t flags_;
  std::unique_ptr<ArrayExporter> dict_exporter_;
  std::vector<ArrayExporter> child_exporters_;
};

}  // namespace

Status ExportArray(const Array& array, struct ArrowArray* out) {
  ArrayExporter exporter{array.data()};
  RETURN_NOT_OK(exporter.Export());
  exporter.Finish(out);
  return Status::OK();
}

Status ExportArray(const Field& field, const Array& array, struct ArrowArray* out) {
  ArrayExporter exporter{array.data()};
  RETURN_NOT_OK(exporter.Export(&field));
  exporter.Finish(out);
  return Status::OK();
}

Status ExportRecordBatch(const RecordBatch& batch, struct ArrowArray* out) {
  std::shared_ptr<Array> array;
  RETURN_NOT_OK(batch.ToStructArray(&array));
  auto field = ::arrow::field("", array->type(), /*nullable=*/false);
  if (batch.schema()->HasMetadata()) {
    field = field->WithMetadata(batch.schema()->metadata());
  }
  return ExportArray(*field, *array, out);
}

//////////////////////////////////////////////////////////////////////////
// C data import

namespace {

Status InvalidFormatString(util::string_view v) {
  return Status::Invalid("Invalid or unsupported format string: '", v, "'");
}

class FormatStringParser {
 public:
  FormatStringParser() {}

  explicit FormatStringParser(util::string_view v) : view_(v), index_(0) {}

  bool AtEnd() const { return index_ >= view_.length(); }

  char Next() { return view_[index_++]; }

  util::string_view Rest() { return view_.substr(index_); }

  Status CheckNext(char c) {
    if (AtEnd() || Next() != c) {
      return Invalid();
    }
    return Status::OK();
  }

  Status CheckHasNext() {
    if (AtEnd()) {
      return Invalid();
    }
    return Status::OK();
  }

  Status CheckAtEnd() {
    if (!AtEnd()) {
      return Invalid();
    }
    return Status::OK();
  }

  template <typename IntType>
  Status ParseInt(util::string_view v, IntType* out) {
    using ArrowIntType = typename CTypeTraits<IntType>::ArrowType;
    internal::StringConverter<ArrowIntType> converter;
    if (!converter(v.data(), v.size(), out)) {
      return Invalid();
    }
    return Status::OK();
  }

  Status ParseTimeUnit(TimeUnit::type* out) {
    RETURN_NOT_OK(CheckHasNext());
    switch (Next()) {
      case 's':
        *out = TimeUnit::SECOND;
        break;
      case 'm':
        *out = TimeUnit::MILLI;
        break;
      case 'u':
        *out = TimeUnit::MICRO;
        break;
      case 'n':
        *out = TimeUnit::NANO;
        break;
      default:
        return Invalid();
    }
    return Status::OK();
  }

  std::vector<util::string_view> Split(util::string_view v, char delim = ',') {
    std::vector<util::string_view> parts;
    size_t start = 0, end;
    while (true) {
      end = v.find_first_of(delim, start);
      parts.push_back(v.substr(start, end - start));
      if (end == util::string_view::npos) {
        break;
      }
      start = end + 1;
    }
    return parts;
  }

  template <typename IntType>
  Status ParseInts(util::string_view v, std::vector<IntType>* out) {
    auto parts = Split(v);
    std::vector<IntType> result;
    result.reserve(parts.size());
    for (const auto& p : parts) {
      IntType i;
      RETURN_NOT_OK(ParseInt(p, &i));
      result.push_back(i);
    }
    *out = std::move(result);
    return Status::OK();
  }

  Status Invalid() { return InvalidFormatString(view_); }

 protected:
  util::string_view view_;
  size_t index_;
};

Status DecodeMetadata(const char* metadata, std::shared_ptr<KeyValueMetadata>* out) {
  auto read_int32 = [&](int32_t* out) -> Status {
    int32_t v;
    memcpy(&v, metadata, 4);
    metadata += 4;
    *out = BitUtil::FromLittleEndian(v);
    if (*out < 0) {
      return Status::Invalid("Invalid encoded metadata string");
    }
    return Status::OK();
  };

  auto read_string = [&](std::string* out) -> Status {
    int32_t len;
    RETURN_NOT_OK(read_int32(&len));
    out->resize(len);
    if (len > 0) {
      memcpy(&(*out)[0], metadata, len);
      metadata += len;
    }
    return Status::OK();
  };

  out->reset();
  if (metadata == nullptr) {
    return Status::OK();
  }
  int32_t npairs;
  RETURN_NOT_OK(read_int32(&npairs));
  if (npairs == 0) {
    return Status::OK();
  }
  std::vector<std::string> keys(npairs);
  std::vector<std::string> values(npairs);
  for (int32_t i = 0; i < npairs; ++i) {
    RETURN_NOT_OK(read_string(&keys[i]));
    RETURN_NOT_OK(read_string(&values[i]));
  }
  *out = key_value_metadata(std::move(keys), std::move(values));
  return Status::OK();
}

// A wrapper struct for an imported C ArrowArray.
// The ArrowArray is released on destruction.
struct ImportedArrayData {
  struct ArrowArray array_;

  ImportedArrayData() {
    array_.format = nullptr;  // Initially released
  }

  void Release() {
    if (array_.format != nullptr && array_.release != nullptr) {
      array_.release(&array_);
      array_.format = nullptr;
    }
  }

  ~ImportedArrayData() { Release(); }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ImportedArrayData);
};

// A buffer wrapping an imported piece of data.
class ImportedBuffer : public Buffer {
 public:
  ImportedBuffer(const uint8_t* data, int64_t size,
                 std::shared_ptr<ImportedArrayData> import)
      : Buffer(data, size), import_(std::move(import)) {}

  ~ImportedBuffer() override {}

 protected:
  std::shared_ptr<ImportedArrayData> import_;
};

static constexpr int64_t kMaxImportRecursionLevel = 64;

struct ArrayImporter {
  ArrayImporter() {}

  Status Import(struct ArrowArray* src) {
    if (src->format == nullptr) {
      return Status::Invalid("Cannot import released ArrowArray");
    }
    recursion_level_ = 0;
    import_ = std::make_shared<ImportedArrayData>();
    c_struct_ = &import_->array_;
    ArrowMoveArray(src, c_struct_);
    total_offset_ = c_struct_->offset;
    return DoImport();
  }

  Status MakeField(std::shared_ptr<Field>* out) {
    std::shared_ptr<KeyValueMetadata> metadata;
    RETURN_NOT_OK(DecodeMetadata(c_struct_->metadata, &metadata));
    const char* name = c_struct_->name ? c_struct_->name : "";
    bool nullable = (c_struct_->flags & ARROW_FLAG_NULLABLE) != 0;
    *out = field(name, data_->type, nullable, metadata);
    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) {
    if (dict_importer_ != nullptr) {
      std::shared_ptr<Array> indices, values;
      indices = MakeArray(data_);
      RETURN_NOT_OK(dict_importer_->Finish(&values));
      bool ordered = (c_struct_->flags & ARROW_FLAG_ORDERED) != 0;
      auto type = dictionary(indices->type(), values->type(), ordered);
      *out = std::make_shared<DictionaryArray>(type, indices, values);
    } else {
      *out = MakeArray(data_);
    }
    return Status::OK();
  }

 protected:
  Status ImportChild(const ArrayImporter* parent, struct ArrowArray* src) {
    if (src->format == nullptr) {
      return Status::Invalid("Cannot import released ArrowArray");
    }
    recursion_level_ = parent->recursion_level_ + 1;
    if (recursion_level_ >= kMaxImportRecursionLevel) {
      return Status::Invalid("Recursion level in ArrowArray struct exceeded");
    }
    // Child buffers keep the entire parent import alive.
    // Perhaps we can move the child structs to an owned area
    // when the parent ImportedArrayData::Release() gets called,
    // but that is another level of complication.
    import_ = parent->import_;
    // The ArrowArray shouldn't be moved, it's owned by its parent
    c_struct_ = src;
    total_offset_ = parent->total_offset_ + c_struct_->offset;
    return DoImport();
  }

  Status ImportDict(const ArrayImporter* parent, struct ArrowArray* src) {
    if (src->format == nullptr) {
      return Status::Invalid("Cannot import released ArrowArray");
    }
    recursion_level_ = parent->recursion_level_ + 1;
    if (recursion_level_ >= kMaxImportRecursionLevel) {
      return Status::Invalid("Recursion level in ArrowArray struct exceeded");
    }
    import_ = parent->import_;
    c_struct_ = src;
    // Parent offset is not inherited
    total_offset_ = c_struct_->offset;
    return DoImport();
  }

  Status DoImport() {
    // First import children (required for reconstituting parent type)
    for (int64_t i = 0; i < c_struct_->n_children; ++i) {
      child_importers_.emplace_back(new ArrayImporter);
      RETURN_NOT_OK(child_importers_.back()->ImportChild(this, c_struct_->children[i]));
    }

    // Import main data
    f_parser_ = FormatStringParser(c_struct_->format);
    RETURN_NOT_OK(ProcessFormat());

    // Import dictionary values
    if (c_struct_->dictionary != nullptr) {
      // Check this index type
      bool indices_ok = false;
      if (is_integer(data_->type->id())) {
        indices_ok = checked_cast<const IntegerType&>(*data_->type).is_signed();
      }
      if (!indices_ok) {
        return Status::Invalid(
            "ArrowArray struct has a dictionary but is not a signed integer type: ",
            data_->type);
      }
      dict_importer_.reset(new ArrayImporter);
      RETURN_NOT_OK(dict_importer_->ImportDict(this, c_struct_->dictionary));
    }
    return Status::OK();
  }

  Status ProcessFormat() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'n':
        return ProcessNull();
      case 'b':
        return ProcessPrimitive(boolean());
      case 'c':
        return ProcessPrimitive(int8());
      case 'C':
        return ProcessPrimitive(uint8());
      case 's':
        return ProcessPrimitive(int16());
      case 'S':
        return ProcessPrimitive(uint16());
      case 'i':
        return ProcessPrimitive(int32());
      case 'I':
        return ProcessPrimitive(uint32());
      case 'l':
        return ProcessPrimitive(int64());
      case 'L':
        return ProcessPrimitive(uint64());
      case 'e':
        return ProcessPrimitive(float16());
      case 'f':
        return ProcessPrimitive(float32());
      case 'g':
        return ProcessPrimitive(float64());
      case 'u':
        return ProcessStringLike<int32_t>(utf8());
      case 'U':
        return ProcessStringLike<int64_t>(large_utf8());
      case 'z':
        return ProcessStringLike<int32_t>(binary());
      case 'Z':
        return ProcessStringLike<int64_t>(large_binary());
      case 'w':
        return ProcessFixedSizeBinary();
      case 'd':
        return ProcessDecimal();
      case 't':
        return ProcessTemporal();
      case '+':
        return ProcessNested();
    }
    return f_parser_.Invalid();
  }

  Status ProcessTemporal() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'd':
        return ProcessDate();
      case 't':
        return ProcessTime();
      case 'D':
        return ProcessDuration();
      case 'i':
        return ProcessInterval();
      case 's':
        return ProcessTimestamp();
    }
    return f_parser_.Invalid();
  }

  Status ProcessNested() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'l':
        return ProcessListLike<ListType>();
      case 'L':
        return ProcessListLike<LargeListType>();
      case 'w':
        return ProcessFixedSizeList();
      case 's':
        return ProcessStruct();
      case 'm':
        return ProcessMap();
      case 'u':
        return ProcessUnion();
    }
    return f_parser_.Invalid();
  }

  Status ProcessDate() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'D':
        return ProcessPrimitive(date32());
      case 'm':
        return ProcessPrimitive(date64());
    }
    return f_parser_.Invalid();
  }

  Status ProcessInterval() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'D':
        return ProcessPrimitive(day_time_interval());
      case 'M':
        return ProcessPrimitive(month_interval());
    }
    return f_parser_.Invalid();
  }

  Status ProcessTime() {
    TimeUnit::type unit;
    RETURN_NOT_OK(f_parser_.ParseTimeUnit(&unit));
    if (unit == TimeUnit::SECOND || unit == TimeUnit::MILLI) {
      return ProcessPrimitive(time32(unit));
    } else {
      return ProcessPrimitive(time64(unit));
    }
  }

  Status ProcessDuration() {
    TimeUnit::type unit;
    RETURN_NOT_OK(f_parser_.ParseTimeUnit(&unit));
    return ProcessPrimitive(duration(unit));
  }

  Status ProcessTimestamp() {
    TimeUnit::type unit;
    RETURN_NOT_OK(f_parser_.ParseTimeUnit(&unit));
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    return ImportFixedSizePrimitive(timestamp(unit, std::string(f_parser_.Rest())));
  }

  Status ProcessFixedSizeBinary() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    int32_t byte_width = -1;
    RETURN_NOT_OK(f_parser_.ParseInt(f_parser_.Rest(), &byte_width));
    if (byte_width < 0) {
      return f_parser_.Invalid();
    }
    return ImportFixedSizePrimitive(fixed_size_binary(byte_width));
  }

  Status ProcessDecimal() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    std::vector<int32_t> prec_scale;
    RETURN_NOT_OK(f_parser_.ParseInts(f_parser_.Rest(), &prec_scale));
    if (prec_scale.size() != 2) {
      return f_parser_.Invalid();
    }
    if (prec_scale[0] <= 0 || prec_scale[1] <= 0) {
      return f_parser_.Invalid();
    }
    return ImportFixedSizePrimitive(decimal(prec_scale[0], prec_scale[1]));
  }

  Status ProcessPrimitive(const std::shared_ptr<DataType>& type) {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    return ImportFixedSizePrimitive(type);
  }

  Status ProcessNull() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    auto type = null();
    RETURN_NOT_OK(CheckNoChildren(type));
    // XXX should we be lenient on the number of buffers?
    RETURN_NOT_OK(CheckNumBuffers(type, 1));
    RETURN_NOT_OK(AllocateArrayData(type));
    RETURN_NOT_OK(ImportBitsBuffer(0));
    return Status::OK();
  }

  template <typename OffsetType>
  Status ProcessStringLike(const std::shared_ptr<DataType>& type) {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    return ImportStringLike<OffsetType>(type);
  }

  template <typename ListType>
  Status ProcessListLike() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    RETURN_NOT_OK(CheckNumChildren(1));
    std::shared_ptr<Field> field;
    RETURN_NOT_OK(MakeChildField(0, &field));
    auto type = std::make_shared<ListType>(field);
    RETURN_NOT_OK(CheckNumBuffers(type, 2));
    RETURN_NOT_OK(AllocateArrayData(type));
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportOffsetsBuffer<typename ListType::offset_type>(1));
    return Status::OK();
  }

  Status ProcessMap() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    RETURN_NOT_OK(CheckNumChildren(1));
    std::shared_ptr<Field> field;
    RETURN_NOT_OK(MakeChildField(0, &field));
    const auto& value_type = field->type();
    if (value_type->id() != Type::STRUCT) {
      return Status::Invalid("Imported map array has unexpected child field type: ",
                             field->ToString());
    }
    if (value_type->num_children() != 2) {
      return Status::Invalid("Imported map array has unexpected child field type: ",
                             field->ToString());
    }

    bool keys_sorted = (c_struct_->flags & ARROW_FLAG_KEYS_SORTED);
    auto type =
        map(value_type->child(0)->type(), value_type->child(1)->type(), keys_sorted);
    // Process buffers as for ListType
    RETURN_NOT_OK(CheckNumBuffers(type, 2));
    RETURN_NOT_OK(AllocateArrayData(type));
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportOffsetsBuffer<typename MapType::offset_type>(1));
    return Status::OK();
  }

  Status ProcessFixedSizeList() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    int32_t list_size = -1;
    RETURN_NOT_OK(f_parser_.ParseInt(f_parser_.Rest(), &list_size));
    if (list_size < 0) {
      return f_parser_.Invalid();
    }
    RETURN_NOT_OK(CheckNumChildren(1));
    std::shared_ptr<Field> field;
    RETURN_NOT_OK(MakeChildField(0, &field));
    auto type = fixed_size_list(field, list_size);
    RETURN_NOT_OK(CheckNumBuffers(type, 1));
    RETURN_NOT_OK(AllocateArrayData(type));
    RETURN_NOT_OK(ImportNullBitmap());
    return Status::OK();
  }

  Status ProcessStruct() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    std::vector<std::shared_ptr<Field>> fields;
    RETURN_NOT_OK(MakeChildFields(&fields));
    auto type = struct_(std::move(fields));
    RETURN_NOT_OK(CheckNumBuffers(type, 1));
    RETURN_NOT_OK(AllocateArrayData(type));
    RETURN_NOT_OK(ImportNullBitmap());
    return Status::OK();
  }

  Status ProcessUnion() {
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    UnionMode::type mode;
    switch (f_parser_.Next()) {
      case 'd':
        mode = UnionMode::DENSE;
        break;
      case 's':
        mode = UnionMode::SPARSE;
        break;
      default:
        return f_parser_.Invalid();
    }
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    std::vector<int8_t> type_codes;
    RETURN_NOT_OK(f_parser_.ParseInts(f_parser_.Rest(), &type_codes));
    std::vector<std::shared_ptr<Field>> fields;
    RETURN_NOT_OK(MakeChildFields(&fields));
    if (fields.size() != type_codes.size()) {
      return Status::Invalid(
          "ArrowArray struct number of children incompatible with format string '",
          c_struct_->format, "'");
    }
    auto type = union_(std::move(fields), std::move(type_codes), mode);
    RETURN_NOT_OK(CheckNumBuffers(type, 3));
    RETURN_NOT_OK(AllocateArrayData(type));
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportFixedSizeBuffer(1, sizeof(int8_t)));
    if (mode == UnionMode::DENSE) {
      RETURN_NOT_OK(ImportFixedSizeBuffer(2, sizeof(int32_t)));
    } else {
      RETURN_NOT_OK(ImportUnusedBuffer(2));
    }
    return Status::OK();
  }

  Status ImportFixedSizePrimitive(const std::shared_ptr<DataType>& type) {
    const auto& fw_type = checked_cast<const FixedWidthType&>(*type);
    RETURN_NOT_OK(CheckNoChildren(type));
    RETURN_NOT_OK(CheckNumBuffers(type, 2));
    RETURN_NOT_OK(AllocateArrayData(type));
    RETURN_NOT_OK(ImportNullBitmap());
    if (BitUtil::IsMultipleOf8(fw_type.bit_width())) {
      RETURN_NOT_OK(ImportFixedSizeBuffer(1, fw_type.bit_width() / 8));
    } else {
      DCHECK_EQ(fw_type.bit_width(), 1);
      RETURN_NOT_OK(ImportBitsBuffer(1));
    }
    return Status::OK();
  }

  template <typename OffsetType>
  Status ImportStringLike(const std::shared_ptr<DataType>& type) {
    RETURN_NOT_OK(CheckNoChildren(type));
    RETURN_NOT_OK(CheckNumBuffers(type, 3));
    RETURN_NOT_OK(AllocateArrayData(type));
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportOffsetsBuffer<OffsetType>(1));
    RETURN_NOT_OK(ImportStringValuesBuffer<OffsetType>(1, 2));
    return Status::OK();
  }

  Status MakeChildField(int64_t child_id, std::shared_ptr<Field>* out) {
    ArrayImporter& child = *child_importers_[child_id];
    if (child.c_struct_->name == nullptr) {
      return Status::Invalid("Expected non-null name in imported array child");
    }
    return child.MakeField(out);
  }

  Status MakeChildFields(std::vector<std::shared_ptr<Field>>* out) {
    std::vector<std::shared_ptr<Field>> fields(child_importers_.size());
    for (int64_t i = 0; i < static_cast<int64_t>(child_importers_.size()); ++i) {
      RETURN_NOT_OK(MakeChildField(i, &fields[i]));
    }
    *out = std::move(fields);
    return Status::OK();
  }

  Status CheckNoChildren(const std::shared_ptr<DataType>& type) {
    return CheckNumChildren(type, 0);
  }

  Status CheckNumChildren(const std::shared_ptr<DataType>& type, int64_t n_children) {
    if (c_struct_->n_children != n_children) {
      return Status::Invalid("Expected ", n_children, " children for imported type ",
                             *type, ", ArrowArray struct has ", c_struct_->n_children);
    }
    return Status::OK();
  }

  Status CheckNumChildren(int64_t n_children) {
    if (c_struct_->n_children != n_children) {
      return Status::Invalid("Expected ", n_children, " children for imported format '",
                             c_struct_->format, "', ArrowArray struct has ",
                             c_struct_->n_children);
    }
    return Status::OK();
  }

  Status CheckNumBuffers(const std::shared_ptr<DataType>& type, int64_t n_buffers) {
    if (n_buffers != c_struct_->n_buffers) {
      return Status::Invalid("Expected ", n_buffers, " buffers for imported type ", *type,
                             ", ArrowArray struct has ", c_struct_->n_buffers);
    }
    return Status::OK();
  }

  Status AllocateArrayData(const std::shared_ptr<DataType>& type) {
    DCHECK_EQ(data_, nullptr);
    data_ = std::make_shared<ArrayData>(type, c_struct_->length, c_struct_->null_count,
                                        c_struct_->offset);
    data_->buffers.resize(static_cast<size_t>(c_struct_->n_buffers));
    data_->child_data.resize(static_cast<size_t>(c_struct_->n_children));
    DCHECK_EQ(child_importers_.size(), data_->child_data.size());
    std::transform(child_importers_.begin(), child_importers_.end(),
                   data_->child_data.begin(),
                   [](std::unique_ptr<ArrayImporter>& child) { return child->data_; });
    return Status::OK();
  }

  Status ImportNullBitmap(int32_t buffer_id = 0) {
    RETURN_NOT_OK(ImportBitsBuffer(buffer_id));
    if (data_->null_count != 0 && data_->buffers[buffer_id] == nullptr) {
      return Status::Invalid(
          "ArrowArray struct has null bitmap buffer but non-zero null_count ",
          data_->null_count);
    }
    return Status::OK();
  }

  Status ImportBitsBuffer(int32_t buffer_id) {
    // Compute visible size of buffer
    int64_t buffer_size =
        BitUtil::RoundUpToMultipleOf8(c_struct_->length + total_offset_) / 8;
    return ImportBuffer(buffer_id, buffer_size);
  }

  Status ImportUnusedBuffer(int32_t buffer_id) { return ImportBuffer(buffer_id, 0); }

  Status ImportFixedSizeBuffer(int32_t buffer_id, int64_t byte_width) {
    // Compute visible size of buffer
    int64_t buffer_size = byte_width * (c_struct_->length + total_offset_);
    return ImportBuffer(buffer_id, buffer_size);
  }

  template <typename OffsetType>
  Status ImportOffsetsBuffer(int32_t buffer_id) {
    // Compute visible size of buffer
    int64_t buffer_size = sizeof(OffsetType) * (c_struct_->length + total_offset_ + 1);
    return ImportBuffer(buffer_id, buffer_size);
  }

  template <typename OffsetType>
  Status ImportStringValuesBuffer(int32_t offsets_buffer_id, int32_t buffer_id,
                                  int64_t byte_width = 1) {
    auto offsets = data_->GetValues<OffsetType>(offsets_buffer_id);
    // Compute visible size of buffer
    int64_t buffer_size = byte_width * offsets[c_struct_->length];
    return ImportBuffer(buffer_id, buffer_size);
  }

  Status ImportBuffer(int32_t buffer_id, int64_t buffer_size) {
    std::shared_ptr<Buffer>* out = &data_->buffers[buffer_id];
    auto data = reinterpret_cast<const uint8_t*>(c_struct_->buffers[buffer_id]);
    if (data != nullptr) {
      *out = std::make_shared<ImportedBuffer>(data, buffer_size, import_);
    } else {
      out->reset();
    }
    return Status::OK();
  }

  std::shared_ptr<ImportedArrayData> import_;
  struct ArrowArray* c_struct_;
  FormatStringParser f_parser_;
  int64_t total_offset_;  // total offset, including parent's
  int64_t recursion_level_;
  std::shared_ptr<ArrayData> data_;
  std::vector<std::unique_ptr<ArrayImporter>> child_importers_;
  std::unique_ptr<ArrayImporter> dict_importer_;
};

}  // namespace

Status ImportArray(struct ArrowArray* array, std::shared_ptr<Array>* out) {
  ArrayImporter importer;
  RETURN_NOT_OK(importer.Import(array));
  return importer.Finish(out);
}

Status ImportArray(struct ArrowArray* array, std::shared_ptr<Field>* out_field,
                   std::shared_ptr<Array>* out_array) {
  ArrayImporter importer;
  RETURN_NOT_OK(importer.Import(array));
  RETURN_NOT_OK(importer.MakeField(out_field));
  return importer.Finish(out_array);
}

Status ImportRecordBatch(struct ArrowArray* array, std::shared_ptr<RecordBatch>* out) {
  std::shared_ptr<Array> array_result;
  std::shared_ptr<Field> field_result;
  RETURN_NOT_OK(ImportArray(array, &field_result, &array_result));
  if (array_result->type_id() != Type::STRUCT) {
    return Status::Invalid("Imported array has type ", array_result->type(),
                           " but only a struct array can be converted to RecordBatch");
  }
  if (array_result->null_count() != 0) {
    return Status::Invalid("Imported array has nulls, cannot convert to RecordBatch");
  }
  if (array_result->offset() != 0) {
    return Status::Invalid(
        "Imported array has non-zero offset, "
        "cannot convert to RecordBatch");
  }
  RETURN_NOT_OK(RecordBatch::FromStructArray(array_result, out));
  *out = (*out)->ReplaceSchemaMetadata(field_result->metadata());
  return Status::OK();
}

}  // namespace arrow
