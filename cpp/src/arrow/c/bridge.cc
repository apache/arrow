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
#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/c/helpers.h"
#include "arrow/c/util_internal.h"
#include "arrow/extension_type.h"
#include "arrow/memory_pool.h"
#include "arrow/memory_pool_internal.h"  // for kZeroSizeArea
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/stl_allocator.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/small_vector.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

using internal::SmallVector;
using internal::StaticVector;

using internal::ArrayExportGuard;
using internal::ArrayExportTraits;
using internal::SchemaExportGuard;
using internal::SchemaExportTraits;

using internal::ToChars;

using memory_pool::internal::kZeroSizeArea;

namespace {

Status ExportingNotImplemented(const DataType& type) {
  return Status::NotImplemented("Exporting ", type.ToString(), " array not supported");
}

// Allocate exported private data using MemoryPool,
// to allow accounting memory and checking for memory leaks.

// XXX use Gandiva's SimpleArena?

template <typename Derived>
struct PoolAllocationMixin {
  static void* operator new(size_t size) {
    DCHECK_EQ(size, sizeof(Derived));
    uint8_t* data;
    ARROW_CHECK_OK(default_memory_pool()->Allocate(static_cast<int64_t>(size), &data));
    return data;
  }

  static void operator delete(void* ptr) {
    default_memory_pool()->Free(reinterpret_cast<uint8_t*>(ptr), sizeof(Derived));
  }
};

//////////////////////////////////////////////////////////////////////////
// C schema export

struct ExportedSchemaPrivateData : PoolAllocationMixin<ExportedSchemaPrivateData> {
  std::string format_;
  std::string name_;
  std::string metadata_;
  struct ArrowSchema dictionary_;
  SmallVector<struct ArrowSchema, 1> children_;
  SmallVector<struct ArrowSchema*, 4> child_pointers_;

  ExportedSchemaPrivateData() = default;
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ExportedSchemaPrivateData);
  ARROW_DISALLOW_COPY_AND_ASSIGN(ExportedSchemaPrivateData);
};

void ReleaseExportedSchema(struct ArrowSchema* schema) {
  if (ArrowSchemaIsReleased(schema)) {
    return;
  }
  for (int64_t i = 0; i < schema->n_children; ++i) {
    struct ArrowSchema* child = schema->children[i];
    ArrowSchemaRelease(child);
    DCHECK(ArrowSchemaIsReleased(child))
        << "Child release callback should have marked it released";
  }
  struct ArrowSchema* dict = schema->dictionary;
  if (dict != nullptr) {
    ArrowSchemaRelease(dict);
    DCHECK(ArrowSchemaIsReleased(dict))
        << "Dictionary release callback should have marked it released";
  }
  DCHECK_NE(schema->private_data, nullptr);
  delete reinterpret_cast<ExportedSchemaPrivateData*>(schema->private_data);

  ArrowSchemaMarkReleased(schema);
}

template <typename SizeType>
Result<int32_t> DowncastMetadataSize(SizeType size) {
  auto res = static_cast<int32_t>(size);
  if (res < 0 || static_cast<SizeType>(res) != size) {
    return Status::Invalid("Metadata too large (more than 2**31 items or bytes)");
  }
  return res;
}

Result<std::string> EncodeMetadata(const KeyValueMetadata& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto npairs, DowncastMetadataSize(metadata.size()));
  std::string exported;

  // Pre-compute total string size
  size_t total_size = 4;
  for (int32_t i = 0; i < npairs; ++i) {
    total_size += 8 + metadata.key(i).length() + metadata.value(i).length();
  }
  exported.resize(total_size);

  char* data_start = &exported[0];
  char* data = data_start;
  auto write_int32 = [&](int32_t v) -> void {
    memcpy(data, &v, 4);
    data += 4;
  };
  auto write_string = [&](const std::string& s) -> Status {
    ARROW_ASSIGN_OR_RAISE(auto len, DowncastMetadataSize(s.length()));
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
  return exported;
}

struct SchemaExporter {
  Status ExportField(const Field& field) {
    export_.name_ = field.name();
    flags_ = field.nullable() ? ARROW_FLAG_NULLABLE : 0;

    const DataType* type = UnwrapExtension(field.type().get());
    RETURN_NOT_OK(ExportFormat(*type));
    RETURN_NOT_OK(ExportChildren(type->fields()));
    RETURN_NOT_OK(ExportMetadata(field.metadata().get()));
    return Status::OK();
  }

  Status ExportType(const DataType& orig_type) {
    flags_ = ARROW_FLAG_NULLABLE;

    const DataType* type = UnwrapExtension(&orig_type);
    RETURN_NOT_OK(ExportFormat(*type));
    RETURN_NOT_OK(ExportChildren(type->fields()));
    // There may be additional metadata to export
    RETURN_NOT_OK(ExportMetadata(nullptr));
    return Status::OK();
  }

  Status ExportSchema(const Schema& schema) {
    static const StructType dummy_struct_type({});
    flags_ = 0;

    RETURN_NOT_OK(ExportFormat(dummy_struct_type));
    RETURN_NOT_OK(ExportChildren(schema.fields()));
    RETURN_NOT_OK(ExportMetadata(schema.metadata().get()));
    return Status::OK();
  }

  // Finalize exporting by setting C struct fields and allocating
  // autonomous private data for each schema node.
  //
  // This function can't fail, as properly reclaiming memory in case of error
  // would be too fragile.  After this function returns, memory is reclaimed
  // by calling the release() pointer in the top level ArrowSchema struct.
  void Finish(struct ArrowSchema* c_struct) {
    // First, create permanent ExportedSchemaPrivateData
    auto pdata = new ExportedSchemaPrivateData(std::move(export_));

    // Second, finish dictionary and children.
    if (dict_exporter_) {
      dict_exporter_->Finish(&pdata->dictionary_);
    }
    pdata->child_pointers_.resize(child_exporters_.size(), nullptr);
    for (size_t i = 0; i < child_exporters_.size(); ++i) {
      auto ptr = pdata->child_pointers_[i] = &pdata->children_[i];
      child_exporters_[i].Finish(ptr);
    }

    // Third, fill C struct.
    DCHECK_NE(c_struct, nullptr);
    memset(c_struct, 0, sizeof(*c_struct));

    c_struct->format = pdata->format_.c_str();
    c_struct->name = pdata->name_.c_str();
    c_struct->metadata = pdata->metadata_.empty() ? nullptr : pdata->metadata_.c_str();
    c_struct->flags = flags_;

    c_struct->n_children = static_cast<int64_t>(child_exporters_.size());
    c_struct->children = c_struct->n_children ? pdata->child_pointers_.data() : nullptr;
    c_struct->dictionary = dict_exporter_ ? &pdata->dictionary_ : nullptr;
    c_struct->private_data = pdata;
    c_struct->release = ReleaseExportedSchema;
  }

  const DataType* UnwrapExtension(const DataType* type) {
    if (type->id() == Type::EXTENSION) {
      const auto& ext_type = checked_cast<const ExtensionType&>(*type);
      additional_metadata_.reserve(2);
      additional_metadata_.emplace_back(kExtensionTypeKeyName, ext_type.extension_name());
      additional_metadata_.emplace_back(kExtensionMetadataKeyName, ext_type.Serialize());
      return ext_type.storage_type().get();
    }
    return type;
  }

  Status ExportFormat(const DataType& type) {
    if (type.id() == Type::DICTIONARY) {
      const auto& dict_type = checked_cast<const DictionaryType&>(type);
      if (dict_type.ordered()) {
        flags_ |= ARROW_FLAG_DICTIONARY_ORDERED;
      }
      // Dictionary type: parent struct describes index type,
      // child dictionary struct describes value type.
      RETURN_NOT_OK(VisitTypeInline(*dict_type.index_type(), this));
      dict_exporter_.reset(new SchemaExporter());
      RETURN_NOT_OK(dict_exporter_->ExportType(*dict_type.value_type()));
    } else {
      RETURN_NOT_OK(VisitTypeInline(type, this));
    }
    DCHECK(!export_.format_.empty());
    return Status::OK();
  }

  Status ExportChildren(const std::vector<std::shared_ptr<Field>>& fields) {
    export_.children_.resize(fields.size());
    child_exporters_.resize(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
      RETURN_NOT_OK(child_exporters_[i].ExportField(*fields[i]));
    }
    return Status::OK();
  }

  Status ExportMetadata(const KeyValueMetadata* orig_metadata) {
    static const KeyValueMetadata empty_metadata;

    if (orig_metadata == nullptr) {
      orig_metadata = &empty_metadata;
    }
    if (additional_metadata_.empty()) {
      if (orig_metadata->size() > 0) {
        ARROW_ASSIGN_OR_RAISE(export_.metadata_, EncodeMetadata(*orig_metadata));
      }
      return Status::OK();
    }
    // Additional metadata needs to be appended to the existing
    // (for extension types)
    KeyValueMetadata metadata(orig_metadata->keys(), orig_metadata->values());
    for (const auto& kv : additional_metadata_) {
      // The metadata may already be there => ignore
      if (metadata.Contains(kv.first)) {
        continue;
      }
      metadata.Append(kv.first, kv.second);
    }
    ARROW_ASSIGN_OR_RAISE(export_.metadata_, EncodeMetadata(metadata));
    return Status::OK();
  }

  Status SetFormat(std::string s) {
    export_.format_ = std::move(s);
    return Status::OK();
  }

  // Type-specific visitors

  Status Visit(const DataType& type) { return ExportingNotImplemented(type); }

  Status Visit(const NullType& type) { return SetFormat("n"); }

  Status Visit(const BooleanType& type) { return SetFormat("b"); }

  Status Visit(const Int8Type& type) { return SetFormat("c"); }

  Status Visit(const UInt8Type& type) { return SetFormat("C"); }

  Status Visit(const Int16Type& type) { return SetFormat("s"); }

  Status Visit(const UInt16Type& type) { return SetFormat("S"); }

  Status Visit(const Int32Type& type) { return SetFormat("i"); }

  Status Visit(const UInt32Type& type) { return SetFormat("I"); }

  Status Visit(const Int64Type& type) { return SetFormat("l"); }

  Status Visit(const UInt64Type& type) { return SetFormat("L"); }

  Status Visit(const HalfFloatType& type) { return SetFormat("e"); }

  Status Visit(const FloatType& type) { return SetFormat("f"); }

  Status Visit(const DoubleType& type) { return SetFormat("g"); }

  Status Visit(const FixedSizeBinaryType& type) {
    return SetFormat("w:" + ToChars(type.byte_width()));
  }

  Status Visit(const DecimalType& type) {
    if (type.bit_width() == 128) {
      // 128 is the default bit-width
      return SetFormat("d:" + ToChars(type.precision()) + "," + ToChars(type.scale()));
    } else {
      return SetFormat("d:" + ToChars(type.precision()) + "," + ToChars(type.scale()) +
                       "," + ToChars(type.bit_width()));
    }
  }

  Status Visit(const BinaryType& type) { return SetFormat("z"); }

  Status Visit(const LargeBinaryType& type) { return SetFormat("Z"); }

  Status Visit(const StringType& type) { return SetFormat("u"); }

  Status Visit(const LargeStringType& type) { return SetFormat("U"); }

  Status Visit(const Date32Type& type) { return SetFormat("tdD"); }

  Status Visit(const Date64Type& type) { return SetFormat("tdm"); }

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

  Status Visit(const MonthIntervalType& type) { return SetFormat("tiM"); }

  Status Visit(const DayTimeIntervalType& type) { return SetFormat("tiD"); }

  Status Visit(const MonthDayNanoIntervalType& type) { return SetFormat("tin"); }

  Status Visit(const ListType& type) { return SetFormat("+l"); }

  Status Visit(const LargeListType& type) { return SetFormat("+L"); }

  Status Visit(const FixedSizeListType& type) {
    return SetFormat("+w:" + ToChars(type.list_size()));
  }

  Status Visit(const StructType& type) { return SetFormat("+s"); }

  Status Visit(const MapType& type) {
    export_.format_ = "+m";
    if (type.keys_sorted()) {
      flags_ |= ARROW_FLAG_MAP_KEYS_SORTED;
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
      s += ToChars(code);
      first = false;
    }
    return Status::OK();
  }

  ExportedSchemaPrivateData export_;
  int64_t flags_ = 0;
  std::vector<std::pair<std::string, std::string>> additional_metadata_;
  std::unique_ptr<SchemaExporter> dict_exporter_;
  std::vector<SchemaExporter> child_exporters_;
};

}  // namespace

Status ExportType(const DataType& type, struct ArrowSchema* out) {
  SchemaExporter exporter;
  RETURN_NOT_OK(exporter.ExportType(type));
  exporter.Finish(out);
  return Status::OK();
}

Status ExportField(const Field& field, struct ArrowSchema* out) {
  SchemaExporter exporter;
  RETURN_NOT_OK(exporter.ExportField(field));
  exporter.Finish(out);
  return Status::OK();
}

Status ExportSchema(const Schema& schema, struct ArrowSchema* out) {
  SchemaExporter exporter;
  RETURN_NOT_OK(exporter.ExportSchema(schema));
  exporter.Finish(out);
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
// C data export

namespace {

struct ExportedArrayPrivateData : PoolAllocationMixin<ExportedArrayPrivateData> {
  // The buffers are owned by the ArrayData member
  StaticVector<const void*, 3> buffers_;
  struct ArrowArray dictionary_;
  SmallVector<struct ArrowArray, 1> children_;
  SmallVector<struct ArrowArray*, 4> child_pointers_;

  std::shared_ptr<ArrayData> data_;

  ExportedArrayPrivateData() = default;
  ARROW_DEFAULT_MOVE_AND_ASSIGN(ExportedArrayPrivateData);
  ARROW_DISALLOW_COPY_AND_ASSIGN(ExportedArrayPrivateData);
};

void ReleaseExportedArray(struct ArrowArray* array) {
  if (ArrowArrayIsReleased(array)) {
    return;
  }
  for (int64_t i = 0; i < array->n_children; ++i) {
    struct ArrowArray* child = array->children[i];
    ArrowArrayRelease(child);
    DCHECK(ArrowArrayIsReleased(child))
        << "Child release callback should have marked it released";
  }
  struct ArrowArray* dict = array->dictionary;
  if (dict != nullptr) {
    ArrowArrayRelease(dict);
    DCHECK(ArrowArrayIsReleased(dict))
        << "Dictionary release callback should have marked it released";
  }
  DCHECK_NE(array->private_data, nullptr);
  delete reinterpret_cast<ExportedArrayPrivateData*>(array->private_data);

  ArrowArrayMarkReleased(array);
}

struct ArrayExporter {
  Status Export(const std::shared_ptr<ArrayData>& data) {
    // Force computing null count.
    // This is because ARROW-9037 is in version 0.17 and 0.17.1, and they are
    // not able to import arrays without a null bitmap and null_count == -1.
    data->GetNullCount();
    // Store buffer pointers
    size_t n_buffers = data->buffers.size();
    auto buffers_begin = data->buffers.begin();
    if (n_buffers > 0 && !internal::HasValidityBitmap(data->type->id())) {
      --n_buffers;
      ++buffers_begin;
    }
    export_.buffers_.resize(n_buffers);
    std::transform(buffers_begin, data->buffers.end(), export_.buffers_.begin(),
                   [](const std::shared_ptr<Buffer>& buffer) -> const void* {
                     return buffer ? buffer->data() : nullptr;
                   });

    // Export dictionary
    if (data->dictionary != nullptr) {
      dict_exporter_.reset(new ArrayExporter());
      RETURN_NOT_OK(dict_exporter_->Export(data->dictionary));
    }

    // Export children
    export_.children_.resize(data->child_data.size());
    child_exporters_.resize(data->child_data.size());
    for (size_t i = 0; i < data->child_data.size(); ++i) {
      RETURN_NOT_OK(child_exporters_[i].Export(data->child_data[i]));
    }

    // Store owning pointer to ArrayData
    export_.data_ = data;

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
    const ArrayData& data = *pdata->data_;

    // Second, finish dictionary and children.
    if (dict_exporter_) {
      dict_exporter_->Finish(&pdata->dictionary_);
    }
    pdata->child_pointers_.resize(data.child_data.size(), nullptr);
    for (size_t i = 0; i < data.child_data.size(); ++i) {
      auto ptr = &pdata->children_[i];
      pdata->child_pointers_[i] = ptr;
      child_exporters_[i].Finish(ptr);
    }

    // Third, fill C struct.
    DCHECK_NE(c_struct_, nullptr);
    memset(c_struct_, 0, sizeof(*c_struct_));

    c_struct_->length = data.length;
    c_struct_->null_count = data.null_count;
    c_struct_->offset = data.offset;
    c_struct_->n_buffers = static_cast<int64_t>(pdata->buffers_.size());
    c_struct_->n_children = static_cast<int64_t>(pdata->child_pointers_.size());
    c_struct_->buffers = pdata->buffers_.data();
    c_struct_->children = c_struct_->n_children ? pdata->child_pointers_.data() : nullptr;
    c_struct_->dictionary = dict_exporter_ ? &pdata->dictionary_ : nullptr;
    c_struct_->private_data = pdata;
    c_struct_->release = ReleaseExportedArray;
  }

  ExportedArrayPrivateData export_;
  std::unique_ptr<ArrayExporter> dict_exporter_;
  std::vector<ArrayExporter> child_exporters_;
};

}  // namespace

Status ExportArray(const Array& array, struct ArrowArray* out,
                   struct ArrowSchema* out_schema) {
  SchemaExportGuard guard(out_schema);
  if (out_schema != nullptr) {
    RETURN_NOT_OK(ExportType(*array.type(), out_schema));
  }
  ArrayExporter exporter;
  RETURN_NOT_OK(exporter.Export(array.data()));
  exporter.Finish(out);
  guard.Detach();
  return Status::OK();
}

Status ExportRecordBatch(const RecordBatch& batch, struct ArrowArray* out,
                         struct ArrowSchema* out_schema) {
  // XXX perhaps bypass ToStructArray() for speed?
  ARROW_ASSIGN_OR_RAISE(auto array, batch.ToStructArray());

  SchemaExportGuard guard(out_schema);
  if (out_schema != nullptr) {
    // Export the schema, not the struct type, so as not to lose top-level metadata
    RETURN_NOT_OK(ExportSchema(*batch.schema(), out_schema));
  }
  ArrayExporter exporter;
  RETURN_NOT_OK(exporter.Export(array->data()));
  exporter.Finish(out);
  guard.Detach();
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
// C schema import

namespace {

static constexpr int64_t kMaxImportRecursionLevel = 64;

Status InvalidFormatString(std::string_view v) {
  return Status::Invalid("Invalid or unsupported format string: '", v, "'");
}

class FormatStringParser {
 public:
  FormatStringParser() {}

  explicit FormatStringParser(std::string_view v) : view_(v), index_(0) {}

  bool AtEnd() const { return index_ >= view_.length(); }

  char Next() { return view_[index_++]; }

  std::string_view Rest() { return view_.substr(index_); }

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

  template <typename IntType = int32_t>
  Result<IntType> ParseInt(std::string_view v) {
    using ArrowIntType = typename CTypeTraits<IntType>::ArrowType;
    IntType value;
    if (!internal::ParseValue<ArrowIntType>(v.data(), v.size(), &value)) {
      return Invalid();
    }
    return value;
  }

  Result<TimeUnit::type> ParseTimeUnit() {
    RETURN_NOT_OK(CheckHasNext());
    switch (Next()) {
      case 's':
        return TimeUnit::SECOND;
      case 'm':
        return TimeUnit::MILLI;
      case 'u':
        return TimeUnit::MICRO;
      case 'n':
        return TimeUnit::NANO;
      default:
        return Invalid();
    }
  }

  SmallVector<std::string_view, 2> Split(std::string_view v, char delim = ',') {
    SmallVector<std::string_view, 2> parts;
    size_t start = 0, end;
    while (true) {
      end = v.find_first_of(delim, start);
      parts.push_back(v.substr(start, end - start));
      if (end == std::string_view::npos) {
        break;
      }
      start = end + 1;
    }
    return parts;
  }

  template <typename IntType = int32_t>
  Result<std::vector<IntType>> ParseInts(std::string_view v) {
    std::vector<IntType> result;
    if (v.empty()) return result;
    auto parts = Split(v);
    result.reserve(parts.size());
    for (const auto& p : parts) {
      ARROW_ASSIGN_OR_RAISE(auto i, ParseInt<IntType>(p));
      result.push_back(i);
    }
    return result;
  }

  Status Invalid() { return InvalidFormatString(view_); }

 protected:
  std::string_view view_;
  size_t index_;
};

struct DecodedMetadata {
  std::shared_ptr<KeyValueMetadata> metadata;
  std::string extension_name;
  std::string extension_serialized;
};

Result<DecodedMetadata> DecodeMetadata(const char* metadata) {
  auto read_int32 = [&](int32_t* out) -> Status {
    int32_t v;
    memcpy(&v, metadata, 4);
    metadata += 4;
    *out = v;
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

  DecodedMetadata decoded;

  if (metadata == nullptr) {
    return decoded;
  }
  int32_t npairs;
  RETURN_NOT_OK(read_int32(&npairs));
  if (npairs == 0) {
    return decoded;
  }
  std::vector<std::string> keys(npairs);
  std::vector<std::string> values(npairs);
  for (int32_t i = 0; i < npairs; ++i) {
    RETURN_NOT_OK(read_string(&keys[i]));
    RETURN_NOT_OK(read_string(&values[i]));
    if (keys[i] == kExtensionTypeKeyName) {
      decoded.extension_name = values[i];
    } else if (keys[i] == kExtensionMetadataKeyName) {
      decoded.extension_serialized = values[i];
    }
  }
  decoded.metadata = key_value_metadata(std::move(keys), std::move(values));
  return decoded;
}

struct SchemaImporter {
  SchemaImporter() : c_struct_(nullptr), guard_(nullptr) {}

  Status Import(struct ArrowSchema* src) {
    if (ArrowSchemaIsReleased(src)) {
      return Status::Invalid("Cannot import released ArrowSchema");
    }
    guard_.Reset(src);
    recursion_level_ = 0;
    c_struct_ = src;
    return DoImport();
  }

  Result<std::shared_ptr<Field>> MakeField() const {
    const char* name = c_struct_->name ? c_struct_->name : "";
    bool nullable = (c_struct_->flags & ARROW_FLAG_NULLABLE) != 0;
    return field(name, type_, nullable, std::move(metadata_.metadata));
  }

  Result<std::shared_ptr<Schema>> MakeSchema() const {
    if (type_->id() != Type::STRUCT) {
      return Status::Invalid(
          "Cannot import schema: ArrowSchema describes non-struct type ",
          type_->ToString());
    }
    return schema(type_->fields(), std::move(metadata_.metadata));
  }

  Result<std::shared_ptr<DataType>> MakeType() const { return type_; }

 protected:
  Status ImportChild(const SchemaImporter* parent, struct ArrowSchema* src) {
    if (ArrowSchemaIsReleased(src)) {
      return Status::Invalid("Cannot import released ArrowSchema");
    }
    recursion_level_ = parent->recursion_level_ + 1;
    if (recursion_level_ >= kMaxImportRecursionLevel) {
      return Status::Invalid("Recursion level in ArrowSchema struct exceeded");
    }
    // The ArrowSchema is owned by its parent, so don't release it ourselves
    c_struct_ = src;
    return DoImport();
  }

  Status ImportDict(const SchemaImporter* parent, struct ArrowSchema* src) {
    return ImportChild(parent, src);
  }

  Status DoImport() {
    // First import children (required for reconstituting parent type)
    child_importers_.resize(c_struct_->n_children);
    for (int64_t i = 0; i < c_struct_->n_children; ++i) {
      DCHECK_NE(c_struct_->children[i], nullptr);
      RETURN_NOT_OK(child_importers_[i].ImportChild(this, c_struct_->children[i]));
    }

    // Import main type
    RETURN_NOT_OK(ProcessFormat());
    DCHECK_NE(type_, nullptr);

    // Import dictionary type
    if (c_struct_->dictionary != nullptr) {
      // Check this index type
      if (!is_integer(type_->id())) {
        return Status::Invalid(
            "ArrowSchema struct has a dictionary but is not an integer type: ",
            type_->ToString());
      }
      SchemaImporter dict_importer;
      RETURN_NOT_OK(dict_importer.ImportDict(this, c_struct_->dictionary));
      bool ordered = (c_struct_->flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0;
      type_ = dictionary(type_, dict_importer.type_, ordered);
    }

    // Import metadata
    ARROW_ASSIGN_OR_RAISE(metadata_, DecodeMetadata(c_struct_->metadata));

    // Detect extension type
    if (!metadata_.extension_name.empty()) {
      const auto registered_ext_type = GetExtensionType(metadata_.extension_name);
      if (registered_ext_type) {
        ARROW_ASSIGN_OR_RAISE(
            type_, registered_ext_type->Deserialize(std::move(type_),
                                                    metadata_.extension_serialized));
      }
    }

    return Status::OK();
  }

  Status ProcessFormat() {
    f_parser_ = FormatStringParser(c_struct_->format);
    RETURN_NOT_OK(f_parser_.CheckHasNext());
    switch (f_parser_.Next()) {
      case 'n':
        return ProcessPrimitive(null());
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
        return ProcessPrimitive(utf8());
      case 'U':
        return ProcessPrimitive(large_utf8());
      case 'z':
        return ProcessPrimitive(binary());
      case 'Z':
        return ProcessPrimitive(large_binary());
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
      case 'n':
        return ProcessPrimitive(month_day_nano_interval());
    }
    return f_parser_.Invalid();
  }

  Status ProcessTime() {
    ARROW_ASSIGN_OR_RAISE(auto unit, f_parser_.ParseTimeUnit());
    if (unit == TimeUnit::SECOND || unit == TimeUnit::MILLI) {
      return ProcessPrimitive(time32(unit));
    } else {
      return ProcessPrimitive(time64(unit));
    }
  }

  Status ProcessDuration() {
    ARROW_ASSIGN_OR_RAISE(auto unit, f_parser_.ParseTimeUnit());
    return ProcessPrimitive(duration(unit));
  }

  Status ProcessTimestamp() {
    ARROW_ASSIGN_OR_RAISE(auto unit, f_parser_.ParseTimeUnit());
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    type_ = timestamp(unit, std::string(f_parser_.Rest()));
    return Status::OK();
  }

  Status ProcessFixedSizeBinary() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    ARROW_ASSIGN_OR_RAISE(auto byte_width, f_parser_.ParseInt(f_parser_.Rest()));
    if (byte_width < 0) {
      return f_parser_.Invalid();
    }
    type_ = fixed_size_binary(byte_width);
    return Status::OK();
  }

  Status ProcessDecimal() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    ARROW_ASSIGN_OR_RAISE(auto prec_scale, f_parser_.ParseInts(f_parser_.Rest()));
    // 3 elements indicates bit width was communicated as well.
    if (prec_scale.size() != 2 && prec_scale.size() != 3) {
      return f_parser_.Invalid();
    }
    if (prec_scale[0] <= 0) {
      return f_parser_.Invalid();
    }
    if (prec_scale.size() == 2 || prec_scale[2] == 128) {
      type_ = decimal128(prec_scale[0], prec_scale[1]);
    } else if (prec_scale[2] == 256) {
      type_ = decimal256(prec_scale[0], prec_scale[1]);
    } else {
      return f_parser_.Invalid();
    }
    return Status::OK();
  }

  Status ProcessPrimitive(const std::shared_ptr<DataType>& type) {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    type_ = type;
    return CheckNoChildren(type);
  }

  template <typename ListType>
  Status ProcessListLike() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    RETURN_NOT_OK(CheckNumChildren(1));
    ARROW_ASSIGN_OR_RAISE(auto field, MakeChildField(0));
    type_ = std::make_shared<ListType>(field);
    return Status::OK();
  }

  Status ProcessMap() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    RETURN_NOT_OK(CheckNumChildren(1));
    ARROW_ASSIGN_OR_RAISE(auto field, MakeChildField(0));
    const auto& value_type = field->type();
    if (value_type->id() != Type::STRUCT) {
      return Status::Invalid("Imported map array has unexpected child field type: ",
                             field->ToString());
    }
    if (value_type->num_fields() != 2) {
      return Status::Invalid("Imported map array has unexpected child field type: ",
                             field->ToString());
    }

    bool keys_sorted = (c_struct_->flags & ARROW_FLAG_MAP_KEYS_SORTED);
    bool values_nullable = value_type->field(1)->nullable();
    // Some implementations of Arrow (such as Rust) use a non-standard field name
    // for key ("keys") and value ("values") fields. For simplicity, we override
    // them on import.
    auto values_field =
        ::arrow::field("value", value_type->field(1)->type(), values_nullable);
    type_ = map(value_type->field(0)->type(), values_field, keys_sorted);
    return Status::OK();
  }

  Status ProcessFixedSizeList() {
    RETURN_NOT_OK(f_parser_.CheckNext(':'));
    ARROW_ASSIGN_OR_RAISE(auto list_size, f_parser_.ParseInt(f_parser_.Rest()));
    if (list_size < 0) {
      return f_parser_.Invalid();
    }
    RETURN_NOT_OK(CheckNumChildren(1));
    ARROW_ASSIGN_OR_RAISE(auto field, MakeChildField(0));
    type_ = fixed_size_list(field, list_size);
    return Status::OK();
  }

  Status ProcessStruct() {
    RETURN_NOT_OK(f_parser_.CheckAtEnd());
    ARROW_ASSIGN_OR_RAISE(auto fields, MakeChildFields());
    type_ = struct_(std::move(fields));
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
    ARROW_ASSIGN_OR_RAISE(auto type_codes, f_parser_.ParseInts<int8_t>(f_parser_.Rest()));
    ARROW_ASSIGN_OR_RAISE(auto fields, MakeChildFields());
    if (fields.size() != type_codes.size()) {
      return Status::Invalid(
          "ArrowArray struct number of children incompatible with format string "
          "(mismatching number of union type codes) ",
          "'", c_struct_->format, "'");
    }
    for (const auto code : type_codes) {
      if (code < 0) {
        return Status::Invalid("Negative type code in union: format string '",
                               c_struct_->format, "'");
      }
    }
    if (mode == UnionMode::SPARSE) {
      type_ = sparse_union(std::move(fields), std::move(type_codes));
    } else {
      type_ = dense_union(std::move(fields), std::move(type_codes));
    }
    return Status::OK();
  }

  Result<std::shared_ptr<Field>> MakeChildField(int64_t child_id) {
    const auto& child = child_importers_[child_id];
    if (child.c_struct_->name == nullptr) {
      return Status::Invalid("Expected non-null name in imported array child");
    }
    return child.MakeField();
  }

  Result<std::vector<std::shared_ptr<Field>>> MakeChildFields() {
    std::vector<std::shared_ptr<Field>> fields(child_importers_.size());
    for (int64_t i = 0; i < static_cast<int64_t>(child_importers_.size()); ++i) {
      ARROW_ASSIGN_OR_RAISE(fields[i], MakeChildField(i));
    }
    return fields;
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

  struct ArrowSchema* c_struct_;
  SchemaExportGuard guard_;
  FormatStringParser f_parser_;
  int64_t recursion_level_;
  std::vector<SchemaImporter> child_importers_;
  std::shared_ptr<DataType> type_;
  DecodedMetadata metadata_;
};

}  // namespace

Result<std::shared_ptr<DataType>> ImportType(struct ArrowSchema* schema) {
  SchemaImporter importer;
  RETURN_NOT_OK(importer.Import(schema));
  return importer.MakeType();
}

Result<std::shared_ptr<Field>> ImportField(struct ArrowSchema* schema) {
  SchemaImporter importer;
  RETURN_NOT_OK(importer.Import(schema));
  return importer.MakeField();
}

Result<std::shared_ptr<Schema>> ImportSchema(struct ArrowSchema* schema) {
  SchemaImporter importer;
  RETURN_NOT_OK(importer.Import(schema));
  return importer.MakeSchema();
}

//////////////////////////////////////////////////////////////////////////
// C data import

namespace {

// A wrapper struct for an imported C ArrowArray.
// The ArrowArray is released on destruction.
struct ImportedArrayData {
  struct ArrowArray array_;

  ImportedArrayData() {
    ArrowArrayMarkReleased(&array_);  // Initially released
  }

  void Release() {
    if (!ArrowArrayIsReleased(&array_)) {
      ArrowArrayRelease(&array_);
      DCHECK(ArrowArrayIsReleased(&array_));
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

struct ArrayImporter {
  explicit ArrayImporter(const std::shared_ptr<DataType>& type)
      : type_(type), zero_size_buffer_(std::make_shared<Buffer>(kZeroSizeArea, 0)) {}

  Status Import(struct ArrowArray* src) {
    if (ArrowArrayIsReleased(src)) {
      return Status::Invalid("Cannot import released ArrowArray");
    }
    recursion_level_ = 0;
    import_ = std::make_shared<ImportedArrayData>();
    c_struct_ = &import_->array_;
    ArrowArrayMove(src, c_struct_);
    return DoImport();
  }

  Result<std::shared_ptr<Array>> MakeArray() {
    DCHECK_NE(data_, nullptr);
    return ::arrow::MakeArray(data_);
  }

  std::shared_ptr<ArrayData> GetArrayData() {
    DCHECK_NE(data_, nullptr);
    return data_;
  }

  Result<std::shared_ptr<RecordBatch>> MakeRecordBatch(std::shared_ptr<Schema> schema) {
    DCHECK_NE(data_, nullptr);
    if (data_->GetNullCount() != 0) {
      return Status::Invalid(
          "ArrowArray struct has non-zero null count, "
          "cannot be imported as RecordBatch");
    }
    if (data_->offset != 0) {
      return Status::Invalid(
          "ArrowArray struct has non-zero offset, "
          "cannot be imported as RecordBatch");
    }
    return RecordBatch::Make(std::move(schema), data_->length,
                             std::move(data_->child_data));
  }

  Status ImportChild(const ArrayImporter* parent, struct ArrowArray* src) {
    if (ArrowArrayIsReleased(src)) {
      return Status::Invalid("Cannot import released ArrowArray");
    }
    recursion_level_ = parent->recursion_level_ + 1;
    if (recursion_level_ >= kMaxImportRecursionLevel) {
      return Status::Invalid("Recursion level in ArrowArray struct exceeded");
    }
    // Child buffers will keep the entire parent import alive.
    // Perhaps we can move the child structs to an owned area
    // when the parent ImportedArrayData::Release() gets called,
    // but that is another level of complication.
    import_ = parent->import_;
    // The ArrowArray shouldn't be moved, it's owned by its parent
    c_struct_ = src;
    return DoImport();
  }

  Status ImportDict(const ArrayImporter* parent, struct ArrowArray* src) {
    return ImportChild(parent, src);
  }

  Status DoImport() {
    // Unwrap extension type
    const DataType* storage_type = type_.get();
    if (storage_type->id() == Type::EXTENSION) {
      storage_type =
          checked_cast<const ExtensionType&>(*storage_type).storage_type().get();
    }

    // First import children (required for reconstituting parent array data)
    const auto& fields = storage_type->fields();
    if (c_struct_->n_children != static_cast<int64_t>(fields.size())) {
      return Status::Invalid("ArrowArray struct has ", c_struct_->n_children,
                             " children, expected ", fields.size(), " for type ",
                             type_->ToString());
    }
    child_importers_.reserve(fields.size());
    for (int64_t i = 0; i < c_struct_->n_children; ++i) {
      DCHECK_NE(c_struct_->children[i], nullptr);
      child_importers_.emplace_back(fields[i]->type());
      RETURN_NOT_OK(child_importers_.back().ImportChild(this, c_struct_->children[i]));
    }

    // Import main data
    RETURN_NOT_OK(VisitTypeInline(*storage_type, this));

    bool is_dict_type = (storage_type->id() == Type::DICTIONARY);
    if (c_struct_->dictionary != nullptr) {
      if (!is_dict_type) {
        return Status::Invalid("Import type is ", type_->ToString(),
                               " but dictionary field in ArrowArray struct is not null");
      }
      const auto& dict_type = checked_cast<const DictionaryType&>(*storage_type);
      // Import dictionary values
      ArrayImporter dict_importer(dict_type.value_type());
      RETURN_NOT_OK(dict_importer.ImportDict(this, c_struct_->dictionary));
      data_->dictionary = dict_importer.GetArrayData();
    } else {
      if (is_dict_type) {
        return Status::Invalid("Import type is ", type_->ToString(),
                               " but dictionary field in ArrowArray struct is null");
      }
    }
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Cannot import array of type ", type_->ToString());
  }

  Status Visit(const FixedWidthType& type) { return ImportFixedSizePrimitive(type); }

  Status Visit(const NullType& type) {
    RETURN_NOT_OK(CheckNoChildren());
    if (c_struct_->n_buffers == 1) {
      // Legacy format exported by older Arrow C++ versions
      RETURN_NOT_OK(AllocateArrayData());
    } else {
      RETURN_NOT_OK(CheckNumBuffers(0));
      RETURN_NOT_OK(AllocateArrayData());
      data_->buffers.insert(data_->buffers.begin(), nullptr);
    }
    data_->null_count = data_->length;
    return Status::OK();
  }

  Status Visit(const StringType& type) { return ImportStringLike(type); }

  Status Visit(const BinaryType& type) { return ImportStringLike(type); }

  Status Visit(const LargeStringType& type) { return ImportStringLike(type); }

  Status Visit(const LargeBinaryType& type) { return ImportStringLike(type); }

  Status Visit(const ListType& type) { return ImportListLike(type); }

  Status Visit(const LargeListType& type) { return ImportListLike(type); }

  Status Visit(const FixedSizeListType& type) {
    RETURN_NOT_OK(CheckNumChildren(1));
    RETURN_NOT_OK(CheckNumBuffers(1));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    RETURN_NOT_OK(CheckNumBuffers(1));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    return Status::OK();
  }

  Status Visit(const SparseUnionType& type) {
    RETURN_NOT_OK(CheckNoNulls());
    if (c_struct_->n_buffers == 2) {
      // ARROW-14179: legacy format exported by older Arrow C++ versions
      RETURN_NOT_OK(AllocateArrayData());
      RETURN_NOT_OK(ImportFixedSizeBuffer(1, sizeof(int8_t)));
    } else {
      RETURN_NOT_OK(CheckNumBuffers(1));
      RETURN_NOT_OK(AllocateArrayData());
      RETURN_NOT_OK(ImportFixedSizeBuffer(0, sizeof(int8_t)));
      // Prepend a null bitmap buffer, as expected by SparseUnionArray
      data_->buffers.insert(data_->buffers.begin(), nullptr);
    }
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) {
    RETURN_NOT_OK(CheckNoNulls());
    if (c_struct_->n_buffers == 3) {
      // ARROW-14179: legacy format exported by older Arrow C++ versions
      RETURN_NOT_OK(AllocateArrayData());
      RETURN_NOT_OK(ImportFixedSizeBuffer(1, sizeof(int8_t)));
      RETURN_NOT_OK(ImportFixedSizeBuffer(2, sizeof(int32_t)));
    } else {
      RETURN_NOT_OK(CheckNumBuffers(2));
      RETURN_NOT_OK(AllocateArrayData());
      RETURN_NOT_OK(ImportFixedSizeBuffer(0, sizeof(int8_t)));
      RETURN_NOT_OK(ImportFixedSizeBuffer(1, sizeof(int32_t)));
      // Prepend a null bitmap pointer, as expected by DenseUnionArray
      data_->buffers.insert(data_->buffers.begin(), nullptr);
    }
    return Status::OK();
  }

  Status ImportFixedSizePrimitive(const FixedWidthType& type) {
    RETURN_NOT_OK(CheckNoChildren());
    RETURN_NOT_OK(CheckNumBuffers(2));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    if (bit_util::IsMultipleOf8(type.bit_width())) {
      RETURN_NOT_OK(ImportFixedSizeBuffer(1, type.bit_width() / 8));
    } else {
      DCHECK_EQ(type.bit_width(), 1);
      RETURN_NOT_OK(ImportBitsBuffer(1));
    }
    return Status::OK();
  }

  template <typename StringType>
  Status ImportStringLike(const StringType& type) {
    RETURN_NOT_OK(CheckNoChildren());
    RETURN_NOT_OK(CheckNumBuffers(3));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportOffsetsBuffer<typename StringType::offset_type>(1));
    RETURN_NOT_OK(ImportStringValuesBuffer<typename StringType::offset_type>(1, 2));
    return Status::OK();
  }

  template <typename ListType>
  Status ImportListLike(const ListType& type) {
    RETURN_NOT_OK(CheckNumChildren(1));
    RETURN_NOT_OK(CheckNumBuffers(2));
    RETURN_NOT_OK(AllocateArrayData());
    RETURN_NOT_OK(ImportNullBitmap());
    RETURN_NOT_OK(ImportOffsetsBuffer<typename ListType::offset_type>(1));
    return Status::OK();
  }

  Status CheckNoChildren() { return CheckNumChildren(0); }

  Status CheckNumChildren(int64_t n_children) {
    if (c_struct_->n_children != n_children) {
      return Status::Invalid("Expected ", n_children, " children for imported type ",
                             type_->ToString(), ", ArrowArray struct has ",
                             c_struct_->n_children);
    }
    return Status::OK();
  }

  Status CheckNumBuffers(int64_t n_buffers) {
    if (n_buffers != c_struct_->n_buffers) {
      return Status::Invalid("Expected ", n_buffers, " buffers for imported type ",
                             type_->ToString(), ", ArrowArray struct has ",
                             c_struct_->n_buffers);
    }
    return Status::OK();
  }

  Status CheckNoNulls() {
    if (c_struct_->null_count != 0) {
      return Status::Invalid("Unexpected non-zero null count for imported type ",
                             type_->ToString());
    }
    return Status::OK();
  }

  Status AllocateArrayData() {
    DCHECK_EQ(data_, nullptr);
    data_ = std::make_shared<ArrayData>(type_, c_struct_->length, c_struct_->null_count,
                                        c_struct_->offset);
    data_->buffers.resize(static_cast<size_t>(c_struct_->n_buffers));
    data_->child_data.resize(static_cast<size_t>(c_struct_->n_children));
    DCHECK_EQ(child_importers_.size(), data_->child_data.size());
    std::transform(child_importers_.begin(), child_importers_.end(),
                   data_->child_data.begin(),
                   [](const ArrayImporter& child) { return child.data_; });
    return Status::OK();
  }

  Status ImportNullBitmap(int32_t buffer_id = 0) {
    RETURN_NOT_OK(ImportBitsBuffer(buffer_id, /*is_null_bitmap=*/true));
    if (data_->null_count > 0 && data_->buffers[buffer_id] == nullptr) {
      return Status::Invalid(
          "ArrowArray struct has null bitmap buffer but non-zero null_count ",
          data_->null_count);
    }
    return Status::OK();
  }

  Status ImportBitsBuffer(int32_t buffer_id, bool is_null_bitmap = false) {
    // Compute visible size of buffer
    int64_t buffer_size =
        (c_struct_->length > 0)
            ? bit_util::BytesForBits(c_struct_->length + c_struct_->offset)
            : 0;
    return ImportBuffer(buffer_id, buffer_size, is_null_bitmap);
  }

  Status ImportFixedSizeBuffer(int32_t buffer_id, int64_t byte_width) {
    // Compute visible size of buffer
    int64_t buffer_size = (c_struct_->length > 0)
                              ? byte_width * (c_struct_->length + c_struct_->offset)
                              : 0;
    return ImportBuffer(buffer_id, buffer_size);
  }

  template <typename OffsetType>
  Status ImportOffsetsBuffer(int32_t buffer_id) {
    // Compute visible size of buffer
    int64_t buffer_size =
        sizeof(OffsetType) * (c_struct_->length + c_struct_->offset + 1);
    return ImportBuffer(buffer_id, buffer_size);
  }

  template <typename OffsetType>
  Status ImportStringValuesBuffer(int32_t offsets_buffer_id, int32_t buffer_id,
                                  int64_t byte_width = 1) {
    auto offsets = data_->GetValues<OffsetType>(offsets_buffer_id);
    // Compute visible size of buffer
    int64_t buffer_size =
        (c_struct_->length > 0) ? byte_width * offsets[c_struct_->length] : 0;
    return ImportBuffer(buffer_id, buffer_size);
  }

  Status ImportBuffer(int32_t buffer_id, int64_t buffer_size,
                      bool is_null_bitmap = false) {
    std::shared_ptr<Buffer>* out = &data_->buffers[buffer_id];
    auto data = reinterpret_cast<const uint8_t*>(c_struct_->buffers[buffer_id]);
    if (data != nullptr) {
      *out = std::make_shared<ImportedBuffer>(data, buffer_size, import_);
    } else if (is_null_bitmap) {
      out->reset();
    } else {
      // Ensure that imported buffers are never null (except for the null bitmap)
      if (buffer_size != 0) {
        return Status::Invalid(
            "ArrowArrayStruct contains null data pointer "
            "for a buffer with non-zero computed size");
      }
      *out = zero_size_buffer_;
    }
    return Status::OK();
  }

  struct ArrowArray* c_struct_;
  int64_t recursion_level_;
  const std::shared_ptr<DataType>& type_;

  std::shared_ptr<ImportedArrayData> import_;
  std::shared_ptr<ArrayData> data_;
  std::vector<ArrayImporter> child_importers_;

  // For imported null buffer pointers
  std::shared_ptr<Buffer> zero_size_buffer_;
};

}  // namespace

Result<std::shared_ptr<Array>> ImportArray(struct ArrowArray* array,
                                           std::shared_ptr<DataType> type) {
  ArrayImporter importer(type);
  RETURN_NOT_OK(importer.Import(array));
  return importer.MakeArray();
}

Result<std::shared_ptr<Array>> ImportArray(struct ArrowArray* array,
                                           struct ArrowSchema* type) {
  auto maybe_type = ImportType(type);
  if (!maybe_type.ok()) {
    ArrowArrayRelease(array);
    return maybe_type.status();
  }
  return ImportArray(array, *maybe_type);
}

Result<std::shared_ptr<RecordBatch>> ImportRecordBatch(struct ArrowArray* array,
                                                       std::shared_ptr<Schema> schema) {
  auto type = struct_(schema->fields());
  ArrayImporter importer(type);
  RETURN_NOT_OK(importer.Import(array));
  return importer.MakeRecordBatch(std::move(schema));
}

Result<std::shared_ptr<RecordBatch>> ImportRecordBatch(struct ArrowArray* array,
                                                       struct ArrowSchema* schema) {
  auto maybe_schema = ImportSchema(schema);
  if (!maybe_schema.ok()) {
    ArrowArrayRelease(array);
    return maybe_schema.status();
  }
  return ImportRecordBatch(array, *maybe_schema);
}

//////////////////////////////////////////////////////////////////////////
// C stream export

namespace {

class ExportedArrayStream {
 public:
  struct PrivateData {
    explicit PrivateData(std::shared_ptr<RecordBatchReader> reader)
        : reader_(std::move(reader)) {}

    std::shared_ptr<RecordBatchReader> reader_;
    std::string last_error_;

    PrivateData() = default;
    ARROW_DISALLOW_COPY_AND_ASSIGN(PrivateData);
  };

  explicit ExportedArrayStream(struct ArrowArrayStream* stream) : stream_(stream) {}

  Status GetSchema(struct ArrowSchema* out_schema) {
    return ExportSchema(*reader()->schema(), out_schema);
  }

  Status GetNext(struct ArrowArray* out_array) {
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader()->ReadNext(&batch));
    if (batch == nullptr) {
      // End of stream
      ArrowArrayMarkReleased(out_array);
      return Status::OK();
    } else {
      return ExportRecordBatch(*batch, out_array);
    }
  }

  const char* GetLastError() {
    const auto& last_error = private_data()->last_error_;
    return last_error.empty() ? nullptr : last_error.c_str();
  }

  void Release() {
    if (ArrowArrayStreamIsReleased(stream_)) {
      return;
    }
    DCHECK_NE(private_data(), nullptr);
    delete private_data();

    ArrowArrayStreamMarkReleased(stream_);
  }

  // C-compatible callbacks

  static int StaticGetSchema(struct ArrowArrayStream* stream,
                             struct ArrowSchema* out_schema) {
    ExportedArrayStream self{stream};
    return self.ToCError(self.GetSchema(out_schema));
  }

  static int StaticGetNext(struct ArrowArrayStream* stream,
                           struct ArrowArray* out_array) {
    ExportedArrayStream self{stream};
    return self.ToCError(self.GetNext(out_array));
  }

  static void StaticRelease(struct ArrowArrayStream* stream) {
    ExportedArrayStream{stream}.Release();
  }

  static const char* StaticGetLastError(struct ArrowArrayStream* stream) {
    return ExportedArrayStream{stream}.GetLastError();
  }

 private:
  int ToCError(const Status& status) {
    if (ARROW_PREDICT_TRUE(status.ok())) {
      private_data()->last_error_.clear();
      return 0;
    }
    private_data()->last_error_ = status.ToString();
    switch (status.code()) {
      case StatusCode::IOError:
        return EIO;
      case StatusCode::NotImplemented:
        return ENOSYS;
      case StatusCode::OutOfMemory:
        return ENOMEM;
      default:
        return EINVAL;  // Fallback for Invalid, TypeError, etc.
    }
  }

  PrivateData* private_data() {
    return reinterpret_cast<PrivateData*>(stream_->private_data);
  }

  const std::shared_ptr<RecordBatchReader>& reader() { return private_data()->reader_; }

  struct ArrowArrayStream* stream_;
};

}  // namespace

Status ExportRecordBatchReader(std::shared_ptr<RecordBatchReader> reader,
                               struct ArrowArrayStream* out) {
  out->get_schema = ExportedArrayStream::StaticGetSchema;
  out->get_next = ExportedArrayStream::StaticGetNext;
  out->get_last_error = ExportedArrayStream::StaticGetLastError;
  out->release = ExportedArrayStream::StaticRelease;
  out->private_data = new ExportedArrayStream::PrivateData{std::move(reader)};
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////
// C stream import

namespace {

class ArrayStreamBatchReader : public RecordBatchReader {
 public:
  explicit ArrayStreamBatchReader(std::shared_ptr<Schema> schema,
                                  struct ArrowArrayStream* stream)
      : schema_(std::move(schema)) {
    ArrowArrayStreamMove(stream, &stream_);
    DCHECK(!ArrowArrayStreamIsReleased(&stream_));
  }

  ~ArrayStreamBatchReader() {
    if (!ArrowArrayStreamIsReleased(&stream_)) {
      ArrowArrayStreamRelease(&stream_);
    }
    DCHECK(ArrowArrayStreamIsReleased(&stream_));
  }

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    struct ArrowArray c_array;
    if (ArrowArrayStreamIsReleased(&stream_)) {
      return Status::Invalid(
          "Attempt to read from a reader that has already been closed");
    }
    RETURN_NOT_OK(StatusFromCError(stream_.get_next(&stream_, &c_array)));
    if (ArrowArrayIsReleased(&c_array)) {
      // End of stream
      batch->reset();
      return Status::OK();
    } else {
      return ImportRecordBatch(&c_array, schema_).Value(batch);
    }
  }

  Status Close() override {
    if (!ArrowArrayStreamIsReleased(&stream_)) {
      ArrowArrayStreamRelease(&stream_);
    }
    return Status::OK();
  }

  static Result<std::shared_ptr<RecordBatchReader>> Make(
      struct ArrowArrayStream* stream) {
    if (ArrowArrayStreamIsReleased(stream)) {
      return Status::Invalid("Cannot import released ArrowArrayStream");
    }
    std::shared_ptr<Schema> schema;
    struct ArrowSchema c_schema = {};
    auto status = StatusFromCError(stream, stream->get_schema(stream, &c_schema));
    if (status.ok()) {
      status = ImportSchema(&c_schema).Value(&schema);
    }
    if (!status.ok()) {
      ArrowArrayStreamRelease(stream);
      return status;
    }
    return std::make_shared<ArrayStreamBatchReader>(std::move(schema), stream);
  }

 private:
  Status StatusFromCError(int errno_like) const {
    return StatusFromCError(&stream_, errno_like);
  }

  static Status StatusFromCError(struct ArrowArrayStream* stream, int errno_like) {
    if (ARROW_PREDICT_TRUE(errno_like == 0)) {
      return Status::OK();
    }
    StatusCode code;
    switch (errno_like) {
      case EDOM:
      case EINVAL:
      case ERANGE:
        code = StatusCode::Invalid;
        break;
      case ENOMEM:
        code = StatusCode::OutOfMemory;
        break;
      case ENOSYS:
        code = StatusCode::NotImplemented;
      default:
        code = StatusCode::IOError;
        break;
    }
    const char* last_error = stream->get_last_error(stream);
    return {code, last_error ? std::string(last_error) : ""};
  }

  mutable struct ArrowArrayStream stream_;
  std::shared_ptr<Schema> schema_;
};

}  // namespace

Result<std::shared_ptr<RecordBatchReader>> ImportRecordBatchReader(
    struct ArrowArrayStream* stream) {
  return ArrayStreamBatchReader::Make(stream);
}

}  // namespace arrow
