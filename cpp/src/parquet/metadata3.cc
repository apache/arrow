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
#include "parquet/metadata3.h"

namespace parquet {

namespace {

template <typename T, typename U>
constexpr bool IsEnumEq(T t, U u) {
  return static_cast<int>(t) == static_cast<int>(u);
}

static_assert(IsEnumEq(format::Type::BOOLEAN, format3::Type::BOOLEAN));
static_assert(IsEnumEq(format::Type::INT32, format3::Type::INT32));
static_assert(IsEnumEq(format::Type::INT64, format3::Type::INT64));
static_assert(IsEnumEq(format::Type::INT96, format3::Type::INT96));
static_assert(IsEnumEq(format::Type::FLOAT, format3::Type::FLOAT));
static_assert(IsEnumEq(format::Type::DOUBLE, format3::Type::DOUBLE));
static_assert(IsEnumEq(format::Type::BYTE_ARRAY, format3::Type::BYTE_ARRAY));
static_assert(IsEnumEq(format::Type::FIXED_LEN_BYTE_ARRAY,
                       format3::Type::FIXED_LEN_BYTE_ARRAY));

static_assert(IsEnumEq(format::FieldRepetitionType::REQUIRED,
                       format3::FieldRepetitionType::REQUIRED));
static_assert(IsEnumEq(format::FieldRepetitionType::OPTIONAL,
                       format3::FieldRepetitionType::OPTIONAL));
static_assert(IsEnumEq(format::FieldRepetitionType::REPEATED,
                       format3::FieldRepetitionType::REPEATED));

static_assert(IsEnumEq(format::Encoding::PLAIN, format3::Encoding::PLAIN));
static_assert(IsEnumEq(format::Encoding::PLAIN_DICTIONARY,
                       format3::Encoding::PLAIN_DICTIONARY));
static_assert(IsEnumEq(format::Encoding::RLE, format3::Encoding::RLE));
static_assert(IsEnumEq(format::Encoding::DELTA_BINARY_PACKED,
                       format3::Encoding::DELTA_BINARY_PACKED));
static_assert(IsEnumEq(format::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                       format3::Encoding::DELTA_LENGTH_BYTE_ARRAY));
static_assert(IsEnumEq(format::Encoding::DELTA_BYTE_ARRAY,
                       format3::Encoding::DELTA_BYTE_ARRAY));
static_assert(IsEnumEq(format::Encoding::RLE_DICTIONARY,
                       format3::Encoding::RLE_DICTIONARY));
static_assert(IsEnumEq(format::Encoding::BYTE_STREAM_SPLIT,
                       format3::Encoding::BYTE_STREAM_SPLIT));

static_assert(IsEnumEq(format::CompressionCodec::UNCOMPRESSED,
                       format3::CompressionCodec::UNCOMPRESSED));
static_assert(IsEnumEq(format::CompressionCodec::SNAPPY,
                       format3::CompressionCodec::SNAPPY));
static_assert(IsEnumEq(format::CompressionCodec::GZIP, format3::CompressionCodec::GZIP));
static_assert(IsEnumEq(format::CompressionCodec::LZO, format3::CompressionCodec::LZO));
static_assert(IsEnumEq(format::CompressionCodec::BROTLI,
                       format3::CompressionCodec::BROTLI));
static_assert(IsEnumEq(format::CompressionCodec::ZSTD, format3::CompressionCodec::ZSTD));
static_assert(IsEnumEq(format::CompressionCodec::LZ4_RAW,
                       format3::CompressionCodec::LZ4_RAW));

static_assert(IsEnumEq(format::PageType::DATA_PAGE, format3::PageType::DATA_PAGE));
static_assert(IsEnumEq(format::PageType::DATA_PAGE_V2, format3::PageType::DATA_PAGE_V2));
static_assert(IsEnumEq(format::PageType::INDEX_PAGE, format3::PageType::INDEX_PAGE));
static_assert(IsEnumEq(format::PageType::DICTIONARY_PAGE,
                       format3::PageType::DICTIONARY_PAGE));

auto GetNumChildren(
    const flatbuffers::Vector<flatbuffers::Offset<format3::SchemaElement>>& s, size_t i) {
  return s.Get(i)->num_children();
}

auto GetNumChildren(const std::vector<format::SchemaElement>& s, size_t i) {
  return s[i].num_children;
}

auto GetName(const flatbuffers::Vector<flatbuffers::Offset<format3::SchemaElement>>& s,
             size_t i) {
  return s.Get(i)->name()->str();
}

auto GetName(const std::vector<format::SchemaElement>& s, size_t i) { return s[i].name; }

class ColumnMap {
 public:
  template <typename Schema>
  explicit ColumnMap(const Schema& s) {
    for (size_t i = 0; i < s.size(); ++i) {
      if (GetNumChildren(s, i) == 0) colchunk2schema_.push_back(i);
    }
    BuildParents(s);
  }

  size_t ToSchema(size_t cc_idx) const { return colchunk2schema_[cc_idx]; }
  std::optional<size_t> ToCc(size_t schema_idx) const {
    auto it =
        std::lower_bound(colchunk2schema_.begin(), colchunk2schema_.end(), schema_idx);
    if (it == colchunk2schema_.end() || *it != schema_idx) return std::nullopt;
    return it - colchunk2schema_.begin();
  }

  template <typename Schema>
  auto ToPath(const Schema& s, size_t col_idx) {
    std::vector<std::string> path;
    size_t len = 0;
    for (size_t idx = ToSchema(col_idx); idx != 0; idx = parents_[idx]) ++len;
    path.reserve(len);

    for (size_t idx = ToSchema(col_idx); idx != 0; idx = parents_[idx]) {
      path.push_back(GetName(s, idx));
    }
    std::reverse(path.begin(), path.end());

    ARROW_DCHECK_EQ(path.size(), len);
    return path;
  }

 private:
  template <typename Schema>
  void BuildParents(const Schema& s) {
    if (s.size() <= 0) return;
    parents_.resize(s.size());
    struct Info {
      uint32_t parent_idx;
      uint32_t remaining_children;
    };
    std::stack<Info> stack;
    parents_[0] = 0;

    stack.push({0, static_cast<uint32_t>(GetNumChildren(s, 0))});
    for (size_t idx = 1; idx < s.size(); ++idx) {
      parents_[idx] = stack.top().parent_idx;
      stack.top().remaining_children--;
      if (auto num_children = GetNumChildren(s, idx); num_children > 0) {
        stack.push({static_cast<uint32_t>(idx), static_cast<uint32_t>(num_children)});
      }
      while (!stack.empty() && stack.top().remaining_children == 0) stack.pop();
    }
  }

  std::vector<uint32_t> colchunk2schema_;
  std::vector<uint32_t> parents_;
};

struct MinMax {
  struct Packed {
    uint32_t lo4 = 0;
    uint64_t lo8 = 0;
    uint64_t hi8 = 0;
    int8_t len = 0;
  };
  Packed min;
  Packed max;
  std::string_view prefix;
};

uint32_t LoadLE32(const void* p) {
  return ::arrow::bit_util::FromLittleEndian(
      ::arrow::util::SafeLoadAs<uint32_t>(static_cast<const uint8_t*>(p)));
}
void StoreLE32(uint32_t v, void* p) {
  v = ::arrow::bit_util::ToLittleEndian(v);
  std::memcpy(p, &v, sizeof(v));
}

uint64_t LoadLE64(const void* p) {
  return ::arrow::bit_util::FromLittleEndian(
      ::arrow::util::SafeLoadAs<uint64_t>(static_cast<const uint8_t*>(p)));
}
void StoreLE64(uint64_t v, void* p) {
  v = ::arrow::bit_util::ToLittleEndian(v);
  std::memcpy(p, &v, sizeof(v));
}

uint32_t LoadBE32(const void* p) {
  return ::arrow::bit_util::FromBigEndian(
      ::arrow::util::SafeLoadAs<uint32_t>(static_cast<const uint8_t*>(p)));
}
void StoreBE32(uint32_t v, void* p) {
  v = ::arrow::bit_util::ToBigEndian(v);
  std::memcpy(p, &v, sizeof(v));
}

uint64_t LoadBE64(const void* p) {
  return ::arrow::bit_util::FromBigEndian(
      ::arrow::util::SafeLoadAs<uint64_t>(static_cast<const uint8_t*>(p)));
}
void StoreBE64(uint64_t v, void* p) {
  v = ::arrow::bit_util::ToBigEndian(v);
  std::memcpy(p, &v, sizeof(v));
}

MinMax Pack(format::Type::type type, const std::string& min, bool is_min_exact,
            const std::string& max, bool is_max_exact) {
  switch (type) {
    case format::Type::BOOLEAN:
      return {};
    case format::Type::INT32:
    case format::Type::FLOAT: {
      auto load = [](std::string_view v, bool is_exact) -> MinMax::Packed {
        return {.lo4 = LoadLE32(v.data()), .len = static_cast<int8_t>(is_exact ? 4 : -4)};
      };
      return {load(min, is_min_exact), load(max, is_max_exact), ""};
    }
    case format::Type::INT64:
    case format::Type::DOUBLE: {
      auto load = [](std::string_view v, bool is_exact) -> MinMax::Packed {
        return {.lo8 = LoadLE64(v.data()), .len = static_cast<int8_t>(is_exact ? 8 : -8)};
      };
      return {load(min, is_min_exact), load(max, is_max_exact), ""};
    }
    case format::Type::INT96: {
      auto load = [](std::string_view v, bool is_exact) -> MinMax::Packed {
        return {.lo4 = LoadLE32(v.data() + 0),
                .lo8 = LoadLE64(v.data() + 4),
                .len = static_cast<int8_t>(is_exact ? 12 : -12)};
      };
      return {load(min, is_min_exact), load(max, is_max_exact), ""};
    }
    case format::Type::FIXED_LEN_BYTE_ARRAY: {
      // Special case for decimal16.
      if (min.size() == 16 && max.size() == 16 && is_min_exact && is_max_exact) {
        auto load = [](std::string_view v) -> MinMax::Packed {
          return {
              .lo8 = LoadBE64(v.data() + 8), .hi8 = LoadBE64(v.data() + 0), .len = 16};
        };
        return {load(min), load(max), ""};
      }
      [[fallthrough]];
    }
    case format::Type::BYTE_ARRAY: {
      auto load = [](std::string_view v, bool is_exact, bool is_max) -> MinMax::Packed {
        if (v.size() <= 4) {
          char buf[4] = {};
          memcpy(buf, v.data(), v.size());
          return {.lo4 = LoadBE32(buf),
                  .len = static_cast<int8_t>(is_exact ? v.size() : -v.size())};
        }
        return {.lo4 = LoadBE32(v.data()) + (is_max ? 1 : -1), .len = -4};
      };
      auto [e1, e2] = std::mismatch(max.begin(), max.end(), min.begin(), min.end());
      size_t prefix_len = e1 - max.begin();
      return {
          load(std::string_view(min).substr(prefix_len), is_min_exact, false),
          load(std::string_view(max).substr(prefix_len), is_max_exact, true),
          std::string_view(max).substr(0, prefix_len),
      };
    }
  }
  ::arrow::Unreachable();
}

bool Unpack(format::Type::type type, const MinMax& packed, std::string* min,
            bool* is_min_exact, std::string* max, bool* is_max_exact) {
  switch (type) {
    case format::Type::BOOLEAN:
      return false;
    case format::Type::INT32:
    case format::Type::FLOAT: {
      auto load = [](const MinMax::Packed& p, std::string* x, bool* is_exact) {
        x->resize(4);
        StoreLE32(p.lo4, x->data());
        *is_exact = p.len > 0;
      };
      load(packed.min, min, is_min_exact);
      load(packed.max, max, is_max_exact);
      return true;
    }
    case format::Type::INT64:
    case format::Type::DOUBLE: {
      auto load = [](const MinMax::Packed& p, std::string* x, bool* is_exact) {
        x->resize(8);
        StoreLE64(p.lo8, x->data());
        *is_exact = p.len > 0;
      };
      load(packed.min, min, is_min_exact);
      load(packed.max, max, is_max_exact);
      return true;
    }
    case format::Type::INT96: {
      auto load = [](const MinMax::Packed& p, std::string* x, bool* is_exact) {
        x->resize(12);
        StoreLE32(p.lo4, x->data() + 0);
        StoreLE64(p.lo8, x->data() + 4);
        *is_exact = p.len > 0;
      };
      load(packed.min, min, is_min_exact);
      load(packed.max, max, is_max_exact);
      return true;
    }
    case format::Type::BYTE_ARRAY:
    case format::Type::FIXED_LEN_BYTE_ARRAY: {
      auto load = [](const MinMax::Packed& p, std::string_view prefix, std::string* x,
                     bool* is_exact) {
        x->resize(prefix.size() + 16);
        if (!prefix.empty()) std::memcpy(x->data(), prefix.data(), prefix.size());
        if (p.len == 16) {
          StoreBE64(p.hi8, x->data() + prefix.size() + 0);
          StoreBE64(p.lo8, x->data() + prefix.size() + 8);
          *is_exact = true;
        } else {
          StoreBE32(p.lo4, x->data() + prefix.size());
          x->resize(prefix.size() + std::abs(p.len));
          *is_exact = p.len >= 0;
        }
      };
      load(packed.min, packed.prefix, min, is_min_exact);
      load(packed.max, packed.prefix, max, is_max_exact);
      return true;
    }
  }
  ::arrow::Unreachable();
}

struct ThriftConverter {
  explicit ThriftConverter(const format3::FileMetaData* md, format::FileMetaData* to)
      : md(md), colmap(*md->schema()), to(to) {}

  const format3::FileMetaData* md;
  ColumnMap colmap;
  format::FileMetaData* to;

  template <typename T, typename... Args>
  auto To(const flatbuffers::Vector<T>* in, Args&&... args) {
    std::vector<decltype(To(in->Get(0), std::forward<Args>(args)..., 0))> out;
    if (!in) return out;
    out.reserve(in->size());
    int idx = 0;
    for (auto&& e : *in) out.push_back(To(e, std::forward<Args>(args)..., idx++));
    return out;
  }

  auto To(format3::TimeUnit t) {
    format::TimeUnit out;
    if (t == format3::TimeUnit::NS)
      out.__set_NANOS({});
    else if (t == format3::TimeUnit::US)
      out.__set_MICROS({});
    else
      out.__set_MILLIS({});
    return out;
  }

  auto To(format3::Type t) { return static_cast<format::Type::type>(t); }
  auto To(format3::FieldRepetitionType t) {
    return static_cast<format::FieldRepetitionType::type>(t);
  }
  auto To(format3::CompressionCodec c) {
    return static_cast<format::CompressionCodec::type>(c);
  }
  auto To(format3::Encoding e, size_t) { return static_cast<format::Encoding::type>(e); }
  auto To(format3::PageType t) { return static_cast<format::PageType::type>(t); }
  auto To(format3::EdgeInterpolationAlgorithm a) {
    return static_cast<format::EdgeInterpolationAlgorithm::type>(a);
  }

  auto To(format3::LogicalType t, size_t col_idx) {
    const format3::SchemaElement* e = md->schema()->Get(col_idx);
    format::LogicalType out;
    switch (t) {
      case format3::LogicalType::StringType:
        out.__set_STRING({});
        break;
      case format3::LogicalType::MapType:
        out.__set_MAP({});
        break;
      case format3::LogicalType::ListType:
        out.__set_LIST({});
        break;
      case format3::LogicalType::EnumType:
        out.__set_ENUM({});
        break;
      case format3::LogicalType::DecimalType: {
        format::DecimalType dt;
        dt.__set_precision(e->logical_type_as_DecimalType()->precision());
        dt.__set_scale(e->logical_type_as_DecimalType()->scale());
        out.__set_DECIMAL(dt);
        break;
      }
      case format3::LogicalType::DateType:
        out.__set_DATE({});
        break;
      case format3::LogicalType::TimeType: {
        format::TimeType tt;
        tt.__set_unit(To(e->logical_type_as_TimeType()->unit()));
        tt.__set_isAdjustedToUTC(e->logical_type_as_TimeType()->is_adjusted_to_utc());
        out.__set_TIME(tt);
        break;
      }
      case format3::LogicalType::TimestampType: {
        format::TimestampType tt;
        tt.__set_unit(To(e->logical_type_as_TimestampType()->unit()));
        tt.__set_isAdjustedToUTC(
            e->logical_type_as_TimestampType()->is_adjusted_to_utc());
        out.__set_TIMESTAMP(tt);
        break;
      }
      case format3::LogicalType::IntType: {
        format::IntType it;
        it.__set_bitWidth(e->logical_type_as_IntType()->bit_width());
        it.__set_isSigned(e->logical_type_as_IntType()->is_signed());
        out.__set_INTEGER(it);
        break;
      }
      case format3::LogicalType::NullType:
        out.__set_UNKNOWN({});
        break;
      case format3::LogicalType::JsonType:
        out.__set_JSON({});
        break;
      case format3::LogicalType::BsonType:
        out.__set_BSON({});
        break;
      case format3::LogicalType::UUIDType:
        out.__set_UUID({});
        break;
      case format3::LogicalType::Float16Type:
        out.__set_FLOAT16({});
        break;
      case format3::LogicalType::VariantType:
        out.__set_VARIANT({});
        break;
      case format3::LogicalType::GeometryType: {
        format::GeometryType gt;
        gt.__set_crs(e->logical_type_as_GeometryType()->crs()->str());
        out.__set_GEOMETRY(gt);
        break;
      }
      case format3::LogicalType::GeographyType: {
        format::GeographyType gt;
        gt.__set_crs(e->logical_type_as_GeographyType()->crs()->str());
        gt.__set_algorithm(To(e->logical_type_as_GeographyType()->algorithm()));
        out.__set_GEOGRAPHY(gt);
        break;
      }
      default:
        ::arrow::Unreachable();
    }
    return out;
  }

  auto To(format3::ColumnOrder) {
    format::ColumnOrder out;
    out.__set_TYPE_ORDER({});
    return out;
  }

  auto To(const format3::SchemaElement* e, size_t col_idx) {
    format::SchemaElement out;
    out.name = e->name()->str();
    if (e->type()) {
      out.__isset.type = true;
      out.type = To(*e->type());
    }
    out.__isset.repetition_type = true;
    out.repetition_type = To(e->repetition_type());
    if (e->logical_type_type() != format3::LogicalType::NONE) {
      out.__isset.logicalType = true;
      out.logicalType = To(e->logical_type_type(), col_idx);
    }
    if (e->type_length()) {
      out.__isset.type_length = true;
      out.type_length = *e->type_length();
    }
    if (e->num_children() != 0) {
      out.__isset.num_children = true;
      out.num_children = e->num_children();
    }

    if (e->field_id()) {
      out.__isset.field_id = true;
      out.field_id = *e->field_id();
    }
    return out;
  }

  auto To(const format3::KV* kv, size_t) {
    format::KeyValue out;
    if (kv->key()->size() != 0) out.key = kv->key()->str();
    if (flatbuffers::IsFieldPresent(kv, format3::KV::VT_VAL)) {
      out.__isset.value = true;
      out.value = kv->val()->str();
    }
    return out;
  }

  auto To(const format3::Statistics* s, size_t, size_t col_idx) {
    format::Statistics out;
    if (s->null_count()) {
      out.__isset.null_count = true;
      out.null_count = *s->null_count();
    }

    bool set = false;
    if (s->min_len() && s->max_len()) {
      MinMax mm{
          .min = {.lo4 = s->min_lo4(),
                  .lo8 = s->min_lo8(),
                  .hi8 = s->min_hi8(),
                  .len = *s->min_len()},
          .max = {.lo4 = s->max_lo4(),
                  .lo8 = s->max_lo8(),
                  .hi8 = s->max_hi8(),
                  .len = *s->max_len()},
          .prefix = flatbuffers::GetStringView(s->prefix()),
      };
      set = Unpack(To(*md->schema()->Get(colmap.ToSchema(col_idx))->type()), mm,
                   &out.min_value, &out.is_min_value_exact, &out.max_value,
                   &out.is_max_value_exact);
    }
    out.__isset.min_value = set;
    out.__isset.max_value = set;
    return out;
  }

  auto To(const format3::ColumnMetadata* cm, size_t rg_idx, size_t col_idx) {
    format::ColumnMetaData out;
    out.type = To(*md->schema()->Get(colmap.ToSchema(col_idx))->type());
    out.codec = To(cm->codec());
    out.num_values =
        cm->num_values() ? *cm->num_values() : md->row_groups()->Get(rg_idx)->num_rows();
    out.total_uncompressed_size = cm->total_uncompressed_size();
    out.total_compressed_size = cm->total_compressed_size();
    out.key_value_metadata = To(cm->key_value_metadata());
    if (cm->data_page_offset() == 0) {
      out.data_page_offset = md->row_groups()->Get(rg_idx)->file_offset();
    } else {
      out.data_page_offset = cm->data_page_offset();
    }
    if (cm->index_page_offset()) {
      out.__isset.index_page_offset = true;
      out.index_page_offset = *cm->index_page_offset();
    }
    if (cm->dictionary_page_offset()) {
      out.__isset.dictionary_page_offset = true;
      out.dictionary_page_offset = *cm->dictionary_page_offset();
    }
    if (cm->statistics()) {
      out.__isset.statistics = true;
      out.statistics = To(cm->statistics(), rg_idx, col_idx);
    }
    if (cm->bloom_filter_offset()) {
      out.__isset.bloom_filter_offset = true;
      out.bloom_filter_offset = *cm->bloom_filter_offset();
    }
    if (cm->bloom_filter_length()) {
      out.__isset.bloom_filter_length = true;
      out.bloom_filter_length = *cm->bloom_filter_length();
    }
    if (cm->is_fully_dict_encoded()) {
      // Adding a fake encoding_stats with data_page page type and dictionary encoding to
      // trick the Photon reader function
      // AtomicParquetColumnReader::HasOnlyDictionaryEncodedPages() into treating this as
      // fully_dict_encoded.
      out.__isset.encoding_stats = true;
      auto& pes = out.encoding_stats.emplace_back();
      pes.__set_page_type(format::PageType::DATA_PAGE);
      pes.__set_encoding(format::Encoding::RLE_DICTIONARY);
    }
    return out;
  }

  auto To(const format3::ColumnChunk* cc, size_t rg_idx, size_t col_idx) {
    format::ColumnChunk out;
    out.__isset.meta_data = true;
    out.meta_data = To(cc->meta_data(), rg_idx, col_idx);
    out.meta_data.path_in_schema = colmap.ToPath(*md->schema(), col_idx);
    return out;
  }

  auto To(const format3::SortingColumn* sc, size_t, size_t) {
    format::SortingColumn out;
    out.column_idx = sc->column_idx();
    out.descending = sc->descending();
    out.nulls_first = sc->nulls_first();
    return out;
  }

  auto To(const format3::RowGroup* rg, size_t rg_idx) {
    format::RowGroup out;
    out.columns = To(rg->columns(), rg_idx);
    if (rg->sorting_columns()) {
      out.__isset.sorting_columns = true;
      out.sorting_columns = To(rg->sorting_columns(), rg_idx);
    }
    out.__set_total_byte_size(rg->total_byte_size());
    out.__set_num_rows(rg->num_rows());
    if (rg->ordinal()) out.__set_ordinal(*rg->ordinal());
    out.__set_file_offset(rg->file_offset());
    out.__set_total_compressed_size(rg->total_compressed_size());
    return out;
  }

  void To() {
    to->__set_version(md->version());
    to->__set_num_rows(md->num_rows());
    to->row_groups = To(md->row_groups());
    if (md->kv()) {
      to->__isset.key_value_metadata = true;
      to->key_value_metadata = To(md->kv());
    }
    if (md->created_by()->size() > 0) {
      to->__isset.created_by = true;
      to->created_by = md->created_by()->str();
    }
    to->schema = To(md->schema());
    for (auto* e : *md->schema()) {
      if (e->num_children() == 0) to->column_orders.push_back(To(e->column_order_type()));
    }
  }
};
struct FlatbufferConverter {
  explicit FlatbufferConverter(const format::FileMetaData& md)
      : md(md), colmap(md.schema) {}
  const format::FileMetaData& md;
  ColumnMap colmap;
  ::flatbuffers::FlatBufferBuilder root;

  template <typename T, typename... Args>
  auto To(const std::vector<T>& in, Args&&... args) {
    std::vector<decltype(To(in.at(0), std::forward<Args>(args)..., 0))> out;
    out.reserve(in.size());
    size_t idx = 0;
    for (auto&& e : in) out.push_back(To(e, std::forward<Args>(args)..., idx++));
    return root.CreateVector(out);
  }

  template <typename T, typename... Args>
  auto OptTo(const std::vector<T>& in, Args&&... args)
      -> std::optional<decltype(To(in, std::forward<Args>(args)...))> {
    if (in.empty()) return std::nullopt;
    return To(in, std::forward<Args>(args)...);
  }

  auto To(format::TimeUnit t) {
    if (t.__isset.NANOS) return format3::TimeUnit::NS;
    if (t.__isset.MICROS) return format3::TimeUnit::US;
    return format3::TimeUnit::MS;
  }
  auto To(format::Type::type t) { return static_cast<format3::Type>(t); }
  auto To(format::FieldRepetitionType::type t) {
    return static_cast<format3::FieldRepetitionType>(t);
  }
  auto To(format::CompressionCodec::type c) {
    return static_cast<format3::CompressionCodec>(c);
  }
  auto To(format::Encoding::type e) { return static_cast<format3::Encoding>(e); }
  auto To(format::PageType::type p) { return static_cast<format3::PageType>(p); }
  auto To(format::EdgeInterpolationAlgorithm::type a) {
    return static_cast<format3::EdgeInterpolationAlgorithm>(a);
  }

  std::pair<format3::LogicalType, ::flatbuffers::Offset<void>> To(
      const format::LogicalType& t) {
    if (t.__isset.STRING) {
      return {format3::LogicalType::StringType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.MAP) {
      return {format3::LogicalType::MapType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.LIST) {
      return {format3::LogicalType::ListType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.ENUM) {
      return {format3::LogicalType::EnumType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.DECIMAL) {
      return {
          format3::LogicalType::DecimalType,
          format3::CreateDecimalOpts(root, t.DECIMAL.precision, t.DECIMAL.scale).Union()};
    } else if (t.__isset.DATE) {
      return {format3::LogicalType::DateType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.TIME) {
      auto tu = To(t.TIME.unit);
      return {format3::LogicalType::TimeType,
              format3::CreateTimeOpts(root, t.TIME.isAdjustedToUTC, tu).Union()};
    } else if (t.__isset.TIMESTAMP) {
      auto tu = To(t.TIMESTAMP.unit);
      return {format3::LogicalType::TimestampType,
              format3::CreateTimeOpts(root, t.TIMESTAMP.isAdjustedToUTC, tu).Union()};
    } else if (t.__isset.INTEGER) {
      return {
          format3::LogicalType::IntType,
          format3::CreateIntOpts(root, t.INTEGER.bitWidth, t.INTEGER.isSigned).Union()};
    } else if (t.__isset.UNKNOWN) {
      return {format3::LogicalType::NullType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.JSON) {
      return {format3::LogicalType::JsonType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.BSON) {
      return {format3::LogicalType::BsonType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.UUID) {
      return {format3::LogicalType::UUIDType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.FLOAT16) {
      return {format3::LogicalType::Float16Type, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.VARIANT) {
      return {format3::LogicalType::VariantType, format3::CreateEmpty(root).Union()};
    } else if (t.__isset.GEOMETRY) {
      auto crs = root.CreateString(t.GEOMETRY.crs);
      return {format3::LogicalType::GeometryType,
              format3::CreateGeometryType(root, crs).Union()};
    } else if (t.__isset.GEOGRAPHY) {
      auto crs = t.GEOGRAPHY.__isset.crs ? root.CreateString(t.GEOGRAPHY.crs) : 0;
      return {format3::LogicalType::GeographyType,
              format3::CreateGeographyType(root, crs, To(t.GEOGRAPHY.algorithm)).Union()};
    }
    ::arrow::Unreachable();
  }

  std::pair<format3::LogicalType, ::flatbuffers::Offset<void>> To(
      const format::ConvertedType::type& t, const format::SchemaElement& e) {
    if (t == format::ConvertedType::UTF8) {
      return {format3::LogicalType::StringType, format3::CreateEmpty(root).Union()};
    } else if (t == format::ConvertedType::MAP) {
      return {format3::LogicalType::MapType, format3::CreateEmpty(root).Union()};
    } else if (t == format::ConvertedType::LIST) {
      return {format3::LogicalType::ListType, format3::CreateEmpty(root).Union()};
    } else if (t == format::ConvertedType::ENUM) {
      return {format3::LogicalType::EnumType, format3::CreateEmpty(root).Union()};
    } else if (t == format::ConvertedType::DECIMAL) {
      return {format3::LogicalType::DecimalType,
              format3::CreateDecimalOpts(root, e.precision, e.scale).Union()};
    } else if (t == format::ConvertedType::DATE) {
      return {format3::LogicalType::DateType, format3::CreateEmpty(root).Union()};
    } else if (t == format::ConvertedType::TIME_MILLIS) {
      return {format3::LogicalType::TimeType,
              format3::CreateTimeOpts(root, false, format3::TimeUnit::MS).Union()};
    } else if (t == format::ConvertedType::TIME_MICROS) {
      return {format3::LogicalType::TimeType,
              format3::CreateTimeOpts(root, false, format3::TimeUnit::US).Union()};
    } else if (t == format::ConvertedType::TIMESTAMP_MILLIS) {
      return {format3::LogicalType::TimestampType,
              format3::CreateTimeOpts(root, false, format3::TimeUnit::MS).Union()};
    } else if (t == format::ConvertedType::TIMESTAMP_MICROS) {
      return {format3::LogicalType::TimestampType,
              format3::CreateTimeOpts(root, false, format3::TimeUnit::US).Union()};
    } else if (t == format::ConvertedType::INT_8) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(root, 8, true).Union()};
    } else if (t == format::ConvertedType::INT_16) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(root, 16, true).Union()};
    } else if (t == format::ConvertedType::INT_32) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(root, 32, true).Union()};
    } else if (t == format::ConvertedType::INT_64) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(root, 64, true).Union()};
    } else if (t == format::ConvertedType::UINT_8) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(root, 8, false).Union()};
    } else if (t == format::ConvertedType::UINT_16) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(root, 16, false).Union()};
    } else if (t == format::ConvertedType::UINT_32) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(root, 32, false).Union()};
    } else if (t == format::ConvertedType::UINT_64) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(root, 64, false).Union()};
    } else if (t == format::ConvertedType::JSON) {
      return {format3::LogicalType::JsonType, format3::CreateEmpty(root).Union()};
    } else if (t == format::ConvertedType::BSON) {
      return {format3::LogicalType::BsonType, format3::CreateEmpty(root).Union()};
    } else if (t == format::ConvertedType::INTERVAL) {
      // todo: no logical type?
      return {format3::LogicalType::NONE, format3::CreateEmpty(root).Union()};
    }
    ::arrow::Unreachable();
  }

  std::pair<format3::ColumnOrder, flatbuffers::Offset<void>> To(
      const format::ColumnOrder& co) {
    (void)co;
    assert(co.__isset.TYPE_ORDER);
    return {format3::ColumnOrder::TypeDefinedOrder, format3::CreateEmpty(root).Union()};
  }

  auto To(const format::SchemaElement& e, size_t schema_idx) {
    auto name = root.CreateSharedString(e.name);
    std::optional<std::pair<format3::LogicalType, flatbuffers::Offset<void>>>
        logical_type;
    if (e.__isset.logicalType) logical_type = To(e.logicalType);
    if (!logical_type && e.__isset.converted_type) logical_type = To(e.converted_type, e);
    std::optional<std::pair<format3::ColumnOrder, flatbuffers::Offset<void>>>
        column_order;
    if (md.__isset.column_orders && !md.column_orders.empty()) {
      if (auto cc_idx = colmap.ToCc(schema_idx)) {
        column_order = To(md.column_orders.at(*cc_idx));
      }
    }

    format3::SchemaElementBuilder b(root);
    if (e.__isset.type) b.add_type(To(e.type));
    if (e.__isset.repetition_type) b.add_repetition_type(To(e.repetition_type));
    if (logical_type) {
      b.add_logical_type_type(logical_type->first);
      b.add_logical_type(logical_type->second);
    }
    if (e.__isset.type_length) b.add_type_length(e.type_length);
    b.add_name(name);
    if (e.__isset.num_children) b.add_num_children(e.num_children);
    if (e.__isset.field_id) b.add_field_id(e.field_id);
    if (column_order) {
      b.add_column_order_type(column_order->first);
      b.add_column_order(column_order->second);
    }
    return b.Finish();
  }

  auto To(const format::KeyValue& kv, size_t) {
    auto key = root.CreateSharedString(kv.key);
    std::optional<::flatbuffers::Offset<::flatbuffers::String>> val;
    if (kv.__isset.value) val = root.CreateSharedString(kv.value);

    format3::KVBuilder b(root);
    b.add_key(key);
    if (val) b.add_val(*val);
    return b.Finish();
  }

  using Binary = ::flatbuffers::Offset<flatbuffers::Vector<signed char>>;
  std::optional<Binary> ToBinary(const std::string& v) {
    if (v.empty()) return std::nullopt;
    return std::make_optional(
        root.CreateVector(reinterpret_cast<const int8_t*>(v.data()), v.size()));
  }

  struct FixedVal {
    uint32_t lo4 = 0;
    uint64_t lo8 = 0;
    uint64_t hi8 = 0;
    uint8_t len = 0;
  };

  auto To(const format::ColumnMetaData& cm) {
    if (!cm.encoding_stats.empty()) {
      for (auto&& e : cm.encoding_stats) {
        if (e.page_type != format::PageType::DATA_PAGE &&
            e.page_type != format::PageType::DATA_PAGE_V2)
          continue;
        if (e.encoding != format::Encoding::PLAIN_DICTIONARY &&
            e.encoding != format::Encoding::RLE_DICTIONARY) {
          return false;
        }
      }
      return true;
    }
    bool has_plain_dictionary_encoding = false;
    bool has_non_dictionary_encoding = false;
    for (auto encoding : cm.encodings) {
      switch (encoding) {
        case format::Encoding::PLAIN_DICTIONARY:
          // PLAIN_DICTIONARY encoding was present, which means at
          // least one page was dictionary encoded and v1.0 encodings are used.
          has_plain_dictionary_encoding = true;
          break;
        case format::Encoding::RLE:
        case format::Encoding::BIT_PACKED:
          // Other than for boolean values, RLE and BIT_PACKED are only used for
          // repetition or definition levels. Additionally booleans are not dictionary
          // encoded hence it is safe to disregard the case where some boolean data pages
          // are dictionary encoded and some boolean pages are RLE/BIT_PACKED encoded.
          break;
        default:
          has_non_dictionary_encoding = true;
          break;
      }
    }
    if (has_plain_dictionary_encoding) {
      // Return true, if there are no encodings other than dictionary or
      // repetition/definition levels.
      return !has_non_dictionary_encoding;
    }

    // If PLAIN_DICTIONARY wasn't present, then either the column is not
    // dictionary-encoded, or the 2.0 encoding, RLE_DICTIONARY, was used.
    // For 2.0, this cannot determine whether a page fell back to non-dictionary encoding
    // without page encoding stats.
    return false;
  }

  auto To(const format::Statistics& s, size_t, size_t col_idx) {
    std::optional<MinMax> mm;
    if (s.__isset.min_value && s.__isset.max_value) {
      const auto& col = md.schema[colmap.ToSchema(col_idx)];
      mm = Pack(col.type, s.min_value, s.is_min_value_exact, s.max_value,
                s.is_max_value_exact);
    }

    std::optional<flatbuffers::Offset<flatbuffers::String>> prefix;
    if (mm && !mm->prefix.empty()) prefix = root.CreateSharedString(mm->prefix);

    format3::StatisticsBuilder b(root);
    if (s.__isset.null_count) b.add_null_count(s.null_count);
    if (prefix) b.add_prefix(*prefix);
    if (mm) {
      b.add_min_lo4(mm->min.lo4);
      b.add_min_lo8(mm->min.lo8);
      b.add_min_hi8(mm->min.hi8);
      b.add_min_len(mm->min.len);
      b.add_max_lo4(mm->max.lo4);
      b.add_max_lo8(mm->max.lo8);
      b.add_max_hi8(mm->max.hi8);
      b.add_max_len(mm->max.len);
    }
    return b.Finish();
  }

  auto To(const format::ColumnMetaData& cm, size_t rg_idx, size_t col_idx) {
    auto codec = To(cm.codec);
    auto kv = OptTo(cm.key_value_metadata);
    std::optional<decltype(To(cm.statistics, rg_idx, col_idx))> statistics;
    if (cm.__isset.statistics) statistics = To(cm.statistics, rg_idx, col_idx);

    // All offsets are relative to the row group.
    const auto& rg = md.row_groups[rg_idx];

    format3::ColumnMetadataBuilder b(root);
    b.add_codec(codec);
    if (rg.num_rows != cm.num_values) b.add_num_values(cm.num_values);
    b.add_total_uncompressed_size(cm.total_uncompressed_size);
    b.add_total_compressed_size(cm.total_compressed_size);
    if (kv) b.add_key_value_metadata(*kv);
    if (cm.data_page_offset != rg.file_offset) {
      b.add_data_page_offset(cm.data_page_offset);
    }
    if (cm.__isset.index_page_offset) b.add_index_page_offset(cm.index_page_offset);
    if (cm.__isset.dictionary_page_offset) {
      b.add_dictionary_page_offset(cm.dictionary_page_offset);
    }
    if (statistics) b.add_statistics(*statistics);
    b.add_is_fully_dict_encoded(To(cm));
    if (cm.__isset.bloom_filter_offset) b.add_bloom_filter_offset(cm.bloom_filter_offset);
    if (cm.__isset.bloom_filter_length) b.add_bloom_filter_length(cm.bloom_filter_length);

    ARROW_DCHECK_EQ(cm.path_in_schema, colmap.ToPath(md.schema, col_idx));
    return b.Finish();
  }

  auto To(const format::ColumnChunk& cc, size_t rg_idx, size_t col_idx) {
    auto meta_data = To(cc.meta_data, rg_idx, col_idx);

    format3::ColumnChunkBuilder b(root);
    b.add_meta_data(meta_data);
    // TODO
    // - crypto_metadata
    // - encrypted_column_metadata
    return b.Finish();
  }

  auto To(const format::SortingColumn& sc, size_t, size_t) {
    return format3::CreateSortingColumn(root, sc.column_idx, sc.descending,
                                        sc.nulls_first);
  }

  auto To(const format::RowGroup& rg, size_t rg_idx) {
    auto columns = To(rg.columns, rg_idx);
    auto sorting_columns = OptTo(rg.sorting_columns, rg_idx);

    format3::RowGroupBuilder b(root);
    b.add_columns(columns);
    b.add_total_byte_size(rg.total_byte_size);
    b.add_num_rows(rg.num_rows);
    if (sorting_columns) b.add_sorting_columns(*sorting_columns);
    if (rg.__isset.file_offset) b.add_file_offset(rg.file_offset);
    if (rg.__isset.total_compressed_size)
      b.add_total_compressed_size(rg.total_compressed_size);
    if (rg.__isset.ordinal) b.add_ordinal(rg.ordinal);
    return b.Finish();
  }

  void To() {
    auto schema = To(md.schema);
    auto row_groups = To(md.row_groups);
    auto kv = OptTo(md.key_value_metadata);
    auto created_by = root.CreateString(md.created_by);

    format3::FileMetaDataBuilder b(root);
    b.add_version(md.version);
    b.add_schema(schema);
    b.add_num_rows(md.num_rows);
    b.add_row_groups(row_groups);
    if (kv) b.add_kv(*kv);
    b.add_created_by(created_by);  // check empty
    // TODO
    // - encryption_algorithm
    // - footer_signing_key_metadata
    root.Finish(b.Finish());
  }
};

std::string ToFlatbuffer(format::FileMetaData* md) {
  FlatbufferConverter conv(*md);
  conv.To();
  return std::string(reinterpret_cast<const char*>(conv.root.GetBufferPointer()),
                     conv.root.GetSize());
}

format::FileMetaData FromFlatbuffer(const format3::FileMetaData* md) {
  format::FileMetaData result;
  ThriftConverter conv(md, &result);
  conv.To();
  return result;
}
} // namespace parquet