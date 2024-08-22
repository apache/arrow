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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <optional>
#include <string_view>
#include <variant>

#include "arrow/util/endian.h"
#include "arrow/util/unreachable.h"
#include "benchmark/benchmark.h"
#include "flatbuffers/flatbuffers.h"
#include "generated/parquet3_generated.h"
#include "generated/parquet_types.h"
#include "parquet/thrift_internal.h"

// Baseline
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=4040696
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=25680
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1379944
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=1174696
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=5906552
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2801976
//
//
// Remove deprecated ColumnChunk.file_offset
//
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=1292376
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=5056
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=214192
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=226112
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=2961808
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=1120360
//
//
// Optimized statistics
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=3874720
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=8208
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1304568
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=721728
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=5538032
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2599152
//
//
// Optimized offsets/num_vals
//
// RowGroup size limited to 2^31 and num values to 2^31. ColumnChunk offsets are relative to
// RowGroup starts which makes then all int32s too.
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=3331720
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=7560
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1214640
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=620344
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=4801656
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2390080
//
//
// Optimized num_values when ColumnChunk is dense
//
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=3265192
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=7568
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1207416
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=611720
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=4433832
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2343608
//
//
// Replace encoding stats with is_fully_dict_encoded
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=2622520
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=6792
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1106640
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=489016
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=4433656
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2062584
//
//
// Remove path_in_schema in ColumnMetadata
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=2092640
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=5544
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1045304
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=333176
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=3697824
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=1922080
//
//

#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"

namespace parquet {
namespace {

template <typename... T>
struct Overload : T... {
  using T::operator()...;
};
template <typename... T> Overload(T...) -> Overload<T...>;

class ThriftConverter {
 public:
  template <typename T, typename... Args>
  auto operator()(const flatbuffers::Vector<flatbuffers::Offset<T>>* in, Args&&... args) {
    std::vector<decltype((*this)(in->Get(0), std::forward<Args>(args)..., 0))> out;
    out.reserve(in->size());
    int idx = 0;
    for (auto&& e : *in) out.push_back((*this)(e, std::forward<Args>(args)..., idx++));
    return out;
  }

  template <typename T, typename... Args>
  auto operator()(const flatbuffers::Vector<T>* in, Args&&... args) {
    std::vector<decltype((*this)(in->Get(0), std::forward<Args>(args)..., 0))> out;
    out.reserve(in->size());
    int idx = 0;
    for (auto&& e : *in) out.push_back((*this)(e, std::forward<Args>(args)..., idx++));
    return out;
  }

  auto operator()(const flatbuffers::String* s, int) { return s->str(); }

  auto operator()(format3::TimeUnit t) {
    format::TimeUnit out;
    if (t == format3::TimeUnit::NS)
      out.__set_NANOS({});
    else if (t == format3::TimeUnit::US)
      out.__set_MICROS({});
    else
      out.__set_MILLIS({});
    return out;
  }

  auto operator()(format3::Type t) { return static_cast<format::Type::type>(t); }
  auto operator()(format3::FieldRepetitionType t) {
    return static_cast<format::FieldRepetitionType::type>(t);
  }
  auto operator()(format3::CompressionCodec c) {
    return static_cast<format::CompressionCodec::type>(c);
  }
  auto operator()(format3::Encoding e, int idx) {
    return static_cast<format::Encoding::type>(e);
  }
  auto operator()(format3::PageType t) { return static_cast<format::PageType::type>(t); }

  auto operator()(format3::LogicalType t, int col_idx) {
    const format3::SchemaElement* e = md_->schema()->Get(col_idx);
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
        tt.__set_unit((*this)(e->logical_type_as_TimeType()->unit()));
        tt.__set_isAdjustedToUTC(e->logical_type_as_TimeType()->is_adjusted_to_utc());
        out.__set_TIME(tt);
        break;
      }
      case format3::LogicalType::TimestampType: {
        format::TimestampType tt;
        tt.__set_unit((*this)(e->logical_type_as_TimestampType()->unit()));
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
      default:
        ::arrow::Unreachable();
    }
    return out;
  }

  auto operator()(format3::ColumnOrder co) {
    format::ColumnOrder out;
    out.__set_TYPE_ORDER({});
    return out;
  }

  auto operator()(const format3::SchemaElement* e, int col_idx) {
    format::SchemaElement out;
    out.name = e->name()->str();
    if (e->type()) {
      out.__isset.type = true;
      out.type = (*this)(*e->type());
    }
    out.__isset.repetition_type = true;
    out.repetition_type = (*this)(e->repetition_type());
    if (e->logical_type_type() != format3::LogicalType::NONE) {
      out.__isset.logicalType = true;
      out.logicalType = (*this)(e->logical_type_type(), col_idx);
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

  auto operator()(const format3::KV* kv, int) {
    format::KeyValue out;
    if (kv->key()->size() != 0) out.__set_key(kv->key()->str());
    if (kv->val()->size() != 0) out.__set_value(kv->val()->str());
    return out;
  }

  auto ToString(const flatbuffers::Vector<signed char>* v) {
    if (!v) return std::string();
    return std::string(reinterpret_cast<const char*>(v->data()), v->size());
  }

  auto ToString(uint32_t v) {
    uint32_t x = ::arrow::bit_util::FromLittleEndian(v);
    return std::string(reinterpret_cast<const char*>(&x), 4);
  }

  auto ToString(uint64_t v) {
    uint64_t x = ::arrow::bit_util::FromLittleEndian(v);
    return std::string(reinterpret_cast<const char*>(&x), 8);
  }

  auto operator()(const format3::Statistics* s, int rg_idx, int col_idx) {
    format::Statistics out;
    if (s->null_count()) {
      out.__isset.null_count = true;
      out.null_count = *s->null_count();
    }
    if (s->distinct_count()) {
      out.__isset.distinct_count = true;
      out.distinct_count = *s->distinct_count();
    }
    auto type = md_->schema()->Get(col_idx)->type();
    if (type) {
      switch (*type) {
        case format3::Type::BOOLEAN:
          out.max_value = std::string(1, s->max1());
          out.min_value = std::string(1, s->min1());
          break;
        case format3::Type::INT32:
        case format3::Type::FLOAT:
          out.max_value = ToString(s->max4());
          out.min_value = ToString(s->min4());
          break;
        case format3::Type::INT64:
        case format3::Type::DOUBLE:
          out.max_value = ToString(s->max8());
          out.min_value = ToString(s->min8());
          break;
        default:
          out.max_value = ToString(s->max_value());
          out.min_value = ToString(s->min_value());
      }
    }
    out.is_max_value_exact = s->is_max_value_exact();
    out.is_min_value_exact = s->is_min_value_exact();
    return out;
  }

  auto operator()(const format3::PageEncodingStats* s, int rg_idx, int col_idx, int idx) {
    format::PageEncodingStats out;
    out.page_type = (*this)(s->page_type());
    out.encoding = (*this)(s->encoding(), idx);
    out.count = s->count();
    return out;
  }

  auto operator()(const format3::ColumnMetadata* cm, int rg_idx, int col_idx) {
    auto off = md_->row_groups()->Get(rg_idx)->file_offset();
    format::ColumnMetaData out;
    out.encodings = (*this)(cm->encodings());
    out.codec = (*this)(cm->codec());
    out.num_values =
        flatbuffers::IsFieldPresent(cm, format3::ColumnMetadata::VT_NUM_VALUES)
            ? cm->num_values()
            : md_->row_groups()->Get(rg_idx)->num_rows();
    out.total_uncompressed_size = cm->total_uncompressed_size();
    out.key_value_metadata = (*this)(cm->key_value_metadata());
    out.data_page_offset = cm->data_page_offset();
    if (cm->index_page_offset()) {
      out.__isset.index_page_offset = true;
      out.index_page_offset = *cm->index_page_offset() + off;
    }
    if (cm->dictionary_page_offset()) {
      out.__isset.dictionary_page_offset = true;
      out.dictionary_page_offset = *cm->dictionary_page_offset() + off;
    }
    out.statistics = (*this)(cm->statistics(), rg_idx, col_idx);
    if (cm->bloom_filter_offset()) {
      out.__isset.bloom_filter_offset = true;
      out.bloom_filter_offset = *cm->bloom_filter_offset() + off;
    }
    if (cm->bloom_filter_length()) {
      out.__isset.bloom_filter_length = true;
      out.bloom_filter_length = *cm->bloom_filter_length() + off;
    }
    return out;
  }

  auto operator()(const format3::ColumnChunk* cc, int rg_idx, int col_idx) {
    format::ColumnChunk out;
    out.__isset.meta_data = true;
    out.meta_data = (*this)(cc->meta_data(), rg_idx, col_idx);
    return out;
  }

  auto operator()(const format3::SortingColumn* sc, int rg_idx, int) {
    format::SortingColumn out;
    out.column_idx = sc->column_idx();
    out.descending = sc->descending();
    out.nulls_first = sc->nulls_first();
    return out;
  }

  auto operator()(const format3::RowGroup* rg, int rg_idx) {
    format::RowGroup out;
    out.columns = (*this)(rg->columns(), rg_idx);
    out.sorting_columns = (*this)(rg->sorting_columns(), rg_idx);
    out.__set_total_byte_size(rg->total_byte_size());
    if (rg->ordinal()) out.__set_ordinal(*rg->ordinal());
    out.__set_file_offset(rg->file_offset());
    out.__set_total_compressed_size(rg->total_compressed_size());
    return out;
  }

  auto operator()(const format3::FileMetaData* md) {
    md_ = md;
    format::FileMetaData out;
    out.__set_version(md->version());
    out.__set_num_rows(md->num_rows());
    out.row_groups = (*this)(md->row_groups());
    out.key_value_metadata = (*this)(md->kv());
    if (md->created_by()->size() > 0) {
      out.__isset.created_by = true;
      out.created_by = md->created_by()->str();
    }
    out.schema = (*this)(md->schema());
    for (auto* e : *md->schema()) {
      if (e->num_children() == 0) out.column_orders.push_back((*this)(e->column_order_type()));
    }
    return out;
  }

 private:
  const format3::FileMetaData* md_;
};

class FlatbufferConverter {
 public:
  auto operator()(format::TimeUnit t) {
    if (t.__isset.NANOS) return format3::TimeUnit::NS;
    if (t.__isset.MICROS) return format3::TimeUnit::US;
    return format3::TimeUnit::MS;
  }

  auto operator()(format::Type::type t) { return static_cast<format3::Type>(t); }
  auto operator()(format::FieldRepetitionType::type t) {
    return static_cast<format3::FieldRepetitionType>(t);
  }
  auto operator()(format::CompressionCodec::type c) {
    return static_cast<format3::CompressionCodec>(c);
  }
  auto operator()(format::Encoding::type e, int idx) {
    return static_cast<format3::Encoding>(e);
  }
  auto operator()(format::PageType::type p) { return static_cast<format3::PageType>(p); }

  template <typename T, typename... Args>
  auto operator()(std::vector<T>* in, Args&&... args) {
    std::vector<decltype((*this)(in->at(0), std::forward<Args>(args)..., 0))> out;
    out.reserve(in->size());
    int idx = 0;
    for (auto&& e : *in) out.push_back((*this)(e, std::forward<Args>(args)..., idx++));
    return builder_.CreateVector(out);
  }

  std::pair<format3::LogicalType, flatbuffers::Offset<void>> operator()(
      const format::LogicalType& t) {
    if (t.__isset.STRING) {
      return {format3::LogicalType::StringType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.MAP) {
      return {format3::LogicalType::MapType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.LIST) {
      return {format3::LogicalType::ListType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.ENUM) {
      return {format3::LogicalType::EnumType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.DECIMAL) {
      return {format3::LogicalType::DecimalType,
              format3::CreateDecimalOpts(builder_, t.DECIMAL.precision, t.DECIMAL.scale)
                  .Union()};
    } else if (t.__isset.DATE) {
      return {format3::LogicalType::DateType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.TIME) {
      auto tu = (*this)(t.TIME.unit);
      return {format3::LogicalType::TimeType,
              format3::CreateTimeOpts(builder_, t.TIME.isAdjustedToUTC, tu).Union()};
    } else if (t.__isset.TIMESTAMP) {
      auto tu = (*this)(t.TIMESTAMP.unit);
      return {format3::LogicalType::TimestampType,
              format3::CreateTimeOpts(builder_, t.TIMESTAMP.isAdjustedToUTC, tu).Union()};
    } else if (t.__isset.INTEGER) {
      return {format3::LogicalType::IntType,
              format3::CreateIntOpts(builder_, t.INTEGER.bitWidth, t.INTEGER.isSigned)
                  .Union()};
    } else if (t.__isset.UNKNOWN) {
      return {format3::LogicalType::NullType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.JSON) {
      return {format3::LogicalType::JsonType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.BSON) {
      return {format3::LogicalType::BsonType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.UUID) {
      return {format3::LogicalType::UUIDType, format3::CreateEmpty(builder_).Union()};
    } else if (t.__isset.FLOAT16) {
      return {format3::LogicalType::Float16Type, format3::CreateEmpty(builder_).Union()};
    }
    ::arrow::Unreachable();
  }

  std::pair<format3::ColumnOrder, flatbuffers::Offset<void>> operator()(
      const format::ColumnOrder& co) {
    return {format3::ColumnOrder::TypeDefinedOrder,
            format3::CreateEmpty(builder_).Union()};
  }

  auto operator()(format::SchemaElement& e, int col_idx) {
    auto name = builder_.CreateSharedString(e.name);
    std::optional<std::pair<format3::LogicalType, flatbuffers::Offset<void>>>
        logical_type;
    if (e.__isset.logicalType) logical_type = (*this)(e.logicalType);
    std::optional<std::pair<format3::ColumnOrder, flatbuffers::Offset<void>>>
        column_order;
    if (md_->__isset.column_orders && !md_->column_orders.empty()) {
      if (size_t i = column_orders_[col_idx]; i != ~0ull) {
        column_order = (*this)(md_->column_orders.at(i));
      }
    }

    format3::SchemaElementBuilder b(builder_);
    if (e.__isset.type) b.add_type((*this)(e.type));
    if (e.__isset.repetition_type) b.add_repetition_type((*this)(e.repetition_type));
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

  auto operator()(format::KeyValue& kv, int idx) {
    auto key = builder_.CreateSharedString(kv.key);
    auto val = builder_.CreateSharedString(kv.value);

    format3::KVBuilder b(builder_);
    b.add_key(key);
    b.add_val(val);
    return b.Finish();
  }

  using Binary = flatbuffers::Offset<flatbuffers::Vector<signed char>>;
  std::optional<Binary> ToBinary(std::string& v) {
    if (v.empty()) return std::nullopt;
    return std::make_optional(
        builder_.CreateVector(reinterpret_cast<const int8_t*>(v.data()), v.size()));
  }

  auto operator()(format::Statistics& s, int rg_idx, int col_idx) {
    using Val = std::variant<std::monostate, uint8_t, uint32_t, uint64_t, Binary>;
    auto to_val = [this](std::string& v, format::Type::type type) -> Val {
      if (v.empty()) return std::monostate{};
      switch (type) {
        case format::Type::BOOLEAN:
          return static_cast<uint8_t>(v[0]);
        case format::Type::INT32:
        case format::Type::FLOAT: {
          uint32_t x;
          memcpy(&x, v.data(), 4);
          return ::arrow::bit_util::ToLittleEndian(x);
        }
        case format::Type::INT64:
        case format::Type::DOUBLE: {
          uint64_t x;
          memcpy(&x, v.data(), 8);
          return ::arrow::bit_util::ToLittleEndian(x);
        }
        default:
          return builder_.CreateVector(reinterpret_cast<const int8_t*>(v.data()),
                                       v.size());
      }
    };
    Val max_value = to_val(s.max_value, md_->schema[col_idx].type);
    Val min_value = to_val(s.min_value, md_->schema[col_idx].type);

    format3::StatisticsBuilder b(builder_);
    if (s.__isset.null_count) b.add_null_count(s.null_count);
    if (s.__isset.distinct_count) b.add_distinct_count(s.distinct_count);
    std::visit(
        Overload{[&](Binary v) { b.add_max_value(v); },
                 [&](uint64_t v) { b.add_max8(v); }, [&](uint32_t v) { b.add_max4(v); },
                 [&](uint8_t v) { b.add_max1(v); }, [&](std::monostate) {}},
        max_value);
    std::visit(
        Overload{[&](Binary v) { b.add_min_value(v); },
                 [&](uint64_t v) { b.add_min8(v); }, [&](uint32_t v) { b.add_min4(v); },
                 [&](uint8_t v) { b.add_min1(v); }, [&](std::monostate) {}},
        min_value);

    if (s.__isset.is_max_value_exact) b.add_is_max_value_exact(s.is_max_value_exact);
    if (s.__isset.is_min_value_exact) b.add_is_min_value_exact(s.is_min_value_exact);
    return b.Finish();
  }

  auto operator()(format::PageEncodingStats& s, int rg_idx, int col_idx, int idx) {
    format3::PageEncodingStatsBuilder b(builder_);
    b.add_page_type((*this)(s.page_type));
    b.add_encoding((*this)(s.encoding, idx));
    b.add_count(s.count);
    return b.Finish();
  }

  auto ToSharedStrings(std::vector<std::string>& in) {
    std::vector<flatbuffers::Offset<flatbuffers::String>> out;
    out.reserve(in.size());
    for (auto&& s : in) out.push_back(builder_.CreateSharedString(s));
    return builder_.CreateVector(out);
  }

  auto operator()(format::ColumnMetaData& cm, int rg_idx, int col_idx) {
    auto encodings = (*this)(&cm.encodings);
    auto codec = (*this)(cm.codec);
    auto kv_metadata = (*this)(&cm.key_value_metadata);
    auto statistics = (*this)(cm.statistics, rg_idx, col_idx);

    // All offsets are relative to the row group.
    const auto& rg = md_->row_groups[rg_idx];
    auto off = rg.file_offset;

    format3::ColumnMetadataBuilder b(builder_);
    b.add_encodings(encodings);
    b.add_codec(codec);
    if (rg.num_rows != cm.num_values) b.add_num_values(cm.num_values);
    b.add_total_uncompressed_size(cm.total_uncompressed_size);
    b.add_key_value_metadata(kv_metadata);
    if (cm.data_page_offset != md_->row_groups[rg_idx].columns[col_idx].file_offset) {
      b.add_data_page_offset(cm.data_page_offset - off);
    }
    if (cm.__isset.index_page_offset) b.add_index_page_offset(cm.index_page_offset - off);
    if (cm.__isset.dictionary_page_offset) {
      b.add_dictionary_page_offset(cm.dictionary_page_offset - off);
    }
    b.add_statistics(statistics);
    bool is_fully_dict_encoded = true;
    for (auto&& e : cm.encoding_stats) {
      if (e.page_type != format::PageType::DATA_PAGE) continue;
      if (e.page_type != format::PageType::DATA_PAGE_V2) continue;
      if (e.encoding != format::Encoding::PLAIN_DICTIONARY &&
          e.encoding != format::Encoding::RLE_DICTIONARY) {
        is_fully_dict_encoded = false;
        break;
      }
    }
    b.add_is_fully_dict_encoded(is_fully_dict_encoded);

    if (cm.__isset.bloom_filter_offset) b.add_bloom_filter_offset(cm.bloom_filter_offset);
    if (cm.__isset.bloom_filter_length) b.add_bloom_filter_length(cm.bloom_filter_length);

    return b.Finish();
  }

  auto operator()(format::ColumnChunk& cc, int rg_idx, int col_idx) {
    auto meta_data = (*this)(cc.meta_data, rg_idx, col_idx);

    format3::ColumnChunkBuilder b(builder_);
    b.add_meta_data(meta_data);
    cc.__isset.crypto_metadata = false;  // TODO
    if (cc.__isset.encrypted_column_metadata) {
      cc.encrypted_column_metadata.clear();  // TODO
      cc.__isset.encrypted_column_metadata = false;
    }

    return b.Finish();
  }

  auto operator()(format::SortingColumn& sc, int rg_idx, int col_idx) {
    return format3::CreateSortingColumn(builder_, sc.column_idx, sc.descending,
                                        sc.nulls_first);
  }

  auto operator()(format::RowGroup& rg, int rg_idx) {
    auto columns = (*this)(&rg.columns, rg_idx);
    auto sorting_columns = (*this)(&rg.sorting_columns, rg_idx);

    format3::RowGroupBuilder b(builder_);
    b.add_columns(columns);
    b.add_total_byte_size(rg.total_byte_size);
    b.add_sorting_columns(sorting_columns);
    if (rg.__isset.file_offset) b.add_file_offset(rg.file_offset);
    if (rg.__isset.total_compressed_size)
      b.add_total_compressed_size(rg.total_compressed_size);
    if (rg.__isset.ordinal) b.add_ordinal(rg.ordinal);
    return b.Finish();
  }

  std::string operator()(format::FileMetaData* md) {
    md_ = md;
    builder_.Clear();
    // Column orders are only for leaf nodes so create a map from schema index to order.
    column_orders_.resize(md_->schema.size());
    for (size_t i = 0, j = 0; i < md_->schema.size(); ++i) {
      column_orders_[i] = md_->schema[i].num_children == 0 ? j++ : ~0ull;
    }

    auto schema = (*this)(&md_->schema);
    auto row_groups = (*this)(&md_->row_groups);
    auto kv = (*this)(&md_->key_value_metadata);
    auto created_by = builder_.CreateString(md_->created_by);

    format3::FileMetaDataBuilder b(builder_);
    b.add_schema(schema);
    b.add_num_rows(md_->num_rows);
    b.add_row_groups(row_groups);
    b.add_kv(kv);
    b.add_created_by(created_by);                    // check empty
    md_->__isset.encryption_algorithm = false;       // TODO
    if (md_->__isset.footer_signing_key_metadata) {  // TODO
      md_->footer_signing_key_metadata.clear();
      md_->__isset.footer_signing_key_metadata = false;
    }
    auto out = b.Finish();
    builder_.Finish(out);
    return {reinterpret_cast<const char*>(builder_.GetBufferPointer()),
            builder_.GetSize()};
  }

 private:
  format::FileMetaData* md_;
  std::vector<size_t> column_orders_;
  flatbuffers::FlatBufferBuilder builder_;
};

std::string ReadFile(const std::string& name) {
  std::stringstream buffer;
  std::ifstream t(name);
  buffer << t.rdbuf();
  return buffer.str();
}

std::string Serialize(const format::FileMetaData& md) {
  ThriftSerializer ser;
  std::string out;
  ser.SerializeToString(&md, &out);
  return out;
}

format::FileMetaData DeserializeThrift(const std::string& bytes) {
  ThriftDeserializer des(100 << 20, 100 << 20);
  format::FileMetaData md;
  uint32_t n = bytes.size();
  des.DeserializeMessage(reinterpret_cast<const uint8_t*>(bytes.data()), &n, &md);
  return md;
}

std::string ToFlatbuffer(format::FileMetaData* md) { return FlatbufferConverter()(md); }
format::FileMetaData ToThrift(const format3::FileMetaData* md) {
  return ThriftConverter()(md);
}

struct Footer {
  std::string name;
  std::string thrift;
  std::string flatbuf;
  format::FileMetaData md;

  static Footer Make(const char* filename) {
    std::string bytes = ReadFile(filename);
    auto md = DeserializeThrift(bytes);
    std::string flatbuf =
        FlatbufferConverter()(&md);  // removes unsupported fields for fair comparison
    return {basename(filename), Serialize(md), std::move(flatbuf), std::move(md)};
  }
};

void Parse(benchmark::State& state, const Footer& footer) {
  for (auto _ : state) {
    auto md = DeserializeThrift(footer.thrift);
  }
}

void AppendUleb(uint32_t x, std::string* out) {
  while (true) {
    uint8_t c = x & 0x7F;
    if (x < 0x80) return out->push_back(c);
    out->push_back(c + 0x80);
    x >>= 7;
  }
};

std::string AppendExtension(std::string thrift, const std::string& ext) {
  thrift.back() = '\x08';      // replace stop field with binary type
  AppendUleb(32767, &thrift);  // field-id
  AppendUleb(ext.size(), &thrift);
  thrift += ext;
  thrift += '\x00';  // add the stop field
  return thrift;
}

void EncodeFlatbuf(benchmark::State& state, const Footer& footer) {
  auto md = footer.md;
  for (auto _ : state) {
    auto ser = ToFlatbuffer(&md);
    benchmark::DoNotOptimize(ser);
  }
}

void ThriftFromFlatbuf(benchmark::State& state, const Footer& footer) {
  for (auto _ : state) {
    auto md = ToThrift(format3::GetFileMetaData(footer.flatbuf.data()));
    benchmark::DoNotOptimize(md);
  }
}

void ParseAndVerifyFlatbuf(benchmark::State& state, const Footer& footer) {
  for (auto _ : state) {
    flatbuffers::Verifier v(reinterpret_cast<const uint8_t*>(footer.flatbuf.data()),
                            footer.flatbuf.size());
    auto fmd = format3::GetFileMetaData(footer.flatbuf.data());
    DCHECK_EQ(fmd->num_rows(), footer.md.num_rows);
    bool ok = fmd->Verify(v);
    DCHECK(ok);
  }
}

void ParseWithExtension(benchmark::State& state, const Footer& footer) {
  auto with_ext = AppendExtension(footer.thrift, footer.flatbuf);

  for (auto _ : state) {
    auto md = DeserializeThrift(with_ext);
  }
}

void Analyze(std::string_view name, const format::FileMetaData& md) {
  std::cerr << "Analyzing: " << name << "\n";
  std::vector<int> sizes;
  int num_cols = md.schema.size() - 1;
  size_t stats_bytes = 0, kv_bytes = 0;
  for (auto& kv : md.key_value_metadata) kv_bytes += kv.key.size() + kv.value.size();
  for (int i = 0; i < num_cols; ++i) {
    for (auto& rg : md.row_groups) {
      auto& cc = rg.columns[i];
      if (!cc.__isset.meta_data) continue;
      auto& cmd = cc.meta_data;
      auto& s = cmd.statistics;
      stats_bytes += s.max_value.size() + s.min_value.size();
      for (auto& kv : cmd.key_value_metadata) kv_bytes += kv.key.size() + kv.value.size();
    }
  }
  std::cerr << "num-rgs=" << md.row_groups.size() << " num-cols=" << num_cols
            << " stats_bytes=" << stats_bytes << " kv_bytes=" << kv_bytes << "\n";
}

struct SiBytes {
  double v;
  int p;
  char u;
};

SiBytes ToSiBytes(size_t v) {
  auto kb = [](size_t n) { return n << 10; };
  auto mb = [](size_t n) { return n << 20; };
  auto gb = [](size_t n) { return n << 30; };
  if (v < kb(2)) return {v * 1., 0, ' '};
  if (v < mb(2)) return {v / 1024., v < kb(10), 'k'};
  if (v < gb(2)) return {v / 1024. / 1024, v < mb(10), 'M'};
  return {v / 1024. / 1024 / 1024, v < gb(10), 'G'};
}

}  // namespace
}  // namespace parquet

int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  std::vector<parquet::Footer> footers;
  for (int i = 1; i < argc; ++i) footers.push_back(parquet::Footer::Make(argv[i]));
  struct {
    std::string name;
    void (*fn)(benchmark::State&, const parquet::Footer&);
  } benchmarks[] = {
      {"Parse", parquet::Parse},
      {"ParseWithExtension", parquet::ParseWithExtension},
      {"EncodeFlatbuf", parquet::EncodeFlatbuf},
      {"ThriftFromFlatbuf", parquet::ThriftFromFlatbuf},
      {"ParseAndVerifyFlatbuf", parquet::ParseAndVerifyFlatbuf},
  };
  for (auto&& footer : footers) {
    for (auto&& [n, fn] : benchmarks) {
      char buf[1024];
      snprintf(buf, sizeof(buf), "%30s/%s", footer.name.c_str(), n.c_str());
      ::benchmark::RegisterBenchmark(buf, fn, footer)->Unit(benchmark::kMillisecond);
    }
  }

  char key[1024];
  char val[1024];
  for (size_t i = 0; i < footers.size(); ++i) {
    auto&& f = footers[i];
    snprintf(key, sizeof(key), "%lu/%s", i, basename(f.name.c_str()));
    auto thrift = parquet::ToSiBytes(f.thrift.size());
    auto flatbuf = parquet::ToSiBytes(f.flatbuf.size());
    snprintf(val, sizeof(val), "num-rgs=%lu num-cols=%lu thrift=%.*f%c flatbuf=%.*f%c",
             f.md.row_groups.size(), f.md.schema.size() - 1, thrift.p, thrift.v, thrift.u,
             flatbuf.p, flatbuf.v, flatbuf.u);
    ::benchmark::AddCustomContext(key, val);
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
