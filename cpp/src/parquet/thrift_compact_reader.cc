// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// SPDX-License-Identifier: Apache-2.0

#include "parquet/thrift_compact_reader.h"

#include <utility>

namespace parquet {
namespace internal {

using namespace format;

namespace {

void ReadDirectStatistics(CompactInput& input, DirectStatistics* out) {
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::Binary);
        out->max = input.ReadBinaryView();
        out->has_max = true;
        break;
      case 2:
        Expect(f.type, CompactType::Binary);
        out->min = input.ReadBinaryView();
        out->has_min = true;
        break;
      case 3:
        Expect(f.type, CompactType::I64);
        out->null_count = input.ReadI64();
        out->has_null_count = true;
        break;
      case 4:
        Expect(f.type, CompactType::I64);
        out->distinct_count = input.ReadI64();
        out->has_distinct_count = true;
        break;
      case 5:
        Expect(f.type, CompactType::Binary);
        out->max_value = input.ReadBinaryView();
        out->has_max_value = true;
        break;
      case 6:
        Expect(f.type, CompactType::Binary);
        out->min_value = input.ReadBinaryView();
        out->has_min_value = true;
        break;
      case 7:
        Expect(f.type, CompactType::BoolTrue);
        out->is_max_value_exact = f.bool_value;
        out->has_max_value_exact = true;
        break;
      case 8:
        Expect(f.type, CompactType::BoolTrue);
        out->is_min_value_exact = f.bool_value;
        out->has_min_value_exact = true;
        break;
      case 9:
        Expect(f.type, CompactType::I64);
        out->nan_count = input.ReadI64();
        out->has_nan_count = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
}

void ReadDirectSizeStatistics(CompactInput& input, DirectSizeStatistics* out) {
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I64);
        out->unencoded_byte_array_data_bytes = input.ReadI64();
        out->has_unencoded_byte_array_data_bytes = true;
        break;
      case 2:
        Expect(f.type, CompactType::List);
        out->repetition_level_histogram = input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); });
        out->has_repetition_level_histogram = true;
        break;
      case 3:
        Expect(f.type, CompactType::List);
        out->definition_level_histogram = input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); });
        out->has_definition_level_histogram = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
}

template <typename T, typename F>
T ReadStructValue(CompactInput& input, F&& f) {
  T value;
  f(input, &value);
  return value;
}

template <typename T, typename F>
std::vector<T> ReadStructList(CompactInput& input, F&& f) {
  return input.ReadList<T>(CompactType::Struct,
                           [&](CompactType) { return ReadStructValue<T>(input, f); });
}

template <typename T, typename F>
void ReadOptionalStruct(CompactInput& input, const CompactField& field, F&& f, T* out) {
  Expect(field.type, CompactType::Struct);
  f(input, out);
}

}  // namespace

void ReadSizeStatistics(CompactInput& input, format::SizeStatistics* out) {
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I64);
        out->__set_unencoded_byte_array_data_bytes(input.ReadI64());
        break;
      case 2: {
        Expect(f.type, CompactType::List);
        out->repetition_level_histogram = input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); });
        out->__isset.repetition_level_histogram = true;
        break;
      }
      case 3: {
        Expect(f.type, CompactType::List);
        out->definition_level_histogram = input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); });
        out->__isset.definition_level_histogram = true;
        break;
      }
      default:
        input.Skip(f.type);
    }
  });
}

void ReadBoundingBox(CompactInput& input, format::BoundingBox* out) {
  bool xmin = false, xmax = false, ymin = false, ymax = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::Double);
        out->__set_xmin(input.ReadDouble());
        xmin = true;
        break;
      case 2:
        Expect(f.type, CompactType::Double);
        out->__set_xmax(input.ReadDouble());
        xmax = true;
        break;
      case 3:
        Expect(f.type, CompactType::Double);
        out->__set_ymin(input.ReadDouble());
        ymin = true;
        break;
      case 4:
        Expect(f.type, CompactType::Double);
        out->__set_ymax(input.ReadDouble());
        ymax = true;
        break;
      case 5:
        Expect(f.type, CompactType::Double);
        out->__set_zmin(input.ReadDouble());
        break;
      case 6:
        Expect(f.type, CompactType::Double);
        out->__set_zmax(input.ReadDouble());
        break;
      case 7:
        Expect(f.type, CompactType::Double);
        out->__set_mmin(input.ReadDouble());
        break;
      case 8:
        Expect(f.type, CompactType::Double);
        out->__set_mmax(input.ReadDouble());
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!xmin || !xmax || !ymin || !ymax)
    throw ParquetException("Required BoundingBox field missing");
}

void ReadGeospatialStatistics(CompactInput& input, format::GeospatialStatistics* out) {
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        ReadOptionalStruct(input, f, ReadBoundingBox, &out->bbox);
        out->__isset.bbox = true;
        break;
      case 2:
        Expect(f.type, CompactType::List);
        out->geospatial_types = input.ReadList<int32_t>(
            CompactType::I32, [&](CompactType) { return input.ReadI32(); });
        out->__isset.geospatial_types = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
}

void ReadStatistics(CompactInput& input, format::Statistics* out) {
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::Binary);
        out->max = input.ReadBinary();
        out->__isset.max = true;
        break;
      case 2:
        Expect(f.type, CompactType::Binary);
        out->min = input.ReadBinary();
        out->__isset.min = true;
        break;
      case 3:
        Expect(f.type, CompactType::I64);
        out->__set_null_count(input.ReadI64());
        break;
      case 4:
        Expect(f.type, CompactType::I64);
        out->__set_distinct_count(input.ReadI64());
        break;
      case 5:
        Expect(f.type, CompactType::Binary);
        out->max_value = input.ReadBinary();
        out->__isset.max_value = true;
        break;
      case 6:
        Expect(f.type, CompactType::Binary);
        out->min_value = input.ReadBinary();
        out->__isset.min_value = true;
        break;
      case 7:
        Expect(f.type, CompactType::BoolTrue);
        out->__set_is_max_value_exact(f.bool_value);
        break;
      case 8:
        Expect(f.type, CompactType::BoolTrue);
        out->__set_is_min_value_exact(f.bool_value);
        break;
      case 9:
        Expect(f.type, CompactType::I64);
        out->__set_nan_count(input.ReadI64());
        break;
      default:
        input.Skip(f.type);
    }
  });
}

void ReadTimeUnit(CompactInput& input, format::TimeUnit* out) {
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_MILLIS({});
        break;
      case 2:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_MICROS({});
        break;
      case 3:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_NANOS({});
        break;
      default:
        input.Skip(f.type);
    }
  });
}

void ReadLogicalType(CompactInput& input, format::LogicalType* out) {
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_STRING({});
        break;
      case 2:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_MAP({});
        break;
      case 3:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_LIST({});
        break;
      case 4:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_ENUM({});
        break;
      case 5: {
        format::DecimalType value;
        bool scale = false, precision = false;
        input.ReadStruct([&](const CompactField& nested) {
          if (nested.id == 1) {
            Expect(nested.type, CompactType::I32);
            value.__set_scale(input.ReadI32());
            scale = true;
          } else if (nested.id == 2) {
            Expect(nested.type, CompactType::I32);
            value.__set_precision(input.ReadI32());
            precision = true;
          } else
            input.Skip(nested.type);
        });
        if (!scale || !precision)
          throw ParquetException("Required DecimalType field missing");
        out->__set_DECIMAL(value);
        break;
      }
      case 6:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_DATE({});
        break;
      case 7: {
        Expect(f.type, CompactType::Struct);
        format::TimeType value;
        bool utc = false, unit = false;
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::BoolTrue);
            value.__set_isAdjustedToUTC(n.bool_value);
            utc = true;
          } else if (n.id == 2) {
            Expect(n.type, CompactType::Struct);
            ReadTimeUnit(input, &value.unit);
            value.__set_unit(value.unit);
            unit = true;
          } else
            input.Skip(n.type);
        });
        if (!utc || !unit) throw ParquetException("Required TimeType field missing");
        out->__set_TIME(value);
        break;
      }
      case 8: {
        Expect(f.type, CompactType::Struct);
        format::TimestampType value;
        bool utc = false, unit = false;
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::BoolTrue);
            value.__set_isAdjustedToUTC(n.bool_value);
            utc = true;
          } else if (n.id == 2) {
            Expect(n.type, CompactType::Struct);
            ReadTimeUnit(input, &value.unit);
            value.__set_unit(value.unit);
            unit = true;
          } else
            input.Skip(n.type);
        });
        if (!utc || !unit) throw ParquetException("Required TimestampType field missing");
        out->__set_TIMESTAMP(value);
        break;
      }
      case 10: {
        Expect(f.type, CompactType::Struct);
        format::IntType value;
        bool width = false, sign = false;
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::Byte);
            value.__set_bitWidth(input.ReadI8());
            width = true;
          } else if (n.id == 2) {
            Expect(n.type, CompactType::BoolTrue);
            value.__set_isSigned(n.bool_value);
            sign = true;
          } else
            input.Skip(n.type);
        });
        if (!width || !sign) throw ParquetException("Required IntType field missing");
        out->__set_INTEGER(value);
        break;
      }
      case 11:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_UNKNOWN({});
        break;
      case 12:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_JSON({});
        break;
      case 13:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_BSON({});
        break;
      case 14:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_UUID({});
        break;
      case 15:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_FLOAT16({});
        break;
      case 16: {
        format::VariantType value;
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::Byte);
            value.__set_specification_version(input.ReadI8());
          } else
            input.Skip(n.type);
        });
        out->__set_VARIANT(value);
        break;
      }
      case 17: {
        format::GeometryType value;
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::Binary);
            value.crs = input.ReadBinary();
            value.__isset.crs = true;
          } else
            input.Skip(n.type);
        });
        out->__set_GEOMETRY(value);
        break;
      }
      case 18: {
        format::GeographyType value;
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::Binary);
            value.crs = input.ReadBinary();
            value.__isset.crs = true;
          } else if (n.id == 2) {
            Expect(n.type, CompactType::I32);
            value.__set_algorithm(ReadEnum<EdgeInterpolationAlgorithm::type>(input));
          } else
            input.Skip(n.type);
        });
        out->__set_GEOGRAPHY(value);
        break;
      }
      case 19:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_FILE({});
        break;
      default:
        input.Skip(f.type);
    }
  });
}

void ReadSchemaElement(CompactInput& input, format::SchemaElement* out) {
  bool name = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->__set_type(ReadEnum<Type::type>(input));
        break;
      case 2:
        Expect(f.type, CompactType::I32);
        out->__set_type_length(input.ReadI32());
        break;
      case 3:
        Expect(f.type, CompactType::I32);
        out->__set_repetition_type(ReadEnum<FieldRepetitionType::type>(input));
        break;
      case 4:
        Expect(f.type, CompactType::Binary);
        out->name = input.ReadBinary();
        name = true;
        break;
      case 5:
        Expect(f.type, CompactType::I32);
        out->__set_num_children(input.ReadI32());
        break;
      case 6:
        Expect(f.type, CompactType::I32);
        out->__set_converted_type(ReadEnum<ConvertedType::type>(input));
        break;
      case 7:
        Expect(f.type, CompactType::I32);
        out->__set_scale(input.ReadI32());
        break;
      case 8:
        Expect(f.type, CompactType::I32);
        out->__set_precision(input.ReadI32());
        break;
      case 9:
        Expect(f.type, CompactType::I32);
        out->__set_field_id(input.ReadI32());
        break;
      case 10:
        Expect(f.type, CompactType::Struct);
        ReadLogicalType(input, &out->logicalType);
        out->__isset.logicalType = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!name) throw ParquetException("Required SchemaElement name missing");
}

void ReadDirectSchemaElement(CompactInput& input, DirectSchemaElement* out) {
  bool name = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->type = ReadEnum<Type::type>(input);
        out->has_type = true;
        break;
      case 2:
        Expect(f.type, CompactType::I32);
        out->type_length = input.ReadI32();
        out->has_type_length = true;
        break;
      case 3:
        Expect(f.type, CompactType::I32);
        out->repetition_type = ReadEnum<FieldRepetitionType::type>(input);
        out->has_repetition_type = true;
        break;
      case 4:
        Expect(f.type, CompactType::Binary);
        out->name = input.ReadBinaryView();
        name = true;
        break;
      case 5:
        Expect(f.type, CompactType::I32);
        out->num_children = input.ReadI32();
        out->has_num_children = true;
        break;
      case 6:
        Expect(f.type, CompactType::I32);
        out->converted_type = ReadEnum<ConvertedType::type>(input);
        out->has_converted_type = true;
        break;
      case 7:
        Expect(f.type, CompactType::I32);
        out->scale = input.ReadI32();
        out->has_scale = true;
        break;
      case 8:
        Expect(f.type, CompactType::I32);
        out->precision = input.ReadI32();
        out->has_precision = true;
        break;
      case 9:
        Expect(f.type, CompactType::I32);
        out->field_id = input.ReadI32();
        out->has_field_id = true;
        break;
      case 10:
        Expect(f.type, CompactType::Struct);
        ReadLogicalType(input, &out->logical_type);
        out->has_logical_type = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!name) throw ParquetException("Required SchemaElement name missing");
}

void ReadKeyValue(CompactInput& input, format::KeyValue* out) {
  bool key = false;
  input.ReadStruct([&](const CompactField& f) {
    if (f.id == 1) {
      Expect(f.type, CompactType::Binary);
      out->key = input.ReadBinary();
      key = true;
    } else if (f.id == 2) {
      Expect(f.type, CompactType::Binary);
      out->value = input.ReadBinary();
      out->__isset.value = true;
    } else
      input.Skip(f.type);
  });
  if (!key) throw ParquetException("Required KeyValue key missing");
}

void ReadDirectKeyValue(CompactInput& input, DirectKeyValue* out) {
  bool key = false;
  input.ReadStruct([&](const CompactField& f) {
    if (f.id == 1) {
      Expect(f.type, CompactType::Binary);
      out->key = input.ReadBinaryView();
      key = true;
    } else if (f.id == 2) {
      Expect(f.type, CompactType::Binary);
      out->value = input.ReadBinaryView();
      out->has_value = true;
    } else {
      input.Skip(f.type);
    }
  });
  if (!key) throw ParquetException("Required KeyValue key missing");
}

void ReadSortingColumn(CompactInput& input, format::SortingColumn* out) {
  bool a = false, b = false, c = false;
  input.ReadStruct([&](const CompactField& f) {
    if (f.id == 1) {
      Expect(f.type, CompactType::I32);
      out->__set_column_idx(input.ReadI32());
      a = true;
    } else if (f.id == 2) {
      Expect(f.type, CompactType::BoolTrue);
      out->__set_descending(f.bool_value);
      b = true;
    } else if (f.id == 3) {
      Expect(f.type, CompactType::BoolTrue);
      out->__set_nulls_first(f.bool_value);
      c = true;
    } else
      input.Skip(f.type);
  });
  if (!a || !b || !c) throw ParquetException("Required SortingColumn field missing");
}

void ReadPageEncodingStats(CompactInput& input, format::PageEncodingStats* out) {
  bool a = false, b = false, c = false;
  input.ReadStruct([&](const CompactField& f) {
    if (f.id == 1) {
      Expect(f.type, CompactType::I32);
      out->__set_page_type(ReadEnum<PageType::type>(input));
      a = true;
    } else if (f.id == 2) {
      Expect(f.type, CompactType::I32);
      out->__set_encoding(ReadEnum<Encoding::type>(input));
      b = true;
    } else if (f.id == 3) {
      Expect(f.type, CompactType::I32);
      out->__set_count(input.ReadI32());
      c = true;
    } else
      input.Skip(f.type);
  });
  if (!a || !b || !c) throw ParquetException("Required PageEncodingStats field missing");
}

void ReadColumnMetaData(CompactInput& input, format::ColumnMetaData* out) {
  bool type = false, enc = false, path = false, codec = false, values = false, us = false,
       cs = false, offset = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->__set_type(ReadEnum<Type::type>(input));
        type = true;
        break;
      case 2:
        out->encodings = input.ReadList<Encoding::type>(
            CompactType::I32,
            [&](CompactType) { return ReadEnum<Encoding::type>(input); });
        enc = true;
        break;
      case 3:
        out->path_in_schema = input.ReadList<std::string>(
            CompactType::Binary, [&](CompactType) { return input.ReadBinary(); });
        path = true;
        break;
      case 4:
        Expect(f.type, CompactType::I32);
        out->__set_codec(ReadEnum<CompressionCodec::type>(input));
        codec = true;
        break;
      case 5:
        Expect(f.type, CompactType::I64);
        out->__set_num_values(input.ReadI64());
        values = true;
        break;
      case 6:
        Expect(f.type, CompactType::I64);
        out->__set_total_uncompressed_size(input.ReadI64());
        us = true;
        break;
      case 7:
        Expect(f.type, CompactType::I64);
        out->__set_total_compressed_size(input.ReadI64());
        cs = true;
        break;
      case 8:
        out->key_value_metadata = ReadStructList<format::KeyValue>(input, ReadKeyValue);
        out->__isset.key_value_metadata = true;
        break;
      case 9:
        Expect(f.type, CompactType::I64);
        out->__set_data_page_offset(input.ReadI64());
        offset = true;
        break;
      case 10:
        Expect(f.type, CompactType::I64);
        out->__set_index_page_offset(input.ReadI64());
        break;
      case 11:
        Expect(f.type, CompactType::I64);
        out->__set_dictionary_page_offset(input.ReadI64());
        break;
      case 12:
        Expect(f.type, CompactType::Struct);
        if (input.skip_column_stats() || input.skip_for_column()) {
          input.Skip(f.type);
        } else {
          ReadStatistics(input, &out->statistics);
          out->__isset.statistics = true;
        }
        break;
      case 13:
        if (input.skip_encoding_stats() || input.skip_for_column()) {
          Expect(f.type, CompactType::List);
          input.Skip(f.type);
        } else {
          out->encoding_stats =
              ReadStructList<format::PageEncodingStats>(input, ReadPageEncodingStats);
          out->__isset.encoding_stats = true;
        }
        break;
      case 14:
        Expect(f.type, CompactType::I64);
        out->__set_bloom_filter_offset(input.ReadI64());
        break;
      case 15:
        Expect(f.type, CompactType::I32);
        out->__set_bloom_filter_length(input.ReadI32());
        break;
      case 16:
        Expect(f.type, CompactType::Struct);
        if (input.skip_size_stats() || input.skip_for_column()) {
          input.Skip(f.type);
        } else {
          ReadSizeStatistics(input, &out->size_statistics);
          out->__isset.size_statistics = true;
        }
        break;
      case 17:
        Expect(f.type, CompactType::Struct);
        ReadGeospatialStatistics(input, &out->geospatial_statistics);
        out->__isset.geospatial_statistics = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!type || !enc || !path || !codec || !values || !us || !cs || !offset) {
    throw ParquetException("Required ColumnMetaData field missing");
  }
}

void ReadColumnCryptoMetaData(CompactInput& input, format::ColumnCryptoMetaData* out) {
  bool seen = false;
  input.ReadStruct([&](const CompactField& f) {
    if (f.id == 1) {
      Expect(f.type, CompactType::Struct);
      input.ReadEmptyStruct();
      out->__set_ENCRYPTION_WITH_FOOTER_KEY({});
      seen = true;
    } else if (f.id == 2) {
      format::EncryptionWithColumnKey value;
      bool path = false;
      Expect(f.type, CompactType::Struct);
      input.ReadStruct([&](const CompactField& n) {
        if (n.id == 1) {
          value.path_in_schema = input.ReadList<std::string>(
              CompactType::Binary, [&](CompactType) { return input.ReadBinary(); });
          path = true;
        } else if (n.id == 2) {
          Expect(n.type, CompactType::Binary);
          value.key_metadata = input.ReadBinary();
          value.__isset.key_metadata = true;
        } else
          input.Skip(n.type);
      });
      if (!path) throw ParquetException("Required encryption path missing");
      out->__set_ENCRYPTION_WITH_COLUMN_KEY(value);
      seen = true;
    } else
      input.Skip(f.type);
  });
  if (!seen) throw ParquetException("Empty ColumnCryptoMetaData union");
}

void ReadColumnChunk(CompactInput& input, format::ColumnChunk* out) {
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::Binary);
        out->file_path = input.ReadBinary();
        out->__isset.file_path = true;
        break;
      case 2:
        Expect(f.type, CompactType::I64);
        out->__set_file_offset(input.ReadI64());
        break;
      case 3:
        Expect(f.type, CompactType::Struct);
        ReadColumnMetaData(input, &out->meta_data);
        out->__isset.meta_data = true;
        break;
      case 4:
        Expect(f.type, CompactType::I64);
        out->__set_offset_index_offset(input.ReadI64());
        break;
      case 5:
        Expect(f.type, CompactType::I32);
        out->__set_offset_index_length(input.ReadI32());
        break;
      case 6:
        Expect(f.type, CompactType::I64);
        out->__set_column_index_offset(input.ReadI64());
        break;
      case 7:
        Expect(f.type, CompactType::I32);
        out->__set_column_index_length(input.ReadI32());
        break;
      case 8:
        Expect(f.type, CompactType::Struct);
        ReadColumnCryptoMetaData(input, &out->crypto_metadata);
        out->__isset.crypto_metadata = true;
        break;
      case 9:
        Expect(f.type, CompactType::Binary);
        out->encrypted_column_metadata = input.ReadBinary();
        out->__isset.encrypted_column_metadata = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
}

void ReadRowGroup(CompactInput& input, format::RowGroup* out) {
  bool columns = false, bytes = false, rows = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::List);
        {
          const uint64_t size = input.ReadListSize(CompactType::Struct);
          out->columns.clear();
          out->columns.reserve(static_cast<size_t>(size));
          for (uint64_t i = 0; i < size; ++i) {
            input.SetColumnIndex(static_cast<int32_t>(i));
            format::ColumnChunk column;
            ReadColumnChunk(input, &column);
            out->columns.emplace_back(std::move(column));
          }
          input.SetColumnIndex(-1);
        }
        columns = true;
        break;
      case 2:
        Expect(f.type, CompactType::I64);
        out->__set_total_byte_size(input.ReadI64());
        bytes = true;
        break;
      case 3:
        Expect(f.type, CompactType::I64);
        out->__set_num_rows(input.ReadI64());
        rows = true;
        break;
      case 4:
        out->sorting_columns =
            ReadStructList<format::SortingColumn>(input, ReadSortingColumn);
        out->__isset.sorting_columns = true;
        break;
      case 5:
        Expect(f.type, CompactType::I64);
        out->__set_file_offset(input.ReadI64());
        break;
      case 6:
        Expect(f.type, CompactType::I64);
        out->__set_total_compressed_size(input.ReadI64());
        break;
      case 7:
        Expect(f.type, CompactType::I16);
        out->__set_ordinal(input.ReadI16());
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!columns || !bytes || !rows)
    throw ParquetException("Required RowGroup field missing");
}

void ReadColumnOrder(CompactInput& input, format::ColumnOrder* out) {
  bool seen = false;
  input.ReadStruct([&](const CompactField& f) {
    if (f.id == 1) {
      Expect(f.type, CompactType::Struct);
      input.ReadEmptyStruct();
      out->__set_TYPE_ORDER({});
      seen = true;
    } else if (f.id == 2) {
      Expect(f.type, CompactType::Struct);
      input.ReadEmptyStruct();
      out->__set_IEEE_754_TOTAL_ORDER({});
      seen = true;
    } else if (f.id == 3) {
      Expect(f.type, CompactType::Struct);
      input.ReadEmptyStruct();
      out->__set_INT96_TIMESTAMP_ORDER({});
      seen = true;
    } else
      input.Skip(f.type);
  });
  if (!seen) throw ParquetException("Empty ColumnOrder union");
}

void ReadEncryptionAlgorithm(CompactInput& input, format::EncryptionAlgorithm* out) {
  bool seen = false;
  input.ReadStruct([&](const CompactField& f) {
    if (f.id == 1 || f.id == 2) {
      Expect(f.type, CompactType::Struct);
      std::string prefix, unique;
      bool supply = false;
      input.ReadStruct([&](const CompactField& n) {
        if (n.id == 1) {
          Expect(n.type, CompactType::Binary);
          prefix = input.ReadBinary();
        } else if (n.id == 2) {
          Expect(n.type, CompactType::Binary);
          unique = input.ReadBinary();
        } else if (n.id == 3) {
          Expect(n.type, CompactType::BoolTrue);
          supply = n.bool_value;
        } else
          input.Skip(n.type);
      });
      if (f.id == 1) {
        format::AesGcmV1 value;
        value.aad_prefix = std::move(prefix);
        value.aad_file_unique = std::move(unique);
        value.supply_aad_prefix = supply;
        value.__isset.aad_prefix = !value.aad_prefix.empty();
        value.__isset.aad_file_unique = !value.aad_file_unique.empty();
        value.__set_supply_aad_prefix(supply);
        out->__set_AES_GCM_V1(value);
      } else {
        format::AesGcmCtrV1 value;
        value.aad_prefix = std::move(prefix);
        value.aad_file_unique = std::move(unique);
        value.supply_aad_prefix = supply;
        value.__isset.aad_prefix = !value.aad_prefix.empty();
        value.__isset.aad_file_unique = !value.aad_file_unique.empty();
        value.__set_supply_aad_prefix(supply);
        out->__set_AES_GCM_CTR_V1(value);
      }
      seen = true;
    } else
      input.Skip(f.type);
  });
  if (!seen) throw ParquetException("Empty EncryptionAlgorithm union");
}

void ReadFileMetaData(CompactInput& input, format::FileMetaData* out) {
  bool version = false, schema = false, rows = false, groups = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->__set_version(input.ReadI32());
        version = true;
        break;
      case 2:
        out->schema = ReadStructList<format::SchemaElement>(input, ReadSchemaElement);
        schema = true;
        break;
      case 3:
        Expect(f.type, CompactType::I64);
        out->__set_num_rows(input.ReadI64());
        rows = true;
        break;
      case 4:
        out->row_groups = ReadStructList<format::RowGroup>(input, ReadRowGroup);
        groups = true;
        break;
      case 5:
        out->key_value_metadata = ReadStructList<format::KeyValue>(input, ReadKeyValue);
        out->__isset.key_value_metadata = true;
        break;
      case 6:
        Expect(f.type, CompactType::Binary);
        out->created_by = input.ReadBinary();
        out->__isset.created_by = true;
        break;
      case 7:
        out->column_orders = ReadStructList<format::ColumnOrder>(input, ReadColumnOrder);
        out->__isset.column_orders = true;
        break;
      case 8:
        Expect(f.type, CompactType::Struct);
        ReadEncryptionAlgorithm(input, &out->encryption_algorithm);
        out->__set_encryption_algorithm(out->encryption_algorithm);
        break;
      case 9:
        Expect(f.type, CompactType::Binary);
        out->footer_signing_key_metadata = input.ReadBinary();
        out->__isset.footer_signing_key_metadata = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!version || !schema || !rows || !groups)
    throw ParquetException("Required FileMetaData field missing");
}

void ReadPageLocation(CompactInput& input, format::PageLocation* out) {
  bool offset = false, size = false, row = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I64);
        out->__set_offset(input.ReadI64());
        offset = true;
        break;
      case 2:
        Expect(f.type, CompactType::I32);
        out->__set_compressed_page_size(input.ReadI32());
        size = true;
        break;
      case 3:
        Expect(f.type, CompactType::I64);
        out->__set_first_row_index(input.ReadI64());
        row = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!offset || !size || !row)
    throw ParquetException("Required PageLocation field missing");
}

void ReadOffsetIndex(CompactInput& input, format::OffsetIndex* out) {
  bool pages = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::List);
        out->page_locations =
            ReadStructList<format::PageLocation>(input, ReadPageLocation);
        pages = true;
        break;
      case 2:
        Expect(f.type, CompactType::List);
        out->unencoded_byte_array_data_bytes = input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); });
        out->__isset.unencoded_byte_array_data_bytes = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!pages) throw ParquetException("Required OffsetIndex page_locations missing");
}

void ReadColumnIndex(CompactInput& input, format::ColumnIndex* out) {
  bool null_pages = false, min_values = false, max_values = false, order = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::List);
        out->null_pages = input.ReadList<bool>(
            CompactType::BoolTrue, [&](CompactType) { return input.ReadBoolElement(); });
        null_pages = true;
        break;
      case 2:
        Expect(f.type, CompactType::List);
        out->min_values = input.ReadList<std::string>(
            CompactType::Binary, [&](CompactType) { return input.ReadBinary(); });
        min_values = true;
        break;
      case 3:
        Expect(f.type, CompactType::List);
        out->max_values = input.ReadList<std::string>(
            CompactType::Binary, [&](CompactType) { return input.ReadBinary(); });
        max_values = true;
        break;
      case 4:
        Expect(f.type, CompactType::I32);
        out->__set_boundary_order(ReadEnum<BoundaryOrder::type>(input));
        order = true;
        break;
      case 5:
        Expect(f.type, CompactType::List);
        out->__set_null_counts(input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); }));
        break;
      case 6:
        Expect(f.type, CompactType::List);
        out->__set_repetition_level_histograms(input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); }));
        break;
      case 7:
        Expect(f.type, CompactType::List);
        out->__set_definition_level_histograms(input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); }));
        break;
      case 8:
        Expect(f.type, CompactType::List);
        out->__set_nan_counts(input.ReadList<int64_t>(
            CompactType::I64, [&](CompactType) { return input.ReadI64(); }));
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!null_pages || !min_values || !max_values || !order) {
    throw ParquetException("Required ColumnIndex field missing");
  }
}

void ReadDataPageHeader(CompactInput& input, format::DataPageHeader* out) {
  bool values = false, encoding = false, definition = false, repetition = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->__set_num_values(input.ReadI32());
        values = true;
        break;
      case 2:
        Expect(f.type, CompactType::I32);
        out->__set_encoding(ReadEnum<Encoding::type>(input));
        encoding = true;
        break;
      case 3:
        Expect(f.type, CompactType::I32);
        out->__set_definition_level_encoding(ReadEnum<Encoding::type>(input));
        definition = true;
        break;
      case 4:
        Expect(f.type, CompactType::I32);
        out->__set_repetition_level_encoding(ReadEnum<Encoding::type>(input));
        repetition = true;
        break;
      case 5:
        Expect(f.type, CompactType::Struct);
        if (input.skip_page_stats()) {
          input.Skip(f.type);
        } else {
          ReadStatistics(input, &out->statistics);
          out->__isset.statistics = true;
        }
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!values || !encoding || !definition || !repetition) {
    throw ParquetException("Required DataPageHeader field missing");
  }
}

void ReadDataPageHeaderV2(CompactInput& input, format::DataPageHeaderV2* out) {
  bool values = false, nulls = false, rows = false, encoding = false, definition = false,
       repetition = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->__set_num_values(input.ReadI32());
        values = true;
        break;
      case 2:
        Expect(f.type, CompactType::I32);
        out->__set_num_nulls(input.ReadI32());
        nulls = true;
        break;
      case 3:
        Expect(f.type, CompactType::I32);
        out->__set_num_rows(input.ReadI32());
        rows = true;
        break;
      case 4:
        Expect(f.type, CompactType::I32);
        out->__set_encoding(ReadEnum<Encoding::type>(input));
        encoding = true;
        break;
      case 5:
        Expect(f.type, CompactType::I32);
        out->__set_definition_levels_byte_length(input.ReadI32());
        definition = true;
        break;
      case 6:
        Expect(f.type, CompactType::I32);
        out->__set_repetition_levels_byte_length(input.ReadI32());
        repetition = true;
        break;
      case 7:
        Expect(f.type, CompactType::BoolTrue);
        out->__set_is_compressed(f.bool_value);
        break;
      case 8:
        Expect(f.type, CompactType::Struct);
        if (input.skip_page_stats()) {
          input.Skip(f.type);
        } else {
          ReadStatistics(input, &out->statistics);
          out->__isset.statistics = true;
        }
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!values || !nulls || !rows || !encoding || !definition || !repetition) {
    throw ParquetException("Required DataPageHeaderV2 field missing");
  }
}

void ReadDictionaryPageHeader(CompactInput& input, format::DictionaryPageHeader* out) {
  bool values = false, encoding = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->__set_num_values(input.ReadI32());
        values = true;
        break;
      case 2:
        Expect(f.type, CompactType::I32);
        out->__set_encoding(ReadEnum<Encoding::type>(input));
        encoding = true;
        break;
      case 3:
        Expect(f.type, CompactType::BoolTrue);
        out->__set_is_sorted(f.bool_value);
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!values || !encoding)
    throw ParquetException("Required DictionaryPageHeader field missing");
}

void ReadPageHeader(CompactInput& input, format::PageHeader* out) {
  bool type = false, uncompressed = false, compressed = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->__set_type(ReadEnum<PageType::type>(input));
        type = true;
        break;
      case 2:
        Expect(f.type, CompactType::I32);
        out->__set_uncompressed_page_size(input.ReadI32());
        uncompressed = true;
        break;
      case 3:
        Expect(f.type, CompactType::I32);
        out->__set_compressed_page_size(input.ReadI32());
        compressed = true;
        break;
      case 4:
        Expect(f.type, CompactType::I32);
        out->__set_crc(input.ReadI32());
        break;
      case 5:
        Expect(f.type, CompactType::Struct);
        ReadDataPageHeader(input, &out->data_page_header);
        out->__isset.data_page_header = true;
        break;
      case 6:
        Expect(f.type, CompactType::Struct);
        input.ReadEmptyStruct();
        out->__set_index_page_header({});
        break;
      case 7:
        Expect(f.type, CompactType::Struct);
        ReadDictionaryPageHeader(input, &out->dictionary_page_header);
        out->__isset.dictionary_page_header = true;
        break;
      case 8:
        Expect(f.type, CompactType::Struct);
        ReadDataPageHeaderV2(input, &out->data_page_header_v2);
        out->__isset.data_page_header_v2 = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!type || !uncompressed || !compressed)
    throw ParquetException("Required PageHeader field missing");
}

void ReadBloomFilterHeader(CompactInput& input, format::BloomFilterHeader* out) {
  bool bytes = false, algorithm = false, hash = false, compression = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->__set_numBytes(input.ReadI32());
        bytes = true;
        break;
      case 2:
        Expect(f.type, CompactType::Struct);
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::Struct);
            input.ReadEmptyStruct();
            out->algorithm.__set_BLOCK({});
          } else
            input.Skip(n.type);
        });
        algorithm = true;
        break;
      case 3:
        Expect(f.type, CompactType::Struct);
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::Struct);
            input.ReadEmptyStruct();
            out->hash.__set_XXHASH({});
          } else
            input.Skip(n.type);
        });
        hash = true;
        break;
      case 4:
        Expect(f.type, CompactType::Struct);
        input.ReadStruct([&](const CompactField& n) {
          if (n.id == 1) {
            Expect(n.type, CompactType::Struct);
            input.ReadEmptyStruct();
            out->compression.__set_UNCOMPRESSED({});
          } else
            input.Skip(n.type);
        });
        compression = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!bytes || !algorithm || !hash || !compression) {
    throw ParquetException("Required BloomFilterHeader field missing");
  }
}

namespace {

void ReadDirectColumnMetadata(CompactInput& input, DirectColumnMetadata* out) {
  bool type = false, encodings = false, path = false, codec = false, values = false,
       uncompressed = false, compressed = false, data_offset = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->type = ReadEnum<format::Type::type>(input);
        type = true;
        break;
      case 2:
        Expect(f.type, CompactType::List);
        out->encodings = input.ReadList<format::Encoding::type>(
            CompactType::I32,
            [&](CompactType) { return ReadEnum<format::Encoding::type>(input); });
        encodings = true;
        break;
      case 3:
        Expect(f.type, CompactType::List);
        out->path_in_schema = input.ReadList<std::string_view>(
            CompactType::Binary, [&](CompactType) { return input.ReadBinaryView(); });
        path = true;
        break;
      case 4:
        Expect(f.type, CompactType::I32);
        out->codec = ReadEnum<format::CompressionCodec::type>(input);
        codec = true;
        break;
      case 5:
        Expect(f.type, CompactType::I64);
        out->num_values = input.ReadI64();
        values = true;
        break;
      case 6:
        Expect(f.type, CompactType::I64);
        out->total_uncompressed_size = input.ReadI64();
        uncompressed = true;
        break;
      case 7:
        Expect(f.type, CompactType::I64);
        out->total_compressed_size = input.ReadI64();
        compressed = true;
        break;
      case 8:
        Expect(f.type, CompactType::List);
        out->key_value_metadata =
            ReadStructList<DirectKeyValue>(input, ReadDirectKeyValue);
        break;
      case 9:
        Expect(f.type, CompactType::I64);
        out->data_page_offset = input.ReadI64();
        data_offset = true;
        break;
      case 10:
        Expect(f.type, CompactType::I64);
        out->index_page_offset = input.ReadI64();
        out->has_index_page_offset = true;
        break;
      case 11:
        Expect(f.type, CompactType::I64);
        out->dictionary_page_offset = input.ReadI64();
        out->has_dictionary_page_offset = true;
        break;
      case 12:
        Expect(f.type, CompactType::Struct);
        if (input.skip_column_stats() || input.skip_for_column())
          input.Skip(f.type);
        else {
          ReadDirectStatistics(input, &out->statistics);
          out->has_statistics = true;
        }
        break;
      case 13:
        Expect(f.type, CompactType::List);
        if (input.skip_encoding_stats() || input.skip_for_column())
          input.Skip(f.type);
        else if (input.encoding_stats_as_mask()) {
          const uint64_t size = input.ReadListSize(CompactType::Struct);
          for (uint64_t i = 0; i < size; ++i) {
            format::PageEncodingStats value;
            ReadPageEncodingStats(input, &value);
            const auto encoding = static_cast<uint32_t>(value.encoding);
            if (encoding < 64) out->encoding_stats_mask |= uint64_t{1} << encoding;
          }
          out->has_encoding_stats_mask = true;
        } else
          out->encoding_stats =
              ReadStructList<format::PageEncodingStats>(input, ReadPageEncodingStats);
        break;
      case 14:
        Expect(f.type, CompactType::I64);
        out->bloom_filter_offset = input.ReadI64();
        out->has_bloom_filter_offset = true;
        break;
      case 15:
        Expect(f.type, CompactType::I32);
        out->bloom_filter_length = input.ReadI32();
        out->has_bloom_filter_length = true;
        break;
      case 16:
        Expect(f.type, CompactType::Struct);
        if (input.skip_size_stats() || input.skip_for_column())
          input.Skip(f.type);
        else {
          ReadDirectSizeStatistics(input, &out->size_statistics);
          out->has_size_statistics = true;
        }
        break;
      case 17:
        Expect(f.type, CompactType::Struct);
        ReadGeospatialStatistics(input, &out->geospatial_statistics);
        out->has_geospatial_statistics = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!type || !encodings || !path || !codec || !values || !uncompressed || !compressed ||
      !data_offset) {
    throw ParquetException("Required direct ColumnMetaData field missing");
  }
}

void ReadDirectColumnChunk(CompactInput& input, DirectColumnChunk* out) {
  bool offset = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::Binary);
        out->file_path = input.ReadBinaryView();
        out->has_file_path = true;
        break;
      case 2:
        Expect(f.type, CompactType::I64);
        out->file_offset = input.ReadI64();
        out->has_file_offset = true;
        offset = true;
        break;
      case 3:
        Expect(f.type, CompactType::Struct);
        ReadDirectColumnMetadata(input, &out->metadata);
        out->has_metadata = true;
        break;
      case 4:
        Expect(f.type, CompactType::I64);
        out->offset_index_offset = input.ReadI64();
        out->has_offset_index_offset = true;
        break;
      case 5:
        Expect(f.type, CompactType::I32);
        out->offset_index_length = input.ReadI32();
        out->has_offset_index_length = true;
        break;
      case 6:
        Expect(f.type, CompactType::I64);
        out->column_index_offset = input.ReadI64();
        out->has_column_index_offset = true;
        break;
      case 7:
        Expect(f.type, CompactType::I32);
        out->column_index_length = input.ReadI32();
        out->has_column_index_length = true;
        break;
      case 8:
        Expect(f.type, CompactType::Struct);
        ReadColumnCryptoMetaData(input, &out->crypto_metadata);
        out->has_crypto_metadata = true;
        break;
      case 9:
        Expect(f.type, CompactType::Binary);
        out->encrypted_column_metadata = input.ReadBinaryView();
        out->has_encrypted_column_metadata = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!offset) throw ParquetException("Required direct ColumnChunk file_offset missing");
}

void ReadDirectRowGroup(CompactInput& input, DirectRowGroup* out) {
  bool columns = false, bytes = false, rows = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1: {
        Expect(f.type, CompactType::List);
        const uint64_t size = input.ReadListSize(CompactType::Struct);
        out->columns.clear();
        out->columns.reserve(static_cast<size_t>(size));
        for (uint64_t i = 0; i < size; ++i) {
          input.SetColumnIndex(static_cast<int32_t>(i));
          DirectColumnChunk value;
          ReadDirectColumnChunk(input, &value);
          out->columns.emplace_back(std::move(value));
        }
        input.SetColumnIndex(-1);
        columns = true;
        break;
      }
      case 2:
        Expect(f.type, CompactType::I64);
        out->total_byte_size = input.ReadI64();
        bytes = true;
        break;
      case 3:
        Expect(f.type, CompactType::I64);
        out->num_rows = input.ReadI64();
        rows = true;
        break;
      case 4:
        Expect(f.type, CompactType::List);
        out->sorting_columns =
            ReadStructList<format::SortingColumn>(input, ReadSortingColumn);
        out->has_sorting_columns = true;
        break;
      case 5:
        Expect(f.type, CompactType::I64);
        out->file_offset = input.ReadI64();
        out->has_file_offset = true;
        break;
      case 6:
        Expect(f.type, CompactType::I64);
        out->total_compressed_size = input.ReadI64();
        out->has_total_compressed_size = true;
        break;
      case 7:
        Expect(f.type, CompactType::I16);
        out->ordinal = input.ReadI16();
        out->has_ordinal = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!columns || !bytes || !rows)
    throw ParquetException("Required direct RowGroup field missing");
}

}  // namespace

void ReadDirectFileMetaData(CompactInput& input, DirectFileMetaData* out) {
  bool version = false, schema = false, rows = false, groups = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->version = input.ReadI32();
        version = true;
        break;
      case 2:
        Expect(f.type, CompactType::List);
        out->schema =
            input.ReadList<DirectSchemaElement>(CompactType::Struct, [&](CompactType) {
              DirectSchemaElement value;
              ReadDirectSchemaElement(input, &value);
              return value;
            });
        schema = true;
        break;
      case 3:
        Expect(f.type, CompactType::I64);
        out->num_rows = input.ReadI64();
        rows = true;
        break;
      case 4:
        Expect(f.type, CompactType::List);
        out->row_groups =
            input.ReadList<DirectRowGroup>(CompactType::Struct, [&](CompactType) {
              DirectRowGroup value;
              ReadDirectRowGroup(input, &value);
              return value;
            });
        groups = true;
        break;
      case 5:
        Expect(f.type, CompactType::List);
        out->key_value_metadata =
            ReadStructList<DirectKeyValue>(input, ReadDirectKeyValue);
        out->has_key_value_metadata = true;
        break;
      case 6:
        Expect(f.type, CompactType::Binary);
        out->created_by = input.ReadBinaryView();
        out->has_created_by = true;
        break;
      case 7:
        Expect(f.type, CompactType::List);
        out->column_orders = ReadStructList<format::ColumnOrder>(input, ReadColumnOrder);
        out->has_column_orders = true;
        break;
      case 8:
        Expect(f.type, CompactType::Struct);
        ReadEncryptionAlgorithm(input, &out->encryption_algorithm);
        out->has_encryption_algorithm = true;
        break;
      case 9:
        Expect(f.type, CompactType::Binary);
        out->footer_signing_key_metadata = input.ReadBinaryView();
        out->has_footer_signing_key_metadata = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!version || !schema || !rows || !groups)
    throw ParquetException("Required direct FileMetaData field missing");
}

void ReadFastFileMetaData(CompactInput& input, FastMetadataSummary* out) {
  bool version = false, schema = false, rows = false, groups = false;
  input.ReadStruct([&](const CompactField& f) {
    switch (f.id) {
      case 1:
        Expect(f.type, CompactType::I32);
        out->version = input.ReadI32();
        version = true;
        break;
      case 2: {
        Expect(f.type, CompactType::List);
        const auto size = input.ReadListSize(CompactType::Struct);
        out->num_schema_elements = static_cast<int32_t>(size);
        for (uint64_t i = 0; i < size; ++i) {
          bool has_type = false;
          input.ReadStruct([&](const CompactField& schema_field) {
            if (schema_field.id == 1) {
              Expect(schema_field.type, CompactType::I32);
              ReadEnum<format::Type::type>(input);
              has_type = true;
            } else {
              input.Skip(schema_field.type);
            }
          });
          if (has_type) ++out->num_columns;
        }
        schema = true;
        break;
      }
      case 3:
        Expect(f.type, CompactType::I64);
        out->num_rows = input.ReadI64();
        rows = true;
        break;
      case 4: {
        Expect(f.type, CompactType::List);
        const auto size = input.ReadListSize(CompactType::Struct);
        out->num_row_groups = static_cast<int32_t>(size);
        for (uint64_t i = 0; i < size; ++i) input.Skip(CompactType::Struct);
        groups = true;
        break;
      }
      case 6:
        Expect(f.type, CompactType::Binary);
        out->created_by = input.ReadBinaryView();
        out->has_created_by = true;
        break;
      default:
        input.Skip(f.type);
    }
  });
  if (!version || !schema || !rows || !groups)
    throw ParquetException("Required fast FileMetaData field missing");
}

}  // namespace internal
}  // namespace parquet
