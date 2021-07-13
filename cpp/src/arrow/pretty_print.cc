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

#include "arrow/pretty_print.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/string.h"
#include "arrow/vendored/datetime.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

class PrettyPrinter {
 public:
  PrettyPrinter(const PrettyPrintOptions& options, std::ostream* sink)
      : options_(options), indent_(options.indent), sink_(sink) {}

  void Write(const char* data);
  void Write(const std::string& data);
  void WriteIndented(const char* data);
  void WriteIndented(const std::string& data);
  void Newline();
  void Indent();
  void OpenArray(const Array& array);
  void CloseArray(const Array& array);

  void Flush() { (*sink_) << std::flush; }

 protected:
  const PrettyPrintOptions& options_;
  int indent_;
  std::ostream* sink_;
};

void PrettyPrinter::OpenArray(const Array& array) {
  if (!options_.skip_new_lines) {
    Indent();
  }
  (*sink_) << "[";
  if (array.length() > 0) {
    Newline();
    indent_ += options_.indent_size;
  }
}

void PrettyPrinter::CloseArray(const Array& array) {
  if (array.length() > 0) {
    indent_ -= options_.indent_size;
    Indent();
  }
  (*sink_) << "]";
}

void PrettyPrinter::Write(const char* data) { (*sink_) << data; }
void PrettyPrinter::Write(const std::string& data) { (*sink_) << data; }

void PrettyPrinter::WriteIndented(const char* data) {
  Indent();
  Write(data);
}

void PrettyPrinter::WriteIndented(const std::string& data) {
  Indent();
  Write(data);
}

void PrettyPrinter::Newline() {
  if (options_.skip_new_lines) {
    return;
  }
  (*sink_) << "\n";
}

void PrettyPrinter::Indent() {
  for (int i = 0; i < indent_; ++i) {
    (*sink_) << " ";
  }
}

class ArrayPrinter : public PrettyPrinter {
 public:
  ArrayPrinter(const PrettyPrintOptions& options, std::ostream* sink)
      : PrettyPrinter(options, sink) {}

  template <typename FormatFunction>
  void WriteValues(const Array& array, FormatFunction&& func) {
    bool skip_comma = true;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (skip_comma) {
        skip_comma = false;
      } else {
        (*sink_) << ",";
        Newline();
      }
      if (!options_.skip_new_lines) {
        Indent();
      }
      if ((i >= options_.window) && (i < (array.length() - options_.window))) {
        (*sink_) << "...";
        Newline();
        i = array.length() - options_.window - 1;
        skip_comma = true;
      } else if (array.IsNull(i)) {
        (*sink_) << options_.null_rep;
      } else {
        func(i);
      }
    }
    Newline();
  }

  Status WriteDataValues(const BooleanArray& array) {
    WriteValues(array, [&](int64_t i) { Write(array.Value(i) ? "true" : "false"); });
    return Status::OK();
  }

  template <typename T>
  enable_if_integer<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    const auto data = array.raw_values();
    // Need to upcast integers to avoid selecting operator<<(char)
    WriteValues(array, [&](int64_t i) { (*sink_) << internal::UpcastInt(data[i]); });
    return Status::OK();
  }

  template <typename T>
  enable_if_floating_point<typename T::TypeClass, Status> WriteDataValues(
      const T& array) {
    const auto data = array.raw_values();
    WriteValues(array, [&](int64_t i) { (*sink_) << data[i]; });
    return Status::OK();
  }

  template <typename T>
  enable_if_date<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    const auto data = array.raw_values();
    using unit = typename std::conditional<std::is_same<T, Date32Array>::value,
                                           arrow_vendored::date::days,
                                           std::chrono::milliseconds>::type;
    WriteValues(array, [&](int64_t i) { FormatDateTime<unit>("%F", data[i], true); });
    return Status::OK();
  }

  template <typename T>
  enable_if_time<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    const auto data = array.raw_values();
    const auto type = static_cast<const TimeType*>(array.type().get());
    WriteValues(array,
                [&](int64_t i) { FormatDateTime(type->unit(), "%T", data[i], false); });
    return Status::OK();
  }

  Status WriteDataValues(const TimestampArray& array) {
    const int64_t* data = array.raw_values();
    const auto type = static_cast<const TimestampType*>(array.type().get());
    WriteValues(array,
                [&](int64_t i) { FormatDateTime(type->unit(), "%F %T", data[i], true); });
    return Status::OK();
  }

  template <typename T>
  enable_if_duration<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    const auto data = array.raw_values();
    WriteValues(array, [&](int64_t i) { (*sink_) << data[i]; });
    return Status::OK();
  }

  Status WriteDataValues(const DayTimeIntervalArray& array) {
    WriteValues(array, [&](int64_t i) {
      auto day_millis = array.GetValue(i);
      (*sink_) << day_millis.days << "d" << day_millis.milliseconds << "ms";
    });
    return Status::OK();
  }

  Status WriteDataValues(const MonthIntervalArray& array) {
    const auto data = array.raw_values();
    WriteValues(array, [&](int64_t i) { (*sink_) << data[i]; });
    return Status::OK();
  }

  template <typename T>
  enable_if_string_like<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << "\"" << array.GetView(i) << "\""; });
    return Status::OK();
  }

  // Binary
  template <typename T>
  enable_if_binary_like<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << HexEncode(array.GetView(i)); });
    return Status::OK();
  }

  Status WriteDataValues(const Decimal128Array& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << array.FormatValue(i); });
    return Status::OK();
  }

  Status WriteDataValues(const Decimal256Array& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << array.FormatValue(i); });
    return Status::OK();
  }

  template <typename T>
  enable_if_list_like<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    bool skip_comma = true;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (skip_comma) {
        skip_comma = false;
      } else {
        (*sink_) << ",";
        Newline();
      }
      if ((i >= options_.window) && (i < (array.length() - options_.window))) {
        Indent();
        (*sink_) << "...";
        Newline();
        i = array.length() - options_.window - 1;
        skip_comma = true;
      } else if (array.IsNull(i)) {
        Indent();
        (*sink_) << options_.null_rep;
      } else {
        std::shared_ptr<Array> slice =
            array.values()->Slice(array.value_offset(i), array.value_length(i));
        RETURN_NOT_OK(
            PrettyPrint(*slice, PrettyPrintOptions{indent_, options_.window}, sink_));
      }
    }
    Newline();
    return Status::OK();
  }

  Status WriteDataValues(const MapArray& array) {
    bool skip_comma = true;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (skip_comma) {
        skip_comma = false;
      } else {
        (*sink_) << ",";
        Newline();
      }

      if (!options_.skip_new_lines) {
        Indent();
      }

      if ((i >= options_.window) && (i < (array.length() - options_.window))) {
        (*sink_) << "...";
        Newline();
        i = array.length() - options_.window - 1;
        skip_comma = true;
      } else if (array.IsNull(i)) {
        (*sink_) << options_.null_rep;
      } else {
        (*sink_) << "keys:";
        Newline();
        auto keys_slice =
            array.keys()->Slice(array.value_offset(i), array.value_length(i));
        RETURN_NOT_OK(PrettyPrint(*keys_slice,
                                  PrettyPrintOptions{indent_, options_.window}, sink_));
        Newline();
        Indent();
        (*sink_) << "values:";
        Newline();
        auto values_slice =
            array.items()->Slice(array.value_offset(i), array.value_length(i));
        RETURN_NOT_OK(PrettyPrint(*values_slice,
                                  PrettyPrintOptions{indent_, options_.window}, sink_));
      }
    }
    (*sink_) << "\n";
    return Status::OK();
  }

  Status Visit(const NullArray& array) {
    (*sink_) << array.length() << " nulls";
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_base_of<PrimitiveArray, T>::value ||
                  std::is_base_of<FixedSizeBinaryArray, T>::value ||
                  std::is_base_of<BinaryArray, T>::value ||
                  std::is_base_of<LargeBinaryArray, T>::value ||
                  std::is_base_of<ListArray, T>::value ||
                  std::is_base_of<LargeListArray, T>::value ||
                  std::is_base_of<MapArray, T>::value ||
                  std::is_base_of<FixedSizeListArray, T>::value,
              Status>
  Visit(const T& array) {
    OpenArray(array);
    if (array.length() > 0) {
      RETURN_NOT_OK(WriteDataValues(array));
    }
    CloseArray(array);
    return Status::OK();
  }

  Status Visit(const ExtensionArray& array) { return Print(*array.storage()); }

  Status WriteValidityBitmap(const Array& array);

  Status PrintChildren(const std::vector<std::shared_ptr<Array>>& fields, int64_t offset,
                       int64_t length) {
    for (size_t i = 0; i < fields.size(); ++i) {
      Newline();
      Indent();
      std::stringstream ss;
      ss << "-- child " << i << " type: " << fields[i]->type()->ToString() << "\n";
      Write(ss.str());

      std::shared_ptr<Array> field = fields[i];
      if (offset != 0) {
        field = field->Slice(offset, length);
      }
      RETURN_NOT_OK(PrettyPrint(*field, indent_ + options_.indent_size, sink_));
    }
    return Status::OK();
  }

  Status Visit(const StructArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i));
    }
    return PrintChildren(children, 0, array.length());
  }

  Status Visit(const UnionArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));

    Newline();
    Indent();
    Write("-- type_ids: ");
    UInt8Array type_codes(array.length(), array.type_codes(), nullptr, 0, array.offset());
    RETURN_NOT_OK(PrettyPrint(type_codes, indent_ + options_.indent_size, sink_));

    if (array.mode() == UnionMode::DENSE) {
      Newline();
      Indent();
      Write("-- value_offsets: ");
      Int32Array value_offsets(
          array.length(), checked_cast<const DenseUnionArray&>(array).value_offsets(),
          nullptr, 0, array.offset());
      RETURN_NOT_OK(PrettyPrint(value_offsets, indent_ + options_.indent_size, sink_));
    }

    // Print the children without any offset, because the type ids are absolute
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i));
    }
    return PrintChildren(children, 0, array.length() + array.offset());
  }

  Status Visit(const DictionaryArray& array) {
    Newline();
    Indent();
    Write("-- dictionary:\n");
    RETURN_NOT_OK(
        PrettyPrint(*array.dictionary(), indent_ + options_.indent_size, sink_));

    Newline();
    Indent();
    Write("-- indices:\n");
    return PrettyPrint(*array.indices(), indent_ + options_.indent_size, sink_);
  }

  Status Print(const Array& array) {
    RETURN_NOT_OK(VisitArrayInline(array, this));
    Flush();
    return Status::OK();
  }

 private:
  template <typename Unit>
  void FormatDateTime(const char* fmt, int64_t value, bool add_epoch) {
    if (add_epoch) {
      (*sink_) << arrow_vendored::date::format(fmt, epoch_ + Unit{value});
    } else {
      (*sink_) << arrow_vendored::date::format(fmt, Unit{value});
    }
  }

  void FormatDateTime(TimeUnit::type unit, const char* fmt, int64_t value,
                      bool add_epoch) {
    switch (unit) {
      case TimeUnit::NANO:
        FormatDateTime<std::chrono::nanoseconds>(fmt, value, add_epoch);
        break;
      case TimeUnit::MICRO:
        FormatDateTime<std::chrono::microseconds>(fmt, value, add_epoch);
        break;
      case TimeUnit::MILLI:
        FormatDateTime<std::chrono::milliseconds>(fmt, value, add_epoch);
        break;
      case TimeUnit::SECOND:
        FormatDateTime<std::chrono::seconds>(fmt, value, add_epoch);
        break;
    }
  }

  static arrow_vendored::date::sys_days epoch_;
};

arrow_vendored::date::sys_days ArrayPrinter::epoch_ =
    arrow_vendored::date::sys_days{arrow_vendored::date::jan / 1 / 1970};

Status ArrayPrinter::WriteValidityBitmap(const Array& array) {
  Indent();
  Write("-- is_valid:");

  if (array.null_count() > 0) {
    Newline();
    Indent();
    BooleanArray is_valid(array.length(), array.null_bitmap(), nullptr, 0,
                          array.offset());
    return PrettyPrint(is_valid, indent_ + options_.indent_size, sink_);
  } else {
    Write(" all not null");
    return Status::OK();
  }
}

Status PrettyPrint(const Array& arr, int indent, std::ostream* sink) {
  PrettyPrintOptions options;
  options.indent = indent;
  ArrayPrinter printer(options, sink);
  return printer.Print(arr);
}

Status PrettyPrint(const Array& arr, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  ArrayPrinter printer(options, sink);
  return printer.Print(arr);
}

Status PrettyPrint(const Array& arr, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(arr, options, &sink));
  *result = sink.str();
  return Status::OK();
}

Status PrettyPrint(const ChunkedArray& chunked_arr, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  int num_chunks = chunked_arr.num_chunks();
  int indent = options.indent;
  int window = options.window;

  for (int i = 0; i < indent; ++i) {
    (*sink) << " ";
  }
  (*sink) << "[";
  if (!options.skip_new_lines) {
    *sink << "\n";
  }
  bool skip_comma = true;
  for (int i = 0; i < num_chunks; ++i) {
    if (skip_comma) {
      skip_comma = false;
    } else {
      (*sink) << ",";
      if (!options.skip_new_lines) {
        *sink << "\n";
      }
    }
    if ((i >= window) && (i < (num_chunks - window))) {
      for (int i = 0; i < indent; ++i) {
        (*sink) << " ";
      }
      (*sink) << "...";
      if (!options.skip_new_lines) {
        *sink << "\n";
      }
      i = num_chunks - window - 1;
      skip_comma = true;
    } else {
      PrettyPrintOptions chunk_options = options;
      chunk_options.indent += options.indent_size;
      ArrayPrinter printer(chunk_options, sink);
      RETURN_NOT_OK(printer.Print(*chunked_arr.chunk(i)));
    }
  }
  if (!options.skip_new_lines) {
    *sink << "\n";
  }

  for (int i = 0; i < indent; ++i) {
    (*sink) << " ";
  }
  (*sink) << "]";

  return Status::OK();
}

Status PrettyPrint(const ChunkedArray& chunked_arr, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(chunked_arr, options, &sink));
  *result = sink.str();
  return Status::OK();
}

Status PrettyPrint(const RecordBatch& batch, int indent, std::ostream* sink) {
  for (int i = 0; i < batch.num_columns(); ++i) {
    const std::string& name = batch.column_name(i);
    (*sink) << name << ": ";
    RETURN_NOT_OK(PrettyPrint(*batch.column(i), indent + 2, sink));
    (*sink) << "\n";
  }
  (*sink) << std::flush;
  return Status::OK();
}

Status PrettyPrint(const RecordBatch& batch, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  for (int i = 0; i < batch.num_columns(); ++i) {
    const std::string& name = batch.column_name(i);
    PrettyPrintOptions column_options = options;
    column_options.indent += 2;

    (*sink) << name << ": ";
    RETURN_NOT_OK(PrettyPrint(*batch.column(i), column_options, sink));
    (*sink) << "\n";
  }
  (*sink) << std::flush;
  return Status::OK();
}

Status PrettyPrint(const Table& table, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  RETURN_NOT_OK(PrettyPrint(*table.schema(), options, sink));
  (*sink) << "\n";
  (*sink) << "----\n";

  PrettyPrintOptions column_options = options;
  column_options.indent += 2;
  for (int i = 0; i < table.num_columns(); ++i) {
    for (int j = 0; j < options.indent; ++j) {
      (*sink) << " ";
    }
    (*sink) << table.schema()->field(i)->name() << ":\n";
    RETURN_NOT_OK(PrettyPrint(*table.column(i), column_options, sink));
    (*sink) << "\n";
  }
  (*sink) << std::flush;
  return Status::OK();
}

Status DebugPrint(const Array& arr, int indent) {
  return PrettyPrint(arr, indent, &std::cerr);
}

class SchemaPrinter : public PrettyPrinter {
 public:
  SchemaPrinter(const Schema& schema, const PrettyPrintOptions& options,
                std::ostream* sink)
      : PrettyPrinter(options, sink), schema_(schema) {}

  Status PrintType(const DataType& type, bool nullable);
  Status PrintField(const Field& field);

  void PrintVerboseMetadata(const KeyValueMetadata& metadata) {
    for (int64_t i = 0; i < metadata.size(); ++i) {
      Newline();
      Indent();
      Write(metadata.key(i) + ": '" + metadata.value(i) + "'");
    }
  }

  void PrintTruncatedMetadata(const KeyValueMetadata& metadata) {
    for (int64_t i = 0; i < metadata.size(); ++i) {
      Newline();
      Indent();
      size_t size = metadata.value(i).size();
      size_t truncated_size = std::max<size_t>(10, 70 - metadata.key(i).size() - indent_);
      if (size <= truncated_size) {
        Write(metadata.key(i) + ": '" + metadata.value(i) + "'");
        continue;
      }

      Write(metadata.key(i) + ": '" + metadata.value(i).substr(0, truncated_size) +
            "' + " + std::to_string(size - truncated_size));
    }
  }

  void PrintMetadata(const std::string& metadata_type, const KeyValueMetadata& metadata) {
    if (metadata.size() > 0) {
      Newline();
      Indent();
      Write(metadata_type);
      if (options_.truncate_metadata) {
        PrintTruncatedMetadata(metadata);
      } else {
        PrintVerboseMetadata(metadata);
      }
    }
  }

  Status Print() {
    for (int i = 0; i < schema_.num_fields(); ++i) {
      if (i > 0) {
        Newline();
        Indent();
      } else {
        Indent();
      }
      RETURN_NOT_OK(PrintField(*schema_.field(i)));
    }

    if (options_.show_schema_metadata && schema_.metadata() != nullptr) {
      PrintMetadata("-- schema metadata --", *schema_.metadata());
    }
    Flush();
    return Status::OK();
  }

 private:
  const Schema& schema_;
};

Status SchemaPrinter::PrintType(const DataType& type, bool nullable) {
  Write(type.ToString());
  if (!nullable) {
    Write(" not null");
  }
  for (int i = 0; i < type.num_fields(); ++i) {
    Newline();
    Indent();

    std::stringstream ss;
    ss << "child " << i << ", ";

    indent_ += options_.indent_size;
    WriteIndented(ss.str());
    RETURN_NOT_OK(PrintField(*type.field(i)));
    indent_ -= options_.indent_size;
  }
  return Status::OK();
}

Status SchemaPrinter::PrintField(const Field& field) {
  Write(field.name());
  Write(": ");
  RETURN_NOT_OK(PrintType(*field.type(), field.nullable()));

  if (options_.show_field_metadata && field.metadata() != nullptr) {
    indent_ += options_.indent_size;
    PrintMetadata("-- field metadata --", *field.metadata());
    indent_ -= options_.indent_size;
  }
  return Status::OK();
}

Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  SchemaPrinter printer(schema, options, sink);
  return printer.Print();
}

Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(schema, options, &sink));
  *result = sink.str();
  return Status::OK();
}

}  // namespace arrow
