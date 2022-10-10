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
#include <limits>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <string_view>
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
#include "arrow/util/formatting.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/rle_util.h"
#include "arrow/util/string.h"
#include "arrow/vendored/datetime.h"
#include "arrow/visit_array_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::StringFormatter;

namespace {

class PrettyPrinter {
 public:
  PrettyPrinter(const PrettyPrintOptions& options, std::ostream* sink)
      : options_(options), indent_(options.indent), sink_(sink) {}

  inline void Write(std::string_view data);
  inline void WriteIndented(std::string_view data);
  inline void Newline();
  inline void Indent();
  inline void IndentAfterNewline();
  void OpenArray(const Array& array);
  void CloseArray(const Array& array);
  void Flush() { (*sink_) << std::flush; }

  PrettyPrintOptions ChildOptions(bool increment_indent = false) const {
    PrettyPrintOptions child_options = options_;
    if (increment_indent) {
      child_options.indent = indent_ + child_options.indent_size;
    } else {
      child_options.indent = indent_;
    }
    return child_options;
  }

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
    if (!options_.skip_new_lines) {
      Indent();
    }
  }
  (*sink_) << "]";
}

void PrettyPrinter::Write(std::string_view data) { (*sink_) << data; }

void PrettyPrinter::WriteIndented(std::string_view data) {
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

void PrettyPrinter::IndentAfterNewline() {
  if (options_.skip_new_lines) {
    return;
  }
  Indent();
}

class ArrayPrinter : public PrettyPrinter {
 public:
  ArrayPrinter(const PrettyPrintOptions& options, std::ostream* sink)
      : PrettyPrinter(options, sink) {}

 private:
  template <typename FormatFunction>
  Status WriteValues(const Array& array, FormatFunction&& func,
                     bool indent_non_null_values = true, bool is_container = false) {
    // `indent_non_null_values` should be false if `FormatFunction` applies
    // indentation itself.
    int window = is_container ? options_.container_window : options_.window;
    for (int64_t i = 0; i < array.length(); ++i) {
      const bool is_last = (i == array.length() - 1);
      if ((i >= window) && (i < (array.length() - window))) {
        IndentAfterNewline();
        (*sink_) << "...";
        if (!is_last && options_.skip_new_lines) {
          (*sink_) << ",";
        }
        i = array.length() - window - 1;
      } else if (array.IsNull(i)) {
        IndentAfterNewline();
        (*sink_) << options_.null_rep;
        if (!is_last) {
          (*sink_) << ",";
        }
      } else {
        if (indent_non_null_values) {
          IndentAfterNewline();
        }
        RETURN_NOT_OK(func(i));
        if (!is_last) {
          (*sink_) << ",";
        }
      }
      Newline();
    }
    return Status::OK();
  }

  template <typename ArrayType, typename Formatter>
  Status WritePrimitiveValues(const ArrayType& array, Formatter* formatter) {
    auto appender = [&](std::string_view v) { (*sink_) << v; };
    auto format_func = [&](int64_t i) {
      (*formatter)(array.GetView(i), appender);
      return Status::OK();
    };
    return WriteValues(array, std::move(format_func));
  }

  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  Status WritePrimitiveValues(const ArrayType& array) {
    StringFormatter<T> formatter{array.type().get()};
    return WritePrimitiveValues(array, &formatter);
  }

  Status WriteValidityBitmap(const Array& array);

  Status PrintChildren(const std::vector<const Array*>& fields, int64_t offset,
                       int64_t length) {
    for (size_t i = 0; i < fields.size(); ++i) {
      Write("\n");  // Always want newline before child array description
      Indent();
      std::stringstream ss;
      ss << "-- child " << i << " type: " << fields[i]->type()->ToString() << "\n";
      Write(ss.str());

      // Indent();
      const Array* field = fields[i];
      if (offset != 0) {
        RETURN_NOT_OK(
            PrettyPrint(*field->Slice(offset, length), ChildOptions(true), sink_));
      } else {
        RETURN_NOT_OK(PrettyPrint(*field, ChildOptions(true), sink_));
      }
    }
    return Status::OK();
  }

  //
  // WriteDataValues(): generic function to write values from an array
  //

  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  enable_if_has_c_type<T, Status> WriteDataValues(const ArrayType& array) {
    return WritePrimitiveValues(array);
  }

  Status WriteDataValues(const HalfFloatArray& array) {
    // XXX do not know how to format half floats yet
    StringFormatter<Int16Type> formatter{array.type().get()};
    return WritePrimitiveValues(array, &formatter);
  }

  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  enable_if_string_like<T, Status> WriteDataValues(const ArrayType& array) {
    return WriteValues(array, [&](int64_t i) {
      (*sink_) << "\"" << array.GetView(i) << "\"";
      return Status::OK();
    });
  }

  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  enable_if_t<is_binary_like_type<T>::value && !is_decimal_type<T>::value, Status>
  WriteDataValues(const ArrayType& array) {
    return WriteValues(array, [&](int64_t i) {
      (*sink_) << HexEncode(array.GetView(i));
      return Status::OK();
    });
  }

  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  enable_if_decimal<T, Status> WriteDataValues(const ArrayType& array) {
    return WriteValues(array, [&](int64_t i) {
      (*sink_) << array.FormatValue(i);
      return Status::OK();
    });
  }

  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  enable_if_list_like<T, Status> WriteDataValues(const ArrayType& array) {
    const auto values = array.values();
    const auto child_options = ChildOptions();
    ArrayPrinter values_printer(child_options, sink_);

    return WriteValues(
        array,
        [&](int64_t i) {
          // XXX this could be much faster if ArrayPrinter allowed specifying start and
          // stop endpoints.
          return values_printer.Print(
              *values->Slice(array.value_offset(i), array.value_length(i)));
        },
        /*indent_non_null_values=*/false,
        /*is_container=*/true);
  }

  Status WriteDataValues(const MapArray& array) {
    const auto keys = array.keys();
    const auto items = array.items();
    const auto child_options = ChildOptions();
    ArrayPrinter values_printer(child_options, sink_);

    return WriteValues(
        array,
        [&](int64_t i) {
          IndentAfterNewline();
          (*sink_) << "keys:";
          Newline();
          RETURN_NOT_OK(values_printer.Print(
              *keys->Slice(array.value_offset(i), array.value_length(i))));
          Newline();
          IndentAfterNewline();
          (*sink_) << "values:";
          Newline();
          RETURN_NOT_OK(values_printer.Print(
              *items->Slice(array.value_offset(i), array.value_length(i))));
          return Status::OK();
        },
        /*indent_non_null_values=*/false);
  }

 public:
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
    Status st = array.Validate();
    if (!st.ok()) {
      (*sink_) << "<Invalid array: " << st.message() << ">";
      return Status::OK();
    }

    OpenArray(array);
    if (array.length() > 0) {
      RETURN_NOT_OK(WriteDataValues(array));
    }
    CloseArray(array);
    return Status::OK();
  }

  Status Visit(const NullArray& array) {
    (*sink_) << array.length() << " nulls";
    return Status::OK();
  }

  Status Visit(const ExtensionArray& array) { return Print(*array.storage()); }

  Status Visit(const StructArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));
    std::vector<const Array*> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i).get());
    }
    return PrintChildren(children, 0, array.length());
  }

  Status Visit(const UnionArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));

    Newline();
    Indent();
    Write("-- type_ids: ");
    UInt8Array type_codes(array.length(), array.type_codes(), nullptr, 0, array.offset());
    RETURN_NOT_OK(PrettyPrint(type_codes, ChildOptions(true), sink_));

    if (array.mode() == UnionMode::DENSE) {
      Newline();
      Indent();
      Write("-- value_offsets: ");
      Int32Array value_offsets(
          array.length(), checked_cast<const DenseUnionArray&>(array).value_offsets(),
          nullptr, 0, array.offset());
      RETURN_NOT_OK(PrettyPrint(value_offsets, ChildOptions(true), sink_));
    }

    // Print the children without any offset, because the type ids are absolute
    std::vector<const Array*> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i).get());
    }
    return PrintChildren(children, 0, array.length() + array.offset());
  }

  Status Visit(const DictionaryArray& array) {
    Newline();
    Indent();
    Write("-- dictionary:\n");
    RETURN_NOT_OK(PrettyPrint(*array.dictionary(), ChildOptions(true), sink_));

    Newline();
    Indent();
    Write("-- indices:\n");
    return PrettyPrint(*array.indices(), ChildOptions(true), sink_);
  }

  Status Visit(const RunLengthEncodedArray& array) {
    Newline();
    Indent();
    Write("-- run ends array (offset: ");
    Write(std::to_string(array.offset()));
    Write(", logical length: ");
    Write(std::to_string(array.length()));
    Write(")\n");
    RETURN_NOT_OK(PrettyPrint(*array.run_ends_array(), ChildOptions(true), sink_));

    Newline();
    Indent();
    Write("-- values:\n");
    return PrettyPrint(*array.values_array(), ChildOptions(true), sink_);
  }

  Status Print(const Array& array) {
    RETURN_NOT_OK(VisitArrayInline(array, this));
    Flush();
    return Status::OK();
  }
};

Status ArrayPrinter::WriteValidityBitmap(const Array& array) {
  Indent();
  Write("-- is_valid:");

  if (array.null_count() > 0) {
    Newline();
    Indent();
    BooleanArray is_valid(array.length(), array.null_bitmap(), nullptr, 0,
                          array.offset());
    return PrettyPrint(is_valid, ChildOptions(true), sink_);
  } else {
    Write(" all not null");
    return Status::OK();
  }
}

}  // namespace

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
  int window = options.container_window;
  // Struct fields are always on new line
  bool skip_new_lines =
      options.skip_new_lines && (chunked_arr.type()->id() != Type::STRUCT);

  for (int i = 0; i < indent; ++i) {
    (*sink) << " ";
  }
  (*sink) << "[";
  if (!skip_new_lines) {
    *sink << "\n";
  }
  bool skip_comma = true;
  for (int i = 0; i < num_chunks; ++i) {
    if (skip_comma) {
      skip_comma = false;
    } else {
      (*sink) << ",";
      if (!skip_new_lines) {
        *sink << "\n";
      }
    }
    if ((i >= window) && (i < (num_chunks - window))) {
      for (int i = 0; i < indent; ++i) {
        (*sink) << " ";
      }
      (*sink) << "...,";
      if (!skip_new_lines) {
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

namespace {

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

}  // namespace

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
