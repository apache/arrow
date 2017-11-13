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

#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class PrettyPrinter {
 public:
  PrettyPrinter(int indent, std::ostream* sink) : indent_(indent), sink_(sink) {}

  void Write(const char* data);
  void Write(const std::string& data);
  void WriteIndented(const char* data);
  void WriteIndented(const std::string& data);
  void Newline();
  void Indent();
  void OpenArray();
  void CloseArray();

  void Flush() { (*sink_) << std::flush; }

 protected:
  int indent_;
  std::ostream* sink_;
};

void PrettyPrinter::OpenArray() { (*sink_) << "["; }

void PrettyPrinter::CloseArray() { (*sink_) << "]"; }

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
  (*sink_) << "\n";
  Indent();
}

void PrettyPrinter::Indent() {
  for (int i = 0; i < indent_; ++i) {
    (*sink_) << " ";
  }
}

class ArrayPrinter : public PrettyPrinter {
 public:
  ArrayPrinter(const Array& array, int indent, std::ostream* sink)
      : PrettyPrinter(indent, sink), array_(array) {}

  template <typename T>
  inline typename std::enable_if<IsInteger<T>::value, void>::type WriteDataValues(
      const T& array) {
    const auto data = array.raw_values();
    for (int i = 0; i < array.length(); ++i) {
      if (i > 0) {
        (*sink_) << ", ";
      }
      if (array.IsNull(i)) {
        (*sink_) << "null";
      } else {
        (*sink_) << static_cast<int64_t>(data[i]);
      }
    }
  }

  template <typename T>
  inline typename std::enable_if<IsFloatingPoint<T>::value, void>::type WriteDataValues(
      const T& array) {
    const auto data = array.raw_values();
    for (int i = 0; i < array.length(); ++i) {
      if (i > 0) {
        (*sink_) << ", ";
      }
      if (array.IsNull(i)) {
        Write("null");
      } else {
        (*sink_) << data[i];
      }
    }
  }

  // String (Utf8)
  template <typename T>
  inline typename std::enable_if<std::is_same<StringArray, T>::value, void>::type
  WriteDataValues(const T& array) {
    int32_t length;
    for (int i = 0; i < array.length(); ++i) {
      if (i > 0) {
        (*sink_) << ", ";
      }
      if (array.IsNull(i)) {
        Write("null");
      } else {
        const char* buf = reinterpret_cast<const char*>(array.GetValue(i, &length));
        (*sink_) << "\"" << std::string(buf, length) << "\"";
      }
    }
  }

  // Binary
  template <typename T>
  inline typename std::enable_if<std::is_same<BinaryArray, T>::value, void>::type
  WriteDataValues(const T& array) {
    int32_t length;
    for (int i = 0; i < array.length(); ++i) {
      if (i > 0) {
        (*sink_) << ", ";
      }
      if (array.IsNull(i)) {
        Write("null");
      } else {
        const uint8_t* buf = array.GetValue(i, &length);
        (*sink_) << HexEncode(buf, length);
      }
    }
  }

  template <typename T>
  inline typename std::enable_if<std::is_same<FixedSizeBinaryArray, T>::value, void>::type
  WriteDataValues(const T& array) {
    int32_t width = array.byte_width();
    for (int i = 0; i < array.length(); ++i) {
      if (i > 0) {
        (*sink_) << ", ";
      }
      if (array.IsNull(i)) {
        Write("null");
      } else {
        (*sink_) << HexEncode(array.GetValue(i), width);
      }
    }
  }

  template <typename T>
  inline typename std::enable_if<std::is_same<Decimal128Array, T>::value, void>::type
  WriteDataValues(const T& array) {
    for (int i = 0; i < array.length(); ++i) {
      if (i > 0) {
        (*sink_) << ", ";
      }
      if (array.IsNull(i)) {
        Write("null");
      } else {
        (*sink_) << array.FormatValue(i);
      }
    }
  }

  template <typename T>
  inline typename std::enable_if<std::is_base_of<BooleanArray, T>::value, void>::type
  WriteDataValues(const T& array) {
    for (int i = 0; i < array.length(); ++i) {
      if (i > 0) {
        (*sink_) << ", ";
      }
      if (array.IsNull(i)) {
        Write("null");
      } else {
        Write(array.Value(i) ? "true" : "false");
      }
    }
  }

  Status Visit(const NullArray& array) {
    (*sink_) << array.length() << " nulls";
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<PrimitiveArray, T>::value ||
                              std::is_base_of<FixedSizeBinaryArray, T>::value ||
                              std::is_base_of<BinaryArray, T>::value,
                          Status>::type
  Visit(const T& array) {
    OpenArray();
    WriteDataValues(array);
    CloseArray();
    return Status::OK();
  }

  Status Visit(const IntervalArray&) { return Status::NotImplemented("interval"); }

  Status WriteValidityBitmap(const Array& array);

  Status Visit(const ListArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));

    Newline();
    Write("-- value_offsets: ");
    Int32Array value_offsets(array.length() + 1, array.value_offsets(), nullptr, 0,
                             array.offset());
    RETURN_NOT_OK(PrettyPrint(value_offsets, indent_ + 2, sink_));

    Newline();
    Write("-- values: ");
    auto values =
        array.values()->Slice(array.value_offset(0), array.value_offset(array.length()));
    RETURN_NOT_OK(PrettyPrint(*values, indent_ + 2, sink_));

    return Status::OK();
  }

  Status PrintChildren(const std::vector<std::shared_ptr<Array>>& fields, int64_t offset,
                       int64_t length) {
    for (size_t i = 0; i < fields.size(); ++i) {
      Newline();
      std::stringstream ss;
      ss << "-- child " << i << " type: " << fields[i]->type()->ToString() << " values: ";
      Write(ss.str());

      std::shared_ptr<Array> field = fields[i];
      if (offset != 0) {
        field = field->Slice(offset, length);
      }

      RETURN_NOT_OK(PrettyPrint(*field, indent_ + 2, sink_));
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
    return PrintChildren(children, array.offset(), array.length());
  }

  Status Visit(const UnionArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));

    Newline();
    Write("-- type_ids: ");
    UInt8Array type_ids(array.length(), array.type_ids(), nullptr, 0, array.offset());
    RETURN_NOT_OK(PrettyPrint(type_ids, indent_ + 2, sink_));

    if (array.mode() == UnionMode::DENSE) {
      Newline();
      Write("-- value_offsets: ");
      Int32Array value_offsets(array.length(), array.value_offsets(), nullptr, 0,
                               array.offset());
      RETURN_NOT_OK(PrettyPrint(value_offsets, indent_ + 2, sink_));
    }

    // Print the children without any offset, because the type ids are absolute
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.child(i));
    }
    return PrintChildren(children, 0, array.length() + array.offset());
  }

  Status Visit(const DictionaryArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));

    Newline();
    Write("-- dictionary: ");
    RETURN_NOT_OK(PrettyPrint(*array.dictionary(), indent_ + 2, sink_));

    Newline();
    Write("-- indices: ");
    return PrettyPrint(*array.indices(), indent_ + 2, sink_);
  }

  Status Print() {
    RETURN_NOT_OK(VisitArrayInline(array_, this));
    Flush();
    return Status::OK();
  }

 private:
  const Array& array_;
};

Status ArrayPrinter::WriteValidityBitmap(const Array& array) {
  Newline();
  Write("-- is_valid: ");

  if (array.null_count() > 0) {
    BooleanArray is_valid(array.length(), array.null_bitmap(), nullptr, 0,
                          array.offset());
    return PrettyPrint(is_valid, indent_ + 2, sink_);
  } else {
    Write("all not null");
    return Status::OK();
  }
}

Status PrettyPrint(const Array& arr, int indent, std::ostream* sink) {
  ArrayPrinter printer(arr, indent, sink);
  return printer.Print();
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

Status DebugPrint(const Array& arr, int indent) {
  return PrettyPrint(arr, indent, &std::cout);
}

class SchemaPrinter : public PrettyPrinter {
 public:
  SchemaPrinter(const Schema& schema, int indent, std::ostream* sink)
      : PrettyPrinter(indent, sink), schema_(schema) {}

  Status PrintType(const DataType& type);
  Status PrintField(const Field& field);

  Status Print() {
    for (int i = 0; i < schema_.num_fields(); ++i) {
      if (i > 0) {
        Newline();
      }
      RETURN_NOT_OK(PrintField(*schema_.field(i)));
    }
    Flush();
    return Status::OK();
  }

 private:
  const Schema& schema_;
};

Status SchemaPrinter::PrintType(const DataType& type) {
  Write(type.ToString());
  if (type.id() == Type::DICTIONARY) {
    Newline();

    indent_ += 2;
    WriteIndented("dictionary: ");
    const auto& dict_type = static_cast<const DictionaryType&>(type);
    RETURN_NOT_OK(PrettyPrint(*dict_type.dictionary(), indent_, sink_));
    indent_ -= 2;
  } else {
    for (int i = 0; i < type.num_children(); ++i) {
      Newline();

      std::stringstream ss;
      ss << "child " << i << ", ";

      indent_ += 2;
      WriteIndented(ss.str());
      RETURN_NOT_OK(PrintField(*type.child(i)));
      indent_ -= 2;
    }
  }
  return Status::OK();
}

Status SchemaPrinter::PrintField(const Field& field) {
  Write(field.name());
  Write(": ");
  return PrintType(*field.type());
}

Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  SchemaPrinter printer(schema, options.indent, sink);
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
