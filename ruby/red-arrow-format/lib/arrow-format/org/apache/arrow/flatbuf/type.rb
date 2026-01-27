# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # ----------------------------------------------------------------------
          # Top-level Type value, enabling extensible type-specific metadata. We can
          # add new logical types to Type without breaking backwards compatibility
          class Type < ::FlatBuffers::Union
            NONE = register("NONE", 0, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Utf8View", "../../../apache/arrow/flatbuf/utf8view")
            NULL = register("Null", 1, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Null", "../../../apache/arrow/flatbuf/null")
            INT = register("Int", 2, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int", "../../../apache/arrow/flatbuf/int")
            FLOATING_POINT = register("FloatingPoint", 3, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::FloatingPoint", "../../../apache/arrow/flatbuf/floating_point")
            BINARY = register("Binary", 4, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Binary", "../../../apache/arrow/flatbuf/binary")
            UTF8 = register("Utf8", 5, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Utf8", "../../../apache/arrow/flatbuf/utf8")
            BOOL = register("Bool", 6, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Bool", "../../../apache/arrow/flatbuf/bool")
            DECIMAL = register("Decimal", 7, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Decimal", "../../../apache/arrow/flatbuf/decimal")
            DATE = register("Date", 8, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Date", "../../../apache/arrow/flatbuf/date")
            TIME = register("Time", 9, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Time", "../../../apache/arrow/flatbuf/time")
            TIMESTAMP = register("Timestamp", 10, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Timestamp", "../../../apache/arrow/flatbuf/timestamp")
            INTERVAL = register("Interval", 11, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Interval", "../../../apache/arrow/flatbuf/interval")
            LIST = register("List", 12, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::List", "../../../apache/arrow/flatbuf/list")
            STRUCT_ = register("Struct_", 13, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Struct", "../../../apache/arrow/flatbuf/struct_")
            UNION = register("Union", 14, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Union", "../../../apache/arrow/flatbuf/union")
            FIXED_SIZE_BINARY = register("FixedSizeBinary", 15, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::FixedSizeBinary", "../../../apache/arrow/flatbuf/fixed_size_binary")
            FIXED_SIZE_LIST = register("FixedSizeList", 16, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::FixedSizeList", "../../../apache/arrow/flatbuf/fixed_size_list")
            MAP = register("Map", 17, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Map", "../../../apache/arrow/flatbuf/map")
            DURATION = register("Duration", 18, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Duration", "../../../apache/arrow/flatbuf/duration")
            LARGE_BINARY = register("LargeBinary", 19, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::LargeBinary", "../../../apache/arrow/flatbuf/large_binary")
            LARGE_UTF8 = register("LargeUtf8", 20, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::LargeUtf8", "../../../apache/arrow/flatbuf/large_utf8")
            LARGE_LIST = register("LargeList", 21, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::LargeList", "../../../apache/arrow/flatbuf/large_list")
            RUN_END_ENCODED = register("RunEndEncoded", 22, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::RunEndEncoded", "../../../apache/arrow/flatbuf/run_end_encoded")
            BINARY_VIEW = register("BinaryView", 23, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::BinaryView", "../../../apache/arrow/flatbuf/binary_view")
            UTF8VIEW = register("Utf8View", 24, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Utf8View", "../../../apache/arrow/flatbuf/utf8view")
            LIST_VIEW = register("ListView", 25, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::ListView", "../../../apache/arrow/flatbuf/list_view")
            LARGE_LIST_VIEW = register("LargeListView", 26, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::LargeListView", "../../../apache/arrow/flatbuf/large_list_view")
            

            private def require_table_class
              require_relative @require_path
            end
          end
        end
      end
    end
  end
end
