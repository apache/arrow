# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/precision"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class FloatingPoint < ::FlatBuffers::Table
            def precision
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_short(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::Precision.try_convert(enum_value) || enum_value
            end
          end
        end
      end
    end
  end
end
