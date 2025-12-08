# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/time_unit"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class Duration < ::FlatBuffers::Table
            def unit
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 1
              else
                enum_value = @view.unpack_short(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::TimeUnit.try_convert(enum_value) || enum_value
            end
          end
        end
      end
    end
  end
end
