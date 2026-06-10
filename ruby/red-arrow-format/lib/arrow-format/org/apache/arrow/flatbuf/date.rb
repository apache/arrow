# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/date_unit"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # Date is either a 32-bit or 64-bit signed integer type representing an
          # elapsed time since UNIX epoch (1970-01-01), stored in either of two units:
          #
          # * Milliseconds (64 bits) indicating UNIX time elapsed since the epoch (no
          #   leap seconds), where the values are evenly divisible by 86400000
          # * Days (32 bits) since the UNIX epoch
          class Date < ::FlatBuffers::Table
            FIELDS = {
              unit: ::FlatBuffers::Field.new(:unit, 0, 4, :short, 0),
            }

            Data = define_data_class

            def unit
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 1
              else
                enum_value = @view.unpack_short(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::DateUnit.try_convert(enum_value) || enum_value
            end
          end
        end
      end
    end
  end
end
