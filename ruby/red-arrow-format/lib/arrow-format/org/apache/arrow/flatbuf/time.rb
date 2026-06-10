# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/time_unit"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # Time is either a 32-bit or 64-bit signed integer type representing an
          # elapsed time since midnight, stored in either of four units: seconds,
          # milliseconds, microseconds or nanoseconds.
          #
          # The integer `bitWidth` depends on the `unit` and must be one of the following:
          # * SECOND and MILLISECOND: 32 bits
          # * MICROSECOND and NANOSECOND: 64 bits
          #
          # The allowed values are between 0 (inclusive) and 86400 (=24*60*60) seconds
          # (exclusive), adjusted for the time unit (for example, up to 86400000
          # exclusive for the MILLISECOND unit).
          # This definition doesn't allow for leap seconds. Time values from
          # measurements with leap seconds will need to be corrected when ingesting
          # into Arrow (for example by replacing the value 86400 with 86399).
          class Time < ::FlatBuffers::Table
            FIELDS = {
              unit: ::FlatBuffers::Field.new(:unit, 0, 4, :short, 0),
              bit_width: ::FlatBuffers::Field.new(:bit_width, 1, 6, :int, 0),
            }

            Data = define_data_class

            def bit_width
              field_offset = @view.unpack_virtual_offset(6)
              return 32 if field_offset.zero?

              @view.unpack_int(field_offset)
            end

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
