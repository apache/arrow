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
          # Exact decimal value represented as an integer value in two's
          # complement. Currently 32-bit (4-byte), 64-bit (8-byte), 
          # 128-bit (16-byte) and 256-bit (32-byte) integers are used.
          # The representation uses the endianness indicated in the Schema.
          class Decimal < ::FlatBuffers::Table
            FIELDS = {
              precision: ::FlatBuffers::Field.new(:precision, 0, 4, :int, 0),
              scale: ::FlatBuffers::Field.new(:scale, 1, 6, :int, 0),
              bit_width: ::FlatBuffers::Field.new(:bit_width, 2, 8, :int, 0),
            }

            Data = define_data_class

            # Number of bits per value. The accepted widths are 32, 64, 128 and 256.
            # We use bitWidth for consistency with Int::bitWidth.
            def bit_width
              field_offset = @view.unpack_virtual_offset(8)
              return 128 if field_offset.zero?

              @view.unpack_int(field_offset)
            end

            # Total number of decimal digits
            def precision
              field_offset = @view.unpack_virtual_offset(4)
              return 0 if field_offset.zero?

              @view.unpack_int(field_offset)
            end

            # Number of digits after the decimal point "."
            def scale
              field_offset = @view.unpack_virtual_offset(6)
              return 0 if field_offset.zero?

              @view.unpack_int(field_offset)
            end
          end
        end
      end
    end
  end
end
