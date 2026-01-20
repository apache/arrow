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
          class Int < ::FlatBuffers::Table
            FIELDS = {
              bit_width: ::FlatBuffers::Field.new(:bit_width, 0, 4, :int, 0),
              signed?: ::FlatBuffers::Field.new(:signed?, 1, 6, :bool, 0),
            }

            Data = define_data_class

            def bit_width
              field_offset = @view.unpack_virtual_offset(4)
              return 0 if field_offset.zero?

              @view.unpack_int(field_offset)
            end

            def signed?
              field_offset = @view.unpack_virtual_offset(6)
              return false if field_offset.zero?

              @view.unpack_bool(field_offset)
            end
          end
        end
      end
    end
  end
end
