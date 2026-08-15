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
          class FixedSizeList < ::FlatBuffers::Table
            FIELDS = {
              list_size: ::FlatBuffers::Field.new(:list_size, 0, 4, :int, 0),
            }

            Data = define_data_class

            # Number of list items per value
            def list_size
              field_offset = @view.unpack_virtual_offset(4)
              return 0 if field_offset.zero?

              @view.unpack_int(field_offset)
            end
          end
        end
      end
    end
  end
end
