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
          # user defined key value pairs to add custom metadata to arrow
          # key namespacing is the responsibility of the user
          class KeyValue < ::FlatBuffers::Table
            FIELDS = {
              key: ::FlatBuffers::Field.new(:key, 0, 4, :string, 0),
              value: ::FlatBuffers::Field.new(:value, 1, 6, :string, 0),
            }

            Data = define_data_class

            def key
              field_offset = @view.unpack_virtual_offset(4)
              return nil if field_offset.zero?

              @view.unpack_string(field_offset)
            end

            def value
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              @view.unpack_string(field_offset)
            end
          end
        end
      end
    end
  end
end
