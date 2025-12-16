# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/union_mode"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # A union is a complex type with children in Field
          # By default ids in the type vector refer to the offsets in the children
          # optionally typeIds provides an indirection between the child offset and the type id
          # for each child `typeIds[offset]` is the id used in the type vector
          class Union < ::FlatBuffers::Table
            def mode
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_short(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::UnionMode.try_convert(enum_value) || enum_value
            end

            def type_ids
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_int(element_offset)
              end
            end
          end
        end
      end
    end
  end
end
