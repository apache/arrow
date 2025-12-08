# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/key_value"
require_relative "../../../apache/arrow/flatbuf/endianness"
require_relative "../../../apache/arrow/flatbuf/field"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # ----------------------------------------------------------------------
          # A Schema describes the columns in a row batch
          class Schema < ::FlatBuffers::Table
            def custom_metadata
              field_offset = @view.unpack_virtual_offset(8)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::KeyValue, element_offset)
              end
            end

            # endianness of the buffer
            # it is Little Endian by default
            # if endianness doesn't match the underlying system then the vectors need to be converted
            def endianness
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_short(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::Endianness.try_convert(enum_value) || enum_value
            end

            # Features used in the stream/file.
            def features
              field_offset = @view.unpack_virtual_offset(10)
              return nil if field_offset.zero?

              element_size = 8
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_long(element_offset)
              end
            end

            def fields
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Field, element_offset)
              end
            end
          end
        end
      end
    end
  end
end
