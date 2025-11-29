# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Message.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/key_value"
require_relative "../../../apache/arrow/flatbuf/message_header"
require_relative "../../../apache/arrow/flatbuf/metadata_version"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class Message < ::FlatBuffers::Table
            def body_length
              field_offset = @view.unpack_virtual_offset(10)
              return 0 if field_offset.zero?

              @view.unpack_long(field_offset)
            end

            def custom_metadata
              field_offset = @view.unpack_virtual_offset(12)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::KeyValue, element_offset)
              end
            end

            def header
              type = header_type
              return nil if type.nil?

              field_offset = @view.unpack_virtual_offset(8)
              return nil if field_offset.zero?
              @view.unpack_union(type.table_class, field_offset)
            end

            def header_type
              field_offset = @view.unpack_virtual_offset(6)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_utype(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::MessageHeader.try_convert(enum_value) || enum_value
            end

            def version
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_short(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::MetadataVersion.try_convert(enum_value) || enum_value
            end
          end
        end
      end
    end
  end
end
