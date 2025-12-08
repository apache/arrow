# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/key_value"
require_relative "../../../apache/arrow/flatbuf/dictionary_encoding"
require_relative "../../../apache/arrow/flatbuf/type"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # ----------------------------------------------------------------------
          # A field represents a named column in a record / row batch or child of a
          # nested type.
          class Field < ::FlatBuffers::Table
            # children apply only to nested data types like Struct, List and Union. For
            # primitive types children will have length 0.
            def children
              field_offset = @view.unpack_virtual_offset(14)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Field, element_offset)
              end
            end

            # User-defined metadata
            def custom_metadata
              field_offset = @view.unpack_virtual_offset(16)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::KeyValue, element_offset)
              end
            end

            # Present only if the field is dictionary encoded.
            def dictionary
              field_offset = @view.unpack_virtual_offset(12)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::DictionaryEncoding, field_offset)
            end

            # Name is not required (e.g., in a List)
            def name
              field_offset = @view.unpack_virtual_offset(4)
              return nil if field_offset.zero?

              @view.unpack_string(field_offset)
            end

            # Whether or not this field can contain nulls. Should be true in general.
            def nullable?
              field_offset = @view.unpack_virtual_offset(6)
              return false if field_offset.zero?

              @view.unpack_bool(field_offset)
            end

            # This is the type of the decoded value if the field is dictionary encoded.
            def type
              type = type_type
              return nil if type.nil?

              field_offset = @view.unpack_virtual_offset(10)
              return nil if field_offset.zero?
              @view.unpack_union(type.table_class, field_offset)
            end

            def type_type
              field_offset = @view.unpack_virtual_offset(8)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_utype(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::Type.try_convert(enum_value) || enum_value
            end
          end
        end
      end
    end
  end
end
