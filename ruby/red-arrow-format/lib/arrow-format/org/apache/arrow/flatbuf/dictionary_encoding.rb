# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/dictionary_kind"
require_relative "../../../apache/arrow/flatbuf/int"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class DictionaryEncoding < ::FlatBuffers::Table
            FIELDS = {
              id: ::FlatBuffers::Field.new(:id, 0, 4, :long, 0),
              index_type: ::FlatBuffers::Field.new(:index_type, 1, 6, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int", 0),
              ordered?: ::FlatBuffers::Field.new(:ordered?, 2, 8, :bool, 0),
              dictionary_kind: ::FlatBuffers::Field.new(:dictionary_kind, 3, 10, :short, 0),
            }

            Data = define_data_class

            def dictionary_kind
              field_offset = @view.unpack_virtual_offset(10)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_short(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::DictionaryKind.try_convert(enum_value) || enum_value
            end

            # The known dictionary id in the application where this data is used. In
            # the file or streaming formats, the dictionary ids are found in the
            # DictionaryBatch messages
            def id
              field_offset = @view.unpack_virtual_offset(4)
              return 0 if field_offset.zero?

              @view.unpack_long(field_offset)
            end

            # The dictionary indices are constrained to be non-negative integers. If
            # this field is null, the indices must be signed int32. To maximize
            # cross-language compatibility and performance, implementations are
            # recommended to prefer signed integer types over unsigned integer types
            # and to avoid uint64 indices unless they are required by an application.
            def index_type
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int, field_offset)
            end

            # By default, dictionaries are not ordered, or the order does not have
            # semantic meaning. In some statistical applications, dictionary-encoding
            # is used to represent ordered categorical data, and we provide a way to
            # preserve that metadata here
            def ordered?
              field_offset = @view.unpack_virtual_offset(8)
              return false if field_offset.zero?

              @view.unpack_bool(field_offset)
            end
          end
        end
      end
    end
  end
end
