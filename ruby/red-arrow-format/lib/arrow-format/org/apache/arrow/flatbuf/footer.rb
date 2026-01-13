# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //File.fbs
# Rooting type:            org.apache.arrow.flatbuf.Footer (//File.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/key_value"
require_relative "../../../apache/arrow/flatbuf/block"
require_relative "../../../apache/arrow/flatbuf/schema"
require_relative "../../../apache/arrow/flatbuf/metadata_version"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # ----------------------------------------------------------------------
          # Arrow File metadata
          #
          class Footer < ::FlatBuffers::RootTable
            class << self
              def file_identifier
                ""
              end

              def file_extension
                ""
              end
            end

            FIELDS = {
              version: ::FlatBuffers::Field.new(:version, 0, 4, :short, 0),
              schema: ::FlatBuffers::Field.new(:schema, 1, 6, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Schema", 0),
              dictionaries: ::FlatBuffers::Field.new(:dictionaries, 2, 8, ["::ArrowFormat::Org::Apache::Arrow::Flatbuf::Block"], 0),
              record_batches: ::FlatBuffers::Field.new(:record_batches, 3, 10, ["::ArrowFormat::Org::Apache::Arrow::Flatbuf::Block"], 0),
              custom_metadata: ::FlatBuffers::Field.new(:custom_metadata, 4, 12, ["::ArrowFormat::Org::Apache::Arrow::Flatbuf::KeyValue"], 0),
            }

            Data = define_data_class

            # User-defined metadata
            def custom_metadata
              field_offset = @view.unpack_virtual_offset(12)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::KeyValue, element_offset)
              end
            end

            def dictionaries
              field_offset = @view.unpack_virtual_offset(8)
              return nil if field_offset.zero?

              element_size = 24
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Block, element_offset)
              end
            end

            def record_batches
              field_offset = @view.unpack_virtual_offset(10)
              return nil if field_offset.zero?

              element_size = 24
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Block, element_offset)
              end
            end

            def schema
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Schema, field_offset)
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
