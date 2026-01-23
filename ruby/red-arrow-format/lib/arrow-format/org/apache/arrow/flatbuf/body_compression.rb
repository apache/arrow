# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Message.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/compression_type"
require_relative "../../../apache/arrow/flatbuf/body_compression_method"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # Optional compression for the memory buffers constituting IPC message
          # bodies. Intended for use with RecordBatch but could be used for other
          # message types
          class BodyCompression < ::FlatBuffers::Table
            FIELDS = {
              codec: ::FlatBuffers::Field.new(:codec, 0, 4, :byte, 0),
              method: ::FlatBuffers::Field.new(:method, 1, 6, :byte, 0),
            }

            Data = define_data_class

            # Compressor library.
            # For LZ4_FRAME, each compressed buffer must consist of a single frame.
            def codec
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_byte(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::CompressionType.try_convert(enum_value) || enum_value
            end

            # Indicates the way the record batch body was compressed
            def method
              field_offset = @view.unpack_virtual_offset(6)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_byte(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::BodyCompressionMethod.try_convert(enum_value) || enum_value
            end
          end
        end
      end
    end
  end
end
