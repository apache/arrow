# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //File.fbs
# Rooting type:            org.apache.arrow.flatbuf.Footer (//File.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class Block < ::FlatBuffers::Struct
            FIELDS = {
              offset: ::FlatBuffers::Field.new(:offset, 0, 0, :long, 0),
              meta_data_length: ::FlatBuffers::Field.new(:meta_data_length, 1, 8, :int, 4),
              body_length: ::FlatBuffers::Field.new(:body_length, 2, 16, :long, 0),
            }

            Data = define_data_class

            # Length of the data (this is aligned so there can be a gap between this and
            # the metadata).
            def body_length
              field_offset = 16
              @view.unpack_long(field_offset)
            end

            # Length of the metadata
            def meta_data_length
              field_offset = 8
              @view.unpack_int(field_offset)
            end

            # Index to the start of the RecordBatch (note this is past the Message header)
            def offset
              field_offset = 0
              @view.unpack_long(field_offset)
            end
          end
        end
      end
    end
  end
end
