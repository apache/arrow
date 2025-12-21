# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Message.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/record_batch"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # For sending dictionary encoding information. Any Field can be
          # dictionary-encoded, but in this case none of its children may be
          # dictionary-encoded.
          # There is one vector / column per dictionary, but that vector / column
          # may be spread across multiple dictionary batches by using the isDelta
          # flag
          class DictionaryBatch < ::FlatBuffers::Table
            def data
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::RecordBatch, field_offset)
            end

            def id
              field_offset = @view.unpack_virtual_offset(4)
              return 0 if field_offset.zero?

              @view.unpack_long(field_offset)
            end

            # If isDelta is true the values in the dictionary are to be appended to a
            # dictionary with the indicated id. If isDelta is false this dictionary
            # should replace the existing dictionary.
            def delta?
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
