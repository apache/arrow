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
          # A Buffer represents a single contiguous memory segment
          class Buffer < ::FlatBuffers::Struct
            FIELDS = {
              offset: ::FlatBuffers::Field.new(:offset, 0, 0, :long, 0),
              length: ::FlatBuffers::Field.new(:length, 1, 8, :long, 0),
            }

            Data = define_data_class

            # The absolute length (in bytes) of the memory buffer. The memory is found
            # from offset (inclusive) to offset + length (non-inclusive). When building
            # messages using the encapsulated IPC message, padding bytes may be written
            # after a buffer, but such padding bytes do not need to be accounted for in
            # the size here.
            def length
              field_offset = 8
              @view.unpack_long(field_offset)
            end

            # The relative offset into the shared memory page where the bytes for this
            # buffer starts
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
