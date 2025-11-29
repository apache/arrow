# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # Logically the same as Binary, but the internal representation uses a view
          # struct that contains the string length and either the string's entire data
          # inline (for small strings) or an inlined prefix, an index of another buffer,
          # and an offset pointing to a slice in that buffer (for non-small strings).
          #
          # Since it uses a variable number of data buffers, each Field with this type
          # must have a corresponding entry in `variadicBufferCounts`.
          class BinaryView < ::FlatBuffers::Table
          end
        end
      end
    end
  end
end
