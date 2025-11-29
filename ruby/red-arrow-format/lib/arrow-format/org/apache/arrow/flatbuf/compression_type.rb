# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Message.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class CompressionType < ::FlatBuffers::Enum
            LZ4_FRAME = register("LZ4_FRAME", 0)
            ZSTD = register("ZSTD", 1)
          end
        end
      end
    end
  end
end
