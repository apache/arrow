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
          class UnionMode < ::FlatBuffers::Enum
            SPARSE = register("Sparse", 0)
            DENSE = register("Dense", 1)
          end
        end
      end
    end
  end
end
