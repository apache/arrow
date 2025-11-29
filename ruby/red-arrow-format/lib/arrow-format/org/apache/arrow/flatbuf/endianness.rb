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
          # ----------------------------------------------------------------------
          # Endianness of the platform producing the data
          class Endianness < ::FlatBuffers::Enum
            LITTLE = register("Little", 0)
            BIG = register("Big", 1)
          end
        end
      end
    end
  end
end
