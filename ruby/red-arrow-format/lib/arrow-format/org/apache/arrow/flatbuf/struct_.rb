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
          # A Struct_ in the flatbuffer metadata is the same as an Arrow Struct
          # (according to the physical memory layout). We used Struct_ here as
          # Struct is a reserved word in Flatbuffers
          class Struct < ::FlatBuffers::Table
          end
        end
      end
    end
  end
end
