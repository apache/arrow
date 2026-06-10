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
          # Dictionary encoding metadata
          # Maintained for forwards compatibility, in the future
          # Dictionaries might be explicit maps between integers and values
          # allowing for non-contiguous index values
          class DictionaryKind < ::FlatBuffers::Enum
            DENSE_ARRAY = register("DenseArray", 0)
          end
        end
      end
    end
  end
end
