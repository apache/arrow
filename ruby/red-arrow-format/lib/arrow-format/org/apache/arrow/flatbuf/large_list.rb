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
          # Same as List, but with 64-bit offsets, allowing to represent
          # extremely large data values.
          class LargeList < ::FlatBuffers::Table
            FIELDS = {
            }

            Data = define_data_class
          end
        end
      end
    end
  end
end
