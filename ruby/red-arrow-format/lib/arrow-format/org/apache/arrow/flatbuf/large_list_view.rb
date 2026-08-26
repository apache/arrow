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
          # Same as ListView, but with 64-bit offsets and sizes, allowing to represent
          # extremely large data values.
          class LargeListView < ::FlatBuffers::Table
            FIELDS = {
            }

            Data = define_data_class
          end
        end
      end
    end
  end
end
