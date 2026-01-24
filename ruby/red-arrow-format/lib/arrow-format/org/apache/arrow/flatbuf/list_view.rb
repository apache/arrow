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
          # Represents the same logical types that List can, but contains offsets and
          # sizes allowing for writes in any order and sharing of child values among
          # list values.
          class ListView < ::FlatBuffers::Table
            FIELDS = {
            }

            Data = define_data_class
          end
        end
      end
    end
  end
end
