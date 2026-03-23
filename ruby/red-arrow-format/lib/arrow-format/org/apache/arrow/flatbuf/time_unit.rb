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
          class TimeUnit < ::FlatBuffers::Enum
            SECOND = register("SECOND", 0)
            MILLISECOND = register("MILLISECOND", 1)
            MICROSECOND = register("MICROSECOND", 2)
            NANOSECOND = register("NANOSECOND", 3)
          end
        end
      end
    end
  end
end
