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
          class IntervalUnit < ::FlatBuffers::Enum
            YEAR_MONTH = register("YEAR_MONTH", 0)
            DAY_TIME = register("DAY_TIME", 1)
            MONTH_DAY_NANO = register("MONTH_DAY_NANO", 2)
          end
        end
      end
    end
  end
end
