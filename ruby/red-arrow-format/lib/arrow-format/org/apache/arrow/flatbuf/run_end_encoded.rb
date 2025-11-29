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
          # Contains two child arrays, run_ends and values.
          # The run_ends child array must be a 16/32/64-bit integer array
          # which encodes the indices at which the run with the value in 
          # each corresponding index in the values child array ends.
          # Like list/struct types, the value array can be of any type.
          class RunEndEncoded < ::FlatBuffers::Table
          end
        end
      end
    end
  end
end
