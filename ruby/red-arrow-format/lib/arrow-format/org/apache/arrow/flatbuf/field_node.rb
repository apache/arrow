# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Message.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # ----------------------------------------------------------------------
          # Data structures for describing a table row batch (a collection of
          # equal-length Arrow arrays)
          # Metadata about a field at some level of a nested type tree (but not
          # its children).
          #
          # For example, a List<Int16> with values `[[1, 2, 3], null, [4], [5, 6], null]`
          # would have {length: 5, null_count: 2} for its List node, and {length: 6,
          # null_count: 0} for its Int16 node, as separate FieldNode structs
          class FieldNode < ::FlatBuffers::Struct
            # The number of value slots in the Arrow array at this level of a nested
            # tree
            def length
              field_offset = 0
              @view.unpack_long(field_offset)
            end

            # The number of observed nulls. Fields with null_count == 0 may choose not
            # to write their physical validity bitmap out as a materialized buffer,
            # instead setting the length of the bitmap buffer to 0.
            def null_count
              field_offset = 8
              @view.unpack_long(field_offset)
            end
          end
        end
      end
    end
  end
end
