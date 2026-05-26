# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Tensor.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # ----------------------------------------------------------------------
          # Data structures for dense tensors
          # Shape data for a single axis in a tensor
          class TensorDim < ::FlatBuffers::Table
            FIELDS = {
              size: ::FlatBuffers::Field.new(:size, 0, 4, :long, 0),
              name: ::FlatBuffers::Field.new(:name, 1, 6, :string, 0),
            }

            Data = define_data_class

            # Name of the dimension, optional
            def name
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              @view.unpack_string(field_offset)
            end

            # Length of dimension
            def size
              field_offset = @view.unpack_virtual_offset(4)
              return 0 if field_offset.zero?

              @view.unpack_long(field_offset)
            end
          end
        end
      end
    end
  end
end
