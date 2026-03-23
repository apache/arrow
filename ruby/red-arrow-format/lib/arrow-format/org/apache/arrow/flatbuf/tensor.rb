# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Tensor.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/buffer"
require_relative "../../../apache/arrow/flatbuf/tensor_dim"
require_relative "../../../apache/arrow/flatbuf/type"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class Tensor < ::FlatBuffers::Table
            FIELDS = {
              type_type: ::FlatBuffers::Field.new(:type_type, 0, 4, :utype, 0),
              type: ::FlatBuffers::Field.new(:type, 1, 6, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Type", 0),
              shape: ::FlatBuffers::Field.new(:shape, 2, 8, ["::ArrowFormat::Org::Apache::Arrow::Flatbuf::TensorDim"], 0),
              strides: ::FlatBuffers::Field.new(:strides, 3, 10, [:long], 0),
              data: ::FlatBuffers::Field.new(:data, 4, 12, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer", 0),
            }

            Data = define_data_class

            # The location and size of the tensor's data
            def data
              field_offset = @view.unpack_virtual_offset(12)
              return nil if field_offset.zero?

              @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer, field_offset)
            end

            # The dimensions of the tensor, optionally named
            def shape
              field_offset = @view.unpack_virtual_offset(8)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::TensorDim, element_offset)
              end
            end

            # Non-negative byte offsets to advance one value cell along each dimension
            # If omitted, default to row-major order (C-like).
            def strides
              field_offset = @view.unpack_virtual_offset(10)
              return nil if field_offset.zero?

              element_size = 8
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_long(element_offset)
              end
            end

            # The type of data contained in a value cell. Currently only fixed-width
            # value types are supported, no strings or nested types
            def type
              type = type_type
              return nil if type.nil?

              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?
              @view.unpack_union(type.table_class, field_offset)
            end

            def type_type
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_utype(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::Type.try_convert(enum_value) || enum_value
            end
          end
        end
      end
    end
  end
end
