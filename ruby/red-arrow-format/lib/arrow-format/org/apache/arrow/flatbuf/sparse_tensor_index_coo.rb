# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //SparseTensor.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/buffer"
require_relative "../../../apache/arrow/flatbuf/int"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # ----------------------------------------------------------------------
          # EXPERIMENTAL: Data structures for sparse tensors
          # Coordinate (COO) format of sparse tensor index.
          #
          # COO's index list is represented as an NxM matrix,
          # where N is the number of non-zero values,
          # and M is the number of dimensions of a sparse tensor.
          #
          # indicesBuffer stores the location and size of the data of these indices
          # matrix.  The value type and the stride of the indices matrix is
          # specified in indicesType and indicesStrides fields.
          #
          # For example, let X be a 2x3x4x5 tensor, and it has the following
          # 6 non-zero values:
          # ```text
          #   X[0, 1, 2, 0] := 1
          #   X[1, 1, 2, 3] := 2
          #   X[0, 2, 1, 0] := 3
          #   X[0, 1, 3, 0] := 4
          #   X[0, 1, 2, 1] := 5
          #   X[1, 2, 0, 4] := 6
          # ```
          # In COO format, the index matrix of X is the following 4x6 matrix:
          # ```text
          #   [[0, 0, 0, 0, 1, 1],
          #    [1, 1, 1, 2, 1, 2],
          #    [2, 2, 3, 1, 2, 0],
          #    [0, 1, 0, 0, 3, 4]]
          # ```
          # When isCanonical is true, the indices are sorted in lexicographical order
          # (row-major order), and it does not have duplicated entries.  Otherwise,
          # the indices may not be sorted, or may have duplicated entries.
          class SparseTensorIndexCOO < ::FlatBuffers::Table
            FIELDS = {
              indices_type: ::FlatBuffers::Field.new(:indices_type, 0, 4, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int", 0),
              indices_strides: ::FlatBuffers::Field.new(:indices_strides, 1, 6, [:long], 0),
              indices_buffer: ::FlatBuffers::Field.new(:indices_buffer, 2, 8, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer", 0),
              canonical?: ::FlatBuffers::Field.new(:canonical?, 3, 10, :bool, 0),
            }

            Data = define_data_class

            # The location and size of the indices matrix's data
            def indices_buffer
              field_offset = @view.unpack_virtual_offset(8)
              return nil if field_offset.zero?

              @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer, field_offset)
            end

            # Non-negative byte offsets to advance one value cell along each dimension
            # If omitted, default to row-major order (C-like).
            def indices_strides
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              element_size = 8
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_long(element_offset)
              end
            end

            # The type of values in indicesBuffer
            def indices_type
              field_offset = @view.unpack_virtual_offset(4)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int, field_offset)
            end

            # This flag is true if and only if the indices matrix is sorted in
            # row-major order, and does not have duplicated entries.
            # This sort order is the same as of Tensorflow's SparseTensor,
            # but it is inverse order of SciPy's canonical coo_matrix
            # (SciPy employs column-major order for its coo_matrix).
            def canonical?
              field_offset = @view.unpack_virtual_offset(10)
              return false if field_offset.zero?

              @view.unpack_bool(field_offset)
            end
          end
        end
      end
    end
  end
end
