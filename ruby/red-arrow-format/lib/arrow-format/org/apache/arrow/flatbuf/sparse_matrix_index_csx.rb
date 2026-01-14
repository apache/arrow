# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //SparseTensor.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/sparse_matrix_compressed_axis"
require_relative "../../../apache/arrow/flatbuf/buffer"
require_relative "../../../apache/arrow/flatbuf/int"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # Compressed Sparse format, that is matrix-specific.
          class SparseMatrixIndexCSX < ::FlatBuffers::Table
            FIELDS = {
              compressed_axis: ::FlatBuffers::Field.new(:compressed_axis, 0, 4, :short, 0),
              indptr_type: ::FlatBuffers::Field.new(:indptr_type, 1, 6, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int", 0),
              indptr_buffer: ::FlatBuffers::Field.new(:indptr_buffer, 2, 8, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer", 0),
              indices_type: ::FlatBuffers::Field.new(:indices_type, 3, 10, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int", 0),
              indices_buffer: ::FlatBuffers::Field.new(:indices_buffer, 4, 12, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer", 0),
            }

            Data = define_data_class

            # Which axis, row or column, is compressed
            def compressed_axis
              field_offset = @view.unpack_virtual_offset(4)
              if field_offset.zero?
                enum_value = 0
              else
                enum_value = @view.unpack_short(field_offset)
              end
              ::ArrowFormat::Org::Apache::Arrow::Flatbuf::SparseMatrixCompressedAxis.try_convert(enum_value) || enum_value
            end

            # indicesBuffer stores the location and size of the array that
            # contains the column indices of the corresponding non-zero values.
            # The type of index value is long.
            #
            # For example, the indices of the above X are:
            # ```text
            #   indices(X) = [1, 2, 2, 1, 3, 0, 2, 3, 1].
            # ```
            # Note that the indices are sorted in lexicographical order for each row.
            def indices_buffer
              field_offset = @view.unpack_virtual_offset(12)
              return nil if field_offset.zero?

              @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer, field_offset)
            end

            # The type of values in indicesBuffer
            def indices_type
              field_offset = @view.unpack_virtual_offset(10)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int, field_offset)
            end

            # indptrBuffer stores the location and size of indptr array that
            # represents the range of the rows.
            # The i-th row spans from `indptr[i]` to `indptr[i+1]` in the data.
            # The length of this array is 1 + (the number of rows), and the type
            # of index value is long.
            #
            # For example, let X be the following 6x4 matrix:
            # ```text
            #   X := [[0, 1, 2, 0],
            #         [0, 0, 3, 0],
            #         [0, 4, 0, 5],
            #         [0, 0, 0, 0],
            #         [6, 0, 7, 8],
            #         [0, 9, 0, 0]].
            # ```
            # The array of non-zero values in X is:
            # ```text
            #   values(X) = [1, 2, 3, 4, 5, 6, 7, 8, 9].
            # ```
            # And the indptr of X is:
            # ```text
            #   indptr(X) = [0, 2, 3, 5, 5, 8, 10].
            # ```
            def indptr_buffer
              field_offset = @view.unpack_virtual_offset(8)
              return nil if field_offset.zero?

              @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer, field_offset)
            end

            # The type of values in indptrBuffer
            def indptr_type
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int, field_offset)
            end
          end
        end
      end
    end
  end
end
