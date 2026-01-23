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
          # Compressed Sparse Fiber (CSF) sparse tensor index.
          class SparseTensorIndexCSF < ::FlatBuffers::Table
            FIELDS = {
              indptr_type: ::FlatBuffers::Field.new(:indptr_type, 0, 4, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int", 0),
              indptr_buffers: ::FlatBuffers::Field.new(:indptr_buffers, 1, 6, ["::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer"], 0),
              indices_type: ::FlatBuffers::Field.new(:indices_type, 2, 8, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int", 0),
              indices_buffers: ::FlatBuffers::Field.new(:indices_buffers, 3, 10, ["::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer"], 0),
              axis_order: ::FlatBuffers::Field.new(:axis_order, 4, 12, [:int], 0),
            }

            Data = define_data_class

            # axisOrder stores the sequence in which dimensions were traversed to
            # produce the prefix tree.
            # For example, the axisOrder for the above X is:
            # ```text
            #   axisOrder(X) = [0, 1, 2, 3].
            # ```
            def axis_order
              field_offset = @view.unpack_virtual_offset(12)
              return nil if field_offset.zero?

              element_size = 4
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_int(element_offset)
              end
            end

            # indicesBuffers stores values of nodes.
            # Each tensor dimension corresponds to a buffer in indicesBuffers.
            # For example, the indicesBuffers for the above X are:
            # ```text
            #   indicesBuffer(X) = [
            #                        [0, 1],
            #                        [0, 1, 1],
            #                        [0, 0, 1, 1],
            #                        [1, 2, 0, 2, 0, 0, 1, 2]
            #                      ].
            # ```
            def indices_buffers
              field_offset = @view.unpack_virtual_offset(10)
              return nil if field_offset.zero?

              element_size = 16
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer, element_offset)
              end
            end

            # The type of values in indicesBuffers
            def indices_type
              field_offset = @view.unpack_virtual_offset(8)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int, field_offset)
            end

            # indptrBuffers stores the sparsity structure.
            # Each two consecutive dimensions in a tensor correspond to a buffer in
            # indptrBuffers. A pair of consecutive values at `indptrBuffers[dim][i]`
            # and `indptrBuffers[dim][i + 1]` signify a range of nodes in
            # `indicesBuffers[dim + 1]` who are children of `indicesBuffers[dim][i]` node.
            #
            # For example, the indptrBuffers for the above X are:
            # ```text
            #   indptrBuffer(X) = [
            #                       [0, 2, 3],
            #                       [0, 1, 3, 4],
            #                       [0, 2, 4, 5, 8]
            #                     ].
            # ```
            def indptr_buffers
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              element_size = 16
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer, element_offset)
              end
            end

            # CSF is a generalization of compressed sparse row (CSR) index.
            # See [smith2017knl](http://shaden.io/pub-files/smith2017knl.pdf)
            #
            # CSF index recursively compresses each dimension of a tensor into a set
            # of prefix trees. Each path from a root to leaf forms one tensor
            # non-zero index. CSF is implemented with two arrays of buffers and one
            # array of integers.
            #
            # For example, let X be a 2x3x4x5 tensor and let it have the following
            # 8 non-zero values:
            # ```text
            #   X[0, 0, 0, 1] := 1
            #   X[0, 0, 0, 2] := 2
            #   X[0, 1, 0, 0] := 3
            #   X[0, 1, 0, 2] := 4
            #   X[0, 1, 1, 0] := 5
            #   X[1, 1, 1, 0] := 6
            #   X[1, 1, 1, 1] := 7
            #   X[1, 1, 1, 2] := 8
            # ```
            # As a prefix tree this would be represented as:
            # ```text
            #         0          1
            #        / \         |
            #       0   1        1
            #      /   / \       |
            #     0   0   1      1
            #    /|  /|   |    /| |
            #   1 2 0 2   0   0 1 2
            # ```
            # The type of values in indptrBuffers
            def indptr_type
              field_offset = @view.unpack_virtual_offset(4)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Int, field_offset)
            end
          end
        end
      end
    end
  end
end
