# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Message.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"
require_relative "../../../apache/arrow/flatbuf/buffer"
require_relative "../../../apache/arrow/flatbuf/body_compression"
require_relative "../../../apache/arrow/flatbuf/field_node"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # A data header describing the shared memory layout of a "record" or "row"
          # batch. Some systems call this a "row batch" internally and others a "record
          # batch".
          class RecordBatch < ::FlatBuffers::Table
            FIELDS = {
              length: ::FlatBuffers::Field.new(:length, 0, 4, :long, 0),
              nodes: ::FlatBuffers::Field.new(:nodes, 1, 6, ["::ArrowFormat::Org::Apache::Arrow::Flatbuf::FieldNode"], 0),
              buffers: ::FlatBuffers::Field.new(:buffers, 2, 8, ["::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer"], 0),
              compression: ::FlatBuffers::Field.new(:compression, 3, 10, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::BodyCompression", 0),
              variadic_buffer_counts: ::FlatBuffers::Field.new(:variadic_buffer_counts, 4, 12, [:long], 0),
            }

            Data = define_data_class

            # Buffers correspond to the pre-ordered flattened buffer tree
            #
            # The number of buffers appended to this list depends on the schema. For
            # example, most primitive arrays will have 2 buffers, 1 for the validity
            # bitmap and 1 for the values. For struct arrays, there will only be a
            # single buffer for the validity (nulls) bitmap
            def buffers
              field_offset = @view.unpack_virtual_offset(8)
              return nil if field_offset.zero?

              element_size = 16
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::Buffer, element_offset)
              end
            end

            # Optional compression of the message body
            def compression
              field_offset = @view.unpack_virtual_offset(10)
              return nil if field_offset.zero?

              @view.unpack_table(::ArrowFormat::Org::Apache::Arrow::Flatbuf::BodyCompression, field_offset)
            end

            # number of records / rows. The arrays in the batch should all have this
            # length
            def length
              field_offset = @view.unpack_virtual_offset(4)
              return 0 if field_offset.zero?

              @view.unpack_long(field_offset)
            end

            # Nodes correspond to the pre-ordered flattened logical schema
            def nodes
              field_offset = @view.unpack_virtual_offset(6)
              return nil if field_offset.zero?

              element_size = 16
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_struct(::ArrowFormat::Org::Apache::Arrow::Flatbuf::FieldNode, element_offset)
              end
            end

            # Some types such as Utf8View are represented using a variable number of buffers.
            # For each such Field in the pre-ordered flattened logical schema, there will be
            # an entry in variadicBufferCounts to indicate the number of variadic
            # buffers which belong to that Field in the current RecordBatch.
            #
            # For example, the schema
            #     col1: Struct<alpha: Int32, beta: BinaryView, gamma: Float64>
            #     col2: Utf8View
            # contains two Fields with variadic buffers so variadicBufferCounts will have
            # two entries, the first counting the variadic buffers of `col1.beta` and the
            # second counting `col2`'s.
            #
            # This field may be omitted if and only if the schema contains no Fields with
            # a variable number of buffers, such as BinaryView and Utf8View.
            def variadic_buffer_counts
              field_offset = @view.unpack_virtual_offset(12)
              return nil if field_offset.zero?

              element_size = 8
              @view.unpack_vector(field_offset, element_size) do |element_offset|
                @view.unpack_long(element_offset)
              end
            end
          end
        end
      end
    end
  end
end
