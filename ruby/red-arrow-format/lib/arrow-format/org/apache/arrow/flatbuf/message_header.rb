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
          # The root Message type
          # This union enables us to easily send different message types without
          # redundant storage, and in the future we can easily add new message types.
          #
          # Arrow implementations do not need to implement all of the message types,
          # which may include experimental metadata types. For maximum compatibility,
          # it is best to send data using RecordBatch
          class MessageHeader < ::FlatBuffers::Union
            NONE = register("NONE", 0, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Utf8View", "../../../apache/arrow/flatbuf/utf8view")
            SCHEMA = register("Schema", 1, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Schema", "../../../apache/arrow/flatbuf/schema")
            DICTIONARY_BATCH = register("DictionaryBatch", 2, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::DictionaryBatch", "../../../apache/arrow/flatbuf/dictionary_batch")
            RECORD_BATCH = register("RecordBatch", 3, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::RecordBatch", "../../../apache/arrow/flatbuf/record_batch")
            TENSOR = register("Tensor", 4, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Tensor", "../../../apache/arrow/flatbuf/tensor")
            SPARSE_TENSOR = register("SparseTensor", 5, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::SparseTensor", "../../../apache/arrow/flatbuf/sparse_tensor")
            

            private def require_table_class
              require_relative @require_path
            end
          end
        end
      end
    end
  end
end
