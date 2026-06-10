# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //SparseTensor.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class SparseTensorIndex < ::FlatBuffers::Union
            NONE = register("NONE", 0, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::Utf8View", "../../../apache/arrow/flatbuf/utf8view")
            SPARSE_TENSOR_INDEX_COO = register("SparseTensorIndexCOO", 1, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::SparseTensorIndexCOO", "../../../apache/arrow/flatbuf/sparse_tensor_index_coo")
            SPARSE_MATRIX_INDEX_CSX = register("SparseMatrixIndexCSX", 2, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::SparseMatrixIndexCSX", "../../../apache/arrow/flatbuf/sparse_matrix_index_csx")
            SPARSE_TENSOR_INDEX_CSF = register("SparseTensorIndexCSF", 3, "::ArrowFormat::Org::Apache::Arrow::Flatbuf::SparseTensorIndexCSF", "../../../apache/arrow/flatbuf/sparse_tensor_index_csf")
            

            private def require_table_class
              require_relative @require_path
            end
          end
        end
      end
    end
  end
end
