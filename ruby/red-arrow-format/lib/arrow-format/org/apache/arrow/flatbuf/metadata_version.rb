# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.4
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          class MetadataVersion < ::FlatBuffers::Enum
            # 0.1.0 (October 2016).
            V1 = register("V1", 0)
            # 0.2.0 (February 2017). Non-backwards compatible with V1.
            V2 = register("V2", 1)
            # 0.3.0 -> 0.7.1 (May - December 2017). Non-backwards compatible with V2.
            V3 = register("V3", 2)
            # >= 0.8.0 (December 2017). Non-backwards compatible with V3.
            V4 = register("V4", 3)
            # >= 1.0.0 (July 2020). Backwards compatible with V4 (V5 readers can read V4
            # metadata and IPC messages). Implementations are recommended to provide a
            # V4 compatibility mode with V5 format changes disabled.
            #
            # Incompatible changes between V4 and V5:
            # - Union buffer layout has changed. In V5, Unions don't have a validity
            #   bitmap buffer.
            V5 = register("V5", 4)
          end
        end
      end
    end
  end
end
