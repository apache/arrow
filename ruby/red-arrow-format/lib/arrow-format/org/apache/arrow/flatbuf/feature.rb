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
          # Represents Arrow Features that might not have full support
          # within implementations. This is intended to be used in
          # two scenarios:
          #  1.  A mechanism for readers of Arrow Streams
          #      and files to understand that the stream or file makes
          #      use of a feature that isn't supported or unknown to
          #      the implementation (and therefore can meet the Arrow
          #      forward compatibility guarantees).
          #  2.  A means of negotiating between a client and server
          #      what features a stream is allowed to use. The enums
          #      values here are intended to represent higher level
          #      features, additional details may be negotiated
          #      with key-value pairs specific to the protocol.
          #
          # Enums added to this list should be assigned power-of-two values
          # to facilitate exchanging and comparing bitmaps for supported
          # features.
          class Feature < ::FlatBuffers::Enum
            # Needed to make flatbuffers happy.
            UNUSED = register("UNUSED", 0)
            # The stream makes use of multiple full dictionaries with the
            # same ID and assumes clients implement dictionary replacement
            # correctly.
            DICTIONARY_REPLACEMENT = register("DICTIONARY_REPLACEMENT", 1)
            # The stream makes use of compressed bodies as described
            # in Message.fbs.
            COMPRESSED_BODY = register("COMPRESSED_BODY", 2)
          end
        end
      end
    end
  end
end
