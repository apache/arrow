# Automatically generated. Don't modify manually.
#
# Red FlatBuffers version: 0.0.3
# Declared by:             //Schema.fbs
# Rooting type:            org.apache.arrow.flatbuf.Message (//Message.fbs)

require "flatbuffers"

module ArrowFormat
  module Org
    module Apache
      module Arrow
        module Flatbuf
          # A Map is a logical nested type that is represented as
          #
          # List<entries: Struct<key: K, value: V>>
          #
          # In this layout, the keys and values are each respectively contiguous. We do
          # not constrain the key and value types, so the application is responsible
          # for ensuring that the keys are hashable and unique. Whether the keys are sorted
          # may be set in the metadata for this field.
          #
          # In a field with Map type, the field has a child Struct field, which then
          # has two children: key type and the second the value type. The names of the
          # child fields may be respectively "entries", "key", and "value", but this is
          # not enforced.
          #
          # Map
          # ```text
          #   - child[0] entries: Struct
          #     - child[0] key: K
          #     - child[1] value: V
          # ```
          # Neither the "entries" field nor the "key" field may be nullable.
          #
          # The metadata is structured so that Arrow systems without special handling
          # for Map can make Map an alias for List. The "layout" attribute for the Map
          # field must have the same contents as a List.
          class Map < ::FlatBuffers::Table
            # Set to true if the keys within each value are sorted
            def keys_sorted?
              field_offset = @view.unpack_virtual_offset(4)
              return false if field_offset.zero?

              @view.unpack_bool(field_offset)
            end
          end
        end
      end
    end
  end
end
