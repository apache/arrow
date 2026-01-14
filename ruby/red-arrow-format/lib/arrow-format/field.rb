# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

module ArrowFormat
  class Field
    attr_reader :name
    attr_reader :type
    attr_reader :dictionary_id
    def initialize(name, type, nullable, dictionary_id)
      @name = name
      @type = type
      @nullable = nullable
      @dictionary_id = dictionary_id
    end

    def nullable?
      @nullable
    end

    def to_flat_buffers
      fb_field = FB::Field::Data.new
      fb_field.name = @name
      fb_field.nullable = @nullable
      if @type.is_a?(DictionaryType)
        fb_field.type = @type.value_type.to_flat_buffers
        dictionary_encoding = FB::DictionaryEncoding::Data.new
        dictionary_encoding.id = @dictionary_id
        int = FB::Int::Data.new
        int.bit_width = @type.index_type.bit_width
        int.signed = @type.index_type.signed?
        dictionary_encoding.index_type = int
        dictionary_encoding.ordered = @type.ordered?
        dictionary_encoding.dictionary_kind =
          FB::DictionaryKind::DENSE_ARRAY
        fb_field.dictionary = dictionary
      else
        fb_field.type = @type.to_flat_buffers
      end
      if @type.respond_to?(:children)
        fb_field.children = @type.children.collect(&:to_flat_buffers)
      end
      # fb_field.custom_metadata = @custom_metadata
      fb_field
    end
  end
end
