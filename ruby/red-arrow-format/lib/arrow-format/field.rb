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
    attr_reader :metadata
    def initialize(name,
                   type,
                   nullable: true,
                   dictionary_id: nil,
                   metadata: nil)
      @name = name
      @type = type
      @nullable = nullable
      @dictionary_id = dictionary_id
      @metadata = metadata
    end

    def nullable?
      @nullable
    end

    def to_flatbuffers
      fb_field = FB::Field::Data.new
      fb_field.name = @name
      fb_field.nullable = @nullable
      if @type.respond_to?(:build_fb_field)
        @type.build_fb_field(fb_field, self)
      else
        fb_field.type = @type.to_flatbuffers
      end
      if @type.respond_to?(:child)
        fb_field.children = [@type.child.to_flatbuffers]
      elsif @type.respond_to?(:children)
        fb_field.children = @type.children.collect(&:to_flatbuffers)
      end
      if @metadata
        fb_field.custom_metadata = @metadata.collect do |key, value|
          fb_key_value = FB::KeyValue::Data.new
          fb_key_value.key = key
          fb_key_value.value = value
          fb_key_value
        end
      end
      fb_field
    end
  end
end
