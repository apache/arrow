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
  class Schema
    attr_reader :fields
    attr_reader :metadata
    def initialize(fields, metadata: nil)
      @fields = fields
      @metadata = metadata
    end

    def to_flatbuffers
      fb_schema = FB::Schema::Data.new
      fb_schema.endianness = FB::Endianness::LITTLE
      fb_schema.fields = fields.collect(&:to_flatbuffers)
      if @metadata
        fb_schema.custom_metadata = @metadata.collect do |key, value|
          fb_key_value = FB::KeyValue::Data.new
          fb_key_value.key = key
          fb_key_value.value = value
          fb_key_value
        end
      end
      # fb_schema.features = @features
      fb_schema
    end
  end
end
