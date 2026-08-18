# Licensed to the Apache Software Foundation (ASF) under one
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
    attr_reader :message_metadata
    def initialize(fields, metadata: nil, message_metadata: nil)
      @fields = fields
      @metadata = metadata
      @message_metadata = message_metadata
    end

    def [](name_or_index)
      case name_or_index
      when Integer
        @fields[name_or_index]
      when Symbol
        name_to_field[name_or_index.to_s]
      else
        name_to_field[name_or_index.to_str]
      end
    end

    def to_flatbuffers
      fb_schema = FB::Schema::Data.new
      fb_schema.endianness = FB::Endianness::LITTLE
      fb_schema.fields = fields.collect(&:to_flatbuffers)
      fb_schema.custom_metadata = FB.build_custom_metadata(@metadata)
      # fb_schema.features = @features
      fb_schema
    end

    private
    def name_to_field
      @name_to_field ||= build_name_to_field
    end

    def build_name_to_field
      name_to_field = {}
      @fields.each do |field|
        name_to_field[field.name] = field
      end
      name_to_field
    end
  end
end
