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

class SchemaTest < Test::Unit::TestCase
  def setup
    @count_field = Arrow::Field.new("count", :uint32)
    @visible_field = Arrow::Field.new("visible", :boolean)
  end

  sub_test_case(".new") do
    test("[Arrow::Field]") do
      fields = [
        @count_field,
        @visible_field,
      ]
      assert_equal("count: uint32\n" +
                   "visible: bool",
                   Arrow::Schema.new(fields).to_s)
    end

    test("[Arrow::Field, Hash]") do
      fields = [
        @count_field,
        {name: "visible", type: :boolean},
      ]
      assert_equal("count: uint32\n" +
                   "visible: bool",
                   Arrow::Schema.new(fields).to_s)
    end

    test("{String, Symbol => Arrow::DataType}") do
      fields = {
        "count" => Arrow::UInt32DataType.new,
        :visible => :boolean,
      }
      assert_equal("count: uint32\n" +
                   "visible: bool",
                   Arrow::Schema.new(fields).to_s)
    end

    test("{String, Symbol => Hash}") do
      fields = {
        "count" => {type: :uint32},
        :tags => {
          type: :list,
          field: {
            name: "tag",
            type: :string,
          },
        },
      }
      assert_equal("count: uint32\n" +
                   "tags: list<tag: string>",
                   Arrow::Schema.new(fields).to_s)
    end
  end

  sub_test_case("instance methods") do
    def setup
      super
      @schema = Arrow::Schema.new([@count_field, @visible_field])
    end

    sub_test_case("#[]") do
      test("[String]") do
        assert_equal([@count_field, @visible_field],
                     [@schema["count"], @schema["visible"]])
      end

      test("[Symbol]") do
        assert_equal([@count_field, @visible_field],
                     [@schema[:count], @schema[:visible]])
      end

      test("[Integer]") do
        assert_equal([@count_field, @visible_field],
                     [@schema[0], @schema[1]])
      end

      test("[invalid]") do
        invalid = []
        message = "field name or index must be String, Symbol or Integer"
        message << ": <#{invalid.inspect}>"
        assert_raise(ArgumentError.new(message)) do
          @schema[invalid]
        end
      end
    end
  end
end
