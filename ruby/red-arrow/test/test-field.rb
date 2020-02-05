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

class FieldTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("String, Arrow::DataType") do
      assert_equal("visible: bool",
                   Arrow::Field.new("visible", Arrow::BooleanDataType.new).to_s)
    end

    test("Symbol, Arrow::DataType") do
      assert_equal("visible: bool",
                   Arrow::Field.new(:visible, Arrow::BooleanDataType.new).to_s)
    end

    test("String, Symbol") do
      assert_equal("visible: bool",
                   Arrow::Field.new(:visible, :boolean).to_s)
    end

    test("String, Hash") do
      assert_equal("visible: bool",
                   Arrow::Field.new(:visible, type: :boolean).to_s)
    end

    test("description: String") do
      assert_equal("visible: bool",
                   Arrow::Field.new(name: "visible",
                                    data_type: :boolean).to_s)
    end

    test("description: Symbol") do
      assert_equal("visible: bool",
                   Arrow::Field.new(name: :visible,
                                    data_type: :boolean).to_s)
    end

    test("description: shortcut") do
      assert_equal("visible: bool",
                   Arrow::Field.new(name: :visible,
                                    type: :boolean).to_s)
    end

    test("Hash: shortcut: additional") do
      description = {
        name: :tags,
        type: :list,
        field: {
          name: "tag",
          type: :string,
        },
      }
      assert_equal("tags: list<tag: string>",
                   Arrow::Field.new(description).to_s)
    end
  end

  sub_test_case("instance methods") do
    def setup
      @field = Arrow::Field.new("count", :uint32)
    end

    sub_test_case("#==") do
      test("Arrow::Field") do
        assert do
          @field == @field
        end
      end

      test("not Arrow::Field") do
        assert do
          not (@field == 29)
        end
      end
    end
  end
end
