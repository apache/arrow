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

class DenseUnionDataTypeTest < Test::Unit::TestCase
  sub_test_case(".new") do
    def setup
      @fields = [
        Arrow::Field.new("visible", :boolean),
        {
          name: "count",
          type: :int32,
        },
      ]
    end

    test("ordered arguments") do
      assert_equal("dense_union<visible: bool=2, count: int32=9>",
                   Arrow::DenseUnionDataType.new(@fields, [2, 9]).to_s)
    end

    test("description") do
      assert_equal("dense_union<visible: bool=2, count: int32=9>",
                   Arrow::DenseUnionDataType.new(fields: @fields,
                                                 type_codes: [2, 9]).to_s)
    end
  end
end
