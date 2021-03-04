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

class DictionaryDataTypeTest < Test::Unit::TestCase
  sub_test_case(".new") do
    def setup
      @index_data_type = :int8
      @value_data_type = :string
      @ordered = true
    end

    test("ordered arguments") do
      assert_equal("dictionary<values=string, indices=int8, ordered=1>",
                   Arrow::DictionaryDataType.new(@index_data_type,
                                                 @value_data_type,
                                                 @ordered).to_s)
    end

    test("description") do
      assert_equal("dictionary<values=string, indices=int8, ordered=1>",
                   Arrow::DictionaryDataType.new(index_data_type: @index_data_type,
                                                 value_data_type: @value_data_type,
                                                 ordered: @ordered).to_s)
    end
  end
end
