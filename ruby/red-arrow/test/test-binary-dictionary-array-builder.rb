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

class BinaryDictionaryArrayBuilderTest < Test::Unit::TestCase
  def setup
    @builder = Arrow::BinaryDictionaryArrayBuilder.new
  end

  sub_test_case("#append_values") do
    test("[nil]") do
      @builder.append_values([nil])
      array = @builder.finish
      assert_equal([
                     [],
                     [nil],
                   ],
                   [
                     array.dictionary.to_a,
                     array.indices.to_a,
                   ])
    end

    test("[String]") do
      @builder.append_values(["he\xffllo"])
      array = @builder.finish
      assert_equal([
                     ["he\xffllo".b],
                     [0],
                   ],
                   [
                     array.dictionary.to_a,
                     array.indices.to_a,
                   ])
    end

    test("[Symbol]") do
      @builder.append_values([:hello])
      array = @builder.finish
      assert_equal([
                     ["hello"],
                     [0],
                   ],
                   [
                     array.dictionary.to_a,
                     array.indices.to_a,
                   ])
    end

    test("[nil, String, Symbol]") do
      @builder.append_values([
                               nil,
                               "He\xffllo",
                               :world,
                               "world",
                             ])
      array = @builder.finish
      assert_equal([
                     ["He\xffllo".b, "world"],
                     [nil, 0, 1, 1],
                   ],
                   [
                     array.dictionary.to_a,
                     array.indices.to_a,
                   ])
    end

    test("is_valids") do
      @builder.append_values([
                               "He\xffllo",
                               :world,
                               :goodbye,
                             ],
                             [
                               true,
                               false,
                               true,
                             ])
      array = @builder.finish
      assert_equal([
                     ["He\xffllo".b, "goodbye"],
                     [0, nil, 1],
                   ],
                   [
                     array.dictionary.to_a,
                     array.indices.to_a,
                   ])
    end
  end
end
