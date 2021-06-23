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

class SortKeyTest < Test::Unit::TestCase
  sub_test_case(".resolve") do
    test("SortKey") do
      assert_equal(Arrow::SortKey.new("-count"),
                   Arrow::SortKey.resolve(Arrow::SortKey.new("-count")))
    end

    test("-String") do
      assert_equal(Arrow::SortKey.new("-count"),
                   Arrow::SortKey.resolve("-count"))
    end

    test("Symbol, Symbol") do
      assert_equal(Arrow::SortKey.new("-count"),
                   Arrow::SortKey.resolve(:count, :desc))
    end
  end

  sub_test_case("#initialize") do
    test("String") do
      assert_equal("+count",
                   Arrow::SortKey.new("count").to_s)
    end

    test("+String") do
      assert_equal("+count",
                   Arrow::SortKey.new("+count").to_s)
    end

    test("-String") do
      assert_equal("-count",
                   Arrow::SortKey.new("-count").to_s)
    end

    test("Symbol") do
      assert_equal("+-count",
                   Arrow::SortKey.new(:"-count").to_s)
    end

    test("String, Symbol") do
      assert_equal("--count",
                   Arrow::SortKey.new("-count", :desc).to_s)
    end

    test("String, String") do
      assert_equal("--count",
                   Arrow::SortKey.new("-count", "desc").to_s)
    end

    test("String, SortOrder") do
      assert_equal("--count",
                   Arrow::SortKey.new("-count",
                                      Arrow::SortOrder::DESCENDING).to_s)
    end
  end

  sub_test_case("#to_s") do
    test("recreatable") do
      key = Arrow::SortKey.new("-count", :desc)
      assert_equal(key,
                   Arrow::SortKey.new(key.to_s))
    end
  end
end
