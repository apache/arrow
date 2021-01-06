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

class Decimal256Test < Test::Unit::TestCase
  sub_test_case("instance methods") do
    def setup
      @decimal256 = Arrow::Decimal256.new("10.1")
    end

    sub_test_case("#==") do
      test("Arrow::Decimal256") do
        assert do
          @decimal256 == @decimal256
        end
      end

      test("not Arrow::Decimal256") do
        assert do
          not (@decimal256 == 10.1)
        end
      end
    end

    sub_test_case("#!=") do
      test("Arrow::Decimal256") do
        assert do
          not (@decimal256 != @decimal256)
        end
      end

      test("not Arrow::Decimal256") do
        assert do
          @decimal256 != 10.1
        end
      end
    end

    sub_test_case("#to_s") do
      test("default") do
        assert_equal("101",
                     @decimal256.to_s)
      end

      test("scale") do
        assert_equal("10.1",
                     @decimal256.to_s(1))
      end
    end

    test("#abs") do
      decimal256 = Arrow::Decimal256.new("-10.1")
      assert_equal([
                     Arrow::Decimal256.new("-10.1"),
                     Arrow::Decimal256.new("10.1"),
                   ],
                   [
                     decimal256,
                     decimal256.abs,
                   ])
    end

    test("#abs!") do
      decimal256 = Arrow::Decimal256.new("-10.1")
      decimal256.abs!
      assert_equal(Arrow::Decimal256.new("10.1"),
                   decimal256)
    end

    test("#negate") do
      decimal256 = Arrow::Decimal256.new("-10.1")
      assert_equal([
                     Arrow::Decimal256.new("-10.1"),
                     Arrow::Decimal256.new("10.1"),
                   ],
                   [
                     decimal256,
                     decimal256.negate,
                   ])
    end

    test("#negate!") do
      decimal256 = Arrow::Decimal256.new("-10.1")
      decimal256.negate!
      assert_equal(Arrow::Decimal256.new("10.1"),
                   decimal256)
    end
  end
end
