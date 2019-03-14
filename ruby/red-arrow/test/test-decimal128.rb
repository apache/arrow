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

class Decimal128Test < Test::Unit::TestCase
  sub_test_case("instance methods") do
    def setup
      @decimal128 = Arrow::Decimal128.new("10.1")
    end

    sub_test_case("#==") do
      test("Arrow::Decimal128") do
        assert do
          @decimal128 == @decimal128
        end
      end

      test("not Arrow::Decimal128") do
        assert do
          not (@decimal128 == 10.1)
        end
      end
    end

    sub_test_case("#!=") do
      test("Arrow::Decimal128") do
        assert do
          not (@decimal128 != @decimal128)
        end
      end

      test("not Arrow::Decimal128") do
        assert do
          @decimal128 != 10.1
        end
      end
    end
  end
end
