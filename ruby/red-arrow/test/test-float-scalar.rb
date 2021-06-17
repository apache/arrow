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

class FloatScalarTest < Test::Unit::TestCase
  sub_test_case("#equal_scalar?") do
    test("no options") do
      scalar1 = Arrow::FloatScalar.new(1.1)
      scalar2 = Arrow::FloatScalar.new(1.1000001)
      assert do
        not scalar1.equal_scalar?(scalar2)
      end
    end

    test(":approx") do
      scalar1 = Arrow::FloatScalar.new(1.1)
      scalar2 = Arrow::FloatScalar.new(1.1000001)
      assert do
        scalar1.equal_scalar?(scalar2, approx: true)
      end
    end

    test(":absolute_tolerance") do
      scalar1 = Arrow::FloatScalar.new(1.1)
      scalar2 = Arrow::FloatScalar.new(1.1001)
      assert do
        scalar1.equal_scalar?(scalar2,
                              approx: true,
                              absolute_tolerance: 0.001)
      end
    end
  end
end
