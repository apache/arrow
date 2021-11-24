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

class TestEqualOptions < Test::Unit::TestCase
  include Helper::Buildable

  sub_test_case("approx") do
    def setup
      @options = Arrow::EqualOptions.new
    end

    def test_accessor
      assert do
        not @options.approx?
      end
      @options.approx = true
      assert do
        @options.approx?
      end
    end

    def test_compare
      array1 = build_float_array([0.01])
      array2 = build_float_array([0.010001])
      @options.approx = true
      assert do
        array1.equal_options(array2, @options)
      end
    end
  end

  sub_test_case("nans-equal") do
    def setup
      @options = Arrow::EqualOptions.new
    end

    def test_accessor
      assert do
        not @options.nans_equal?
      end
      @options.nans_equal = true
      assert do
        @options.nans_equal?
      end
    end

    def test_compare
      array1 = build_float_array([0.1, Float::NAN, 0.2])
      array2 = build_float_array([0.1, Float::NAN, 0.2])
      @options.nans_equal = true
      assert do
        array1.equal_options(array2, @options)
      end
    end
  end

  sub_test_case("absolute-tolerance") do
    def setup
      @options = Arrow::EqualOptions.new
    end

    def test_accessor
      assert do
        @options.absolute_tolerance < 0.001
      end
      @options.absolute_tolerance = 0.001
      assert do
        @options.absolute_tolerance >= 0.001
      end
    end

    def test_compare
      array1 = build_float_array([0.01])
      array2 = build_float_array([0.0109])
      @options.approx = true
      @options.absolute_tolerance = 0.001
      assert do
        array1.equal_options(array2, @options)
      end
    end
  end
end
