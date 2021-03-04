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

class TestCast < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_safe
    data = [-1, 2, nil]
    assert_equal(build_int32_array(data),
                 build_int8_array(data).cast(Arrow::Int32DataType.new))
  end

  sub_test_case("allow-int-overflow") do
    def test_default
      assert_raise(Arrow::Error::Invalid) do
        build_int32_array([128]).cast(Arrow::Int8DataType.new)
      end
    end

    def test_true
      options = Arrow::CastOptions.new
      options.allow_int_overflow = true
      assert_equal(build_int8_array([-128]),
                   build_int32_array([128]).cast(Arrow::Int8DataType.new,
                                                 options))
    end
  end

  sub_test_case("allow-time-truncate") do
    def test_default
      after_epoch_in_milli = 1504953190854 # 2017-09-09T10:33:10.854Z
      second_timestamp_data_type = Arrow::TimestampDataType.new(:second)
      milli_array = build_timestamp_array(:milli, [after_epoch_in_milli])
      assert_raise(Arrow::Error::Invalid) do
        milli_array.cast(second_timestamp_data_type)
      end
    end

    def test_true
      options = Arrow::CastOptions.new
      options.allow_time_truncate = true
      after_epoch_in_milli = 1504953190854 # 2017-09-09T10:33:10.854Z
      second_array = build_timestamp_array(:second,
                                           [after_epoch_in_milli / 1000])
      milli_array  = build_timestamp_array(:milli, [after_epoch_in_milli])
      second_timestamp_data_type = Arrow::TimestampDataType.new(:second)
      assert_equal(second_array,
                   milli_array.cast(second_timestamp_data_type, options))
    end
  end

  sub_test_case("allow-time-overflow") do
    def test_default
      after_epoch_in_second = 95617584000 # 5000-01-01T00:00:00Z
      nano_timestamp_data_type = Arrow::TimestampDataType.new(:nano)
      second_array = build_timestamp_array(:second, [after_epoch_in_second])
      assert_raise(Arrow::Error::Invalid) do
        second_array.cast(nano_timestamp_data_type)
      end
    end

    def test_true
      options = Arrow::CastOptions.new
      options.allow_time_overflow = true
      after_epoch_in_second = 95617584000 # 5000-01-01T00:00:00Z
      second_array = build_timestamp_array(:second,
                                           [after_epoch_in_second])
      after_epoch_in_nano_overflowed =
        (after_epoch_in_second * 1000 * 1000 * 1000) % (2 ** 64)
      nano_array   = build_timestamp_array(:nano,
                                           [after_epoch_in_nano_overflowed])
      nano_timestamp_data_type = Arrow::TimestampDataType.new(:nano)
      assert_equal(nano_array,
                   second_array.cast(nano_timestamp_data_type, options))
    end
  end

  sub_test_case("allow-decimal-truncate") do
    def test_default
      decimal128_data_type = Arrow::Decimal128DataType.new(8, 2)
      decimal128_array = build_decimal128_array(decimal128_data_type,
                                                ["23423445"])
      assert_raise(Arrow::Error::Invalid) do
        decimal128_array.cast(Arrow::Int64DataType.new)
      end
    end

    def test_true
      options = Arrow::CastOptions.new
      options.allow_decimal_truncate = true
      decimal128_data_type = Arrow::Decimal128DataType.new(8, 2)
      decimal128_array = build_decimal128_array(decimal128_data_type,
                                                ["23423445"])
      assert_equal(build_int64_array([234234]),
                   decimal128_array.cast(Arrow::Int64DataType.new, options))
    end
  end

  sub_test_case("allow-float-truncate") do
    def test_default
      assert_raise(Arrow::Error::Invalid) do
        build_float_array([1.1]).cast(Arrow::Int8DataType.new)
      end
    end

    def test_true
      options = Arrow::CastOptions.new
      options.allow_float_truncate = true
      int8_data_type = Arrow::Int8DataType.new
      assert_equal(build_int8_array([1]),
                   build_float_array([1.1]).cast(int8_data_type, options))
    end
  end

  sub_test_case("allow-invalid-utf8") do
    def test_default
      assert_raise(Arrow::Error::Invalid) do
        build_binary_array(["\xff"]).cast(Arrow::StringDataType.new)
      end
    end

    def test_true
      options = Arrow::CastOptions.new
      options.allow_invalid_utf8 = true
      string_data_type = Arrow::StringDataType.new
      assert_equal(build_string_array(["\xff"]),
                   build_binary_array(["\xff"]).cast(string_data_type, options))
    end
  end
end
