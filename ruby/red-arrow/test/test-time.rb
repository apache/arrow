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

class TimeTest < Test::Unit::TestCase
  sub_test_case("#==") do
    test("same unit") do
      assert do
        Arrow::Time.new(:second, 10) == Arrow::Time.new(:second, 10)
      end
    end

    test("different unit") do
      assert do
        Arrow::Time.new(:second, 10) == Arrow::Time.new(:milli, 10 * 1000)
      end
    end

    test("false") do
      assert do
        not(Arrow::Time.new(:second, 10) == Arrow::Time.new(:second, 11))
      end
    end
  end

  sub_test_case("#cast") do
    test("same unit") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, 10)
      casted_time = time.cast(Arrow::TimeUnit::SECOND)
      assert_equal([time.unit, time.value],
                   [casted_time.unit, casted_time.value])
    end

    test("second -> milli") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, 10)
      casted_time = time.cast(Arrow::TimeUnit::MILLI)
      assert_equal([
                     Arrow::TimeUnit::MILLI,
                     time.value * 1000,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("second -> micro") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, 10)
      casted_time = time.cast(Arrow::TimeUnit::MICRO)
      assert_equal([
                     Arrow::TimeUnit::MICRO,
                     time.value * 1000 * 1000,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("second -> nano") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, 10)
      casted_time = time.cast(Arrow::TimeUnit::NANO)
      assert_equal([
                     Arrow::TimeUnit::NANO,
                     time.value * 1000 * 1000 * 1000,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("milli -> second") do
      time = Arrow::Time.new(Arrow::TimeUnit::MILLI, 10_200)
      casted_time = time.cast(Arrow::TimeUnit::SECOND)
      assert_equal([
                     Arrow::TimeUnit::SECOND,
                     10,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("milli -> micro") do
      time = Arrow::Time.new(Arrow::TimeUnit::MILLI, 10_200)
      casted_time = time.cast(Arrow::TimeUnit::MICRO)
      assert_equal([
                     Arrow::TimeUnit::MICRO,
                     time.value * 1000,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("milli -> nano") do
      time = Arrow::Time.new(Arrow::TimeUnit::MILLI, 10_200)
      casted_time = time.cast(Arrow::TimeUnit::NANO)
      assert_equal([
                     Arrow::TimeUnit::NANO,
                     time.value * 1000 * 1000,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("micro -> second") do
      time = Arrow::Time.new(Arrow::TimeUnit::MICRO, 10_200_300)
      casted_time = time.cast(Arrow::TimeUnit::SECOND)
      assert_equal([
                     Arrow::TimeUnit::SECOND,
                     10,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("micro -> milli") do
      time = Arrow::Time.new(Arrow::TimeUnit::MICRO, 10_200_300)
      casted_time = time.cast(Arrow::TimeUnit::MILLI)
      assert_equal([
                     Arrow::TimeUnit::MILLI,
                     10_200,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("micro -> nano") do
      time = Arrow::Time.new(Arrow::TimeUnit::MICRO, 10_200_300)
      casted_time = time.cast(Arrow::TimeUnit::NANO)
      assert_equal([
                     Arrow::TimeUnit::NANO,
                     time.value * 1000,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("nano -> second") do
      time = Arrow::Time.new(Arrow::TimeUnit::NANO, 10_200_300_400)
      casted_time = time.cast(Arrow::TimeUnit::SECOND)
      assert_equal([
                     Arrow::TimeUnit::SECOND,
                     10,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("nano -> milli") do
      time = Arrow::Time.new(Arrow::TimeUnit::NANO, 10_200_300_400)
      casted_time = time.cast(Arrow::TimeUnit::MILLI)
      assert_equal([
                     Arrow::TimeUnit::MILLI,
                     10_200,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end

    test("nano -> micro") do
      time = Arrow::Time.new(Arrow::TimeUnit::NANO, 10_200_300_400)
      casted_time = time.cast(Arrow::TimeUnit::MICRO)
      assert_equal([
                     Arrow::TimeUnit::MICRO,
                     10_200_300,
                   ],
                   [
                     casted_time.unit,
                     casted_time.value,
                   ])
    end
  end

  sub_test_case("#to_f") do
    test("second") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, 10)
      assert_in_delta(10.0, time.to_f)
    end

    test("milli") do
      time = Arrow::Time.new(Arrow::TimeUnit::MILLI, 10_200)
      assert_in_delta(10.2, time.to_f)
    end

    test("micro") do
      time = Arrow::Time.new(Arrow::TimeUnit::MICRO, 10_200_300)
      assert_in_delta(10.2003, time.to_f)
    end

    test("nano") do
      time = Arrow::Time.new(Arrow::TimeUnit::NANO, 10_200_300_400)
      assert_in_delta(10.2003004, time.to_f)
    end
  end

  sub_test_case("#positive?") do
    test("true") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, 10)
      assert do
        time.positive?
      end
    end

    test("false") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, -10)
      assert do
        not time.positive?
      end
    end
  end

  sub_test_case("#negative?") do
    test("true") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, -10)
      assert do
        time.negative?
      end
    end

    test("false") do
      time = Arrow::Time.new(Arrow::TimeUnit::SECOND, 10)
      assert do
        not time.negative?
      end
    end
  end

  test("#hour") do
    time = Arrow::Time.new(Arrow::TimeUnit::SECOND,
                           (5 * 60 * 60) + (12 * 60) + 10)
    assert_equal(5, time.hour)
  end

  test("#minute") do
    time = Arrow::Time.new(Arrow::TimeUnit::SECOND,
                           (5 * 60 * 60) + (12 * 60) + 10)
    assert_equal(12, time.minute)
  end

  test("#second") do
    time = Arrow::Time.new(Arrow::TimeUnit::SECOND,
                           (5 * 60 * 60) + (12 * 60) + 10)
    assert_equal(10, time.second)
  end

  test("#nano_second") do
    time = Arrow::Time.new(Arrow::TimeUnit::NANO, 1234)
    assert_equal(1234, time.nano_second)
  end

  test("#to_s") do
    time = Arrow::Time.new(Arrow::TimeUnit::NANO,
                           -(((5 * 60 * 60) + (12 * 60) + 10) * 1_000_000_000 +
                             1234))
    assert_equal("-05:12:10.000001234",
                 time.to_s)
  end
end
