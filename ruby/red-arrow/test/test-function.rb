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

class FunctionTest < Test::Unit::TestCase
  sub_test_case("#execute") do
    test("Arrow::Array") do
      or_function = Arrow::Function.find("or")
      args = [
        Arrow::BooleanArray.new([true, false, false]),
        Arrow::BooleanArray.new([true, false, true]),
      ]
      assert_equal([true, false, true],
                   or_function.execute(args).value.to_a)
    end

    test("Array") do
      or_function = Arrow::Function.find("or")
      args = [
        [true, false, false],
        [true, false, true],
      ]
      assert_equal([true, false, true],
                   or_function.execute(args).value.to_a)
    end

    test("Arrow::ChunkedArray") do
      or_function = Arrow::Function.find("or")
      args = [
        Arrow::ChunkedArray.new([
                                  Arrow::BooleanArray.new([true]),
                                  Arrow::BooleanArray.new([false, false]),
                                ]),
        Arrow::ChunkedArray.new([
                                  Arrow::BooleanArray.new([true, false]),
                                  Arrow::BooleanArray.new([true]),
                                ]),
      ]
      assert_equal([true, false, true],
                   or_function.execute(args).value.to_a)
    end

    test("Arrow::Column") do
      or_function = Arrow::Function.find("or")
      table = Arrow::Table.new(a: [true, false, false],
                               b: [true, false, true])
      assert_equal([true, false, true],
                   or_function.execute([table.a, table.b]).value.to_a)
    end

    test("Arrow::Scalar") do
      add_function = Arrow::Function.find("add")
      args = [
        Arrow::Int8Array.new([1, 2, 3]),
        Arrow::Int8Scalar.new(5),
      ]
      assert_equal([6, 7, 8],
                   add_function.execute(args).value.to_a)
    end

    test("Integer") do
      add_function = Arrow::Function.find("add")
      args = [
        [1, 2, 3],
        5,
      ]
      assert_equal([6, 7, 8],
                   add_function.execute(args).value.to_a)
    end

    test("Float") do
      add_function = Arrow::Function.find("add")
      args = [
        [1, 2, 3],
        5.1,
      ]
      assert_equal([6.1, 7.1, 8.1],
                   add_function.execute(args).value.to_a)
    end

    test("true") do
      and_function = Arrow::Function.find("and")
      args = [
        Arrow::BooleanArray.new([true, false, false]),
        true,
      ]
      assert_equal([true, false, false],
                   and_function.execute(args).value.to_a)
    end

    test("false") do
      or_function = Arrow::Function.find("or")
      args = [
        Arrow::BooleanArray.new([true, false, false]),
        false,
      ]
      assert_equal([true, false, false],
                   or_function.execute(args).value.to_a)
    end

    test("String") do
      ascii_upper_function = Arrow::Function.find("ascii_upper")
      args = [
        "Hello",
      ]
      assert_equal("HELLO",
                   ascii_upper_function.execute(args).value.to_s)
    end

    test("Date") do
      cast_function = Arrow::Function.find("cast")
      date = Date.new(2021, 6, 12)
      args = [date]
      options = {
        to_data_type: Arrow::TimestampDataType.new(:second),
      }
      time = Time.utc(date.year,
                      date.month,
                      date.day)
      assert_equal(Arrow::TimestampScalar.new(options[:to_data_type],
                                              time.to_i),
                   cast_function.execute(args, options).value)
    end

    test("Arrow::Time: second") do
      cast_function = Arrow::Function.find("cast")
      arrow_time = Arrow::Time.new(Arrow::TimeUnit::SECOND,
                                   # 00:10:00
                                   60 * 10)
      args = [arrow_time]
      options = {
        to_data_type: Arrow::Time64DataType.new(:micro),
      }
      assert_equal(Arrow::Time64Scalar.new(options[:to_data_type],
                                           # 00:10:00.000000
                                           60 * 10 * 1000 * 1000),
                   cast_function.execute(args, options).value)
    end

    test("Arrow::Time: micro") do
      cast_function = Arrow::Function.find("cast")
      arrow_time = Arrow::Time.new(Arrow::TimeUnit::MICRO,
                                   # 00:10:00.000000
                                   60 * 10 * 1000 * 1000)
      args = [arrow_time]
      options = {
        to_data_type: Arrow::Time32DataType.new(:second),
        allow_time_truncate: true,
      }
      assert_equal(Arrow::Time32Scalar.new(options[:to_data_type],
                                           # 00:10:00
                                           60 * 10),
                   cast_function.execute(args, options).value)
    end

    test("Time") do
      cast_function = Arrow::Function.find("cast")
      time = Time.utc(2021, 6, 12, 1, 2, 3, 1)
      args = [time]
      options = {
        to_data_type: Arrow::TimestampDataType.new(:second),
        allow_time_truncate: true,
      }
      time = Time.utc(time.year,
                      time.month,
                      time.day,
                      time.hour,
                      time.min,
                      time.sec)
      assert_equal(Arrow::TimestampScalar.new(options[:to_data_type],
                                              time.to_i),
                   cast_function.execute(args, options).value)
    end

    test("SetLookupOptions") do
      is_in_function = Arrow::Function.find("is_in")
      args = [
        Arrow::Int16Array.new([1, 0, 1, 2]),
      ]
      options = {
        value_set: Arrow::Int16Array.new([2, 0]),
      }
      assert_equal(Arrow::BooleanArray.new([false, true, false, true]),
                   is_in_function.execute(args, options).value)
    end
  end

  def test_call
    or_function = Arrow::Function.find("or")
    args = [
      Arrow::BooleanArray.new([true, false, false]),
      Arrow::BooleanArray.new([true, false, true]),
    ]
    assert_equal([true, false, true],
                 or_function.call(args).value.to_a)
  end
end
