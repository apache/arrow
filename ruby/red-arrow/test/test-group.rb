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

class GroupTest < Test::Unit::TestCase
  include Helper::Fixture

  def setup
    raw_table = {
      :group_key1 => Arrow::UInt8Array.new([1, 1, 2, 3, 3, 3]),
      :group_key2 => Arrow::UInt8Array.new([1, 1, 1, 1, 2, 2]),
      :int => Arrow::Int32Array.new([-1, -2, nil, -4, -5, -6]),
      :uint => Arrow::UInt32Array.new([1, nil, 3, 4, 5, 6]),
      :float => Arrow::FloatArray.new([nil, 2.2, 3.3, 4.4, 5.5, 6.6]),
      :string => Arrow::StringArray.new(["a", "b", "c", nil, "e", "f"]),
    }
    @table = Arrow::Table.new(raw_table)
  end

  sub_test_case("key") do
    test("Time") do
      time_values = [
        Time.parse("2018-01-29"),
        Time.parse("2018-01-30"),
      ]
      raw_table = {
        :time => Arrow::ArrayBuilder.build(time_values),
        :int => Arrow::Int32Array.new([-1, -2]),
      }
      table = Arrow::Table.new(raw_table)
      assert_equal(<<-TABLE, table.group(:time).count.to_s)
	count(int)	                     time
0	         1	#{time_values[0].iso8601}
1	         1	#{time_values[1].iso8601}
      TABLE
    end
  end

  sub_test_case("#count") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).count.to_s)
	count(group_key2)	count(int)	count(uint)	count(float)	count(string)	group_key1
0	                2	         2	          1	           1	            2	         1
1	                1	         0	          1	           1	            1	         2
2	                3	         3	          3	           3	            2	         3
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).count.to_s)
	count(int)	count(uint)	count(float)	count(string)	group_key1	group_key2
0	         2	          1	           1	            2	         1	         1
1	         0	          1	           1	            1	         2	         1
2	         1	          1	           1	            0	         3	         1
3	         2	          2	           2	            2	         3	         2
      TABLE
    end

    test("column") do
      group = @table.group(:group_key1, :group_key2)
      assert_equal(<<-TABLE, group.count(:int, :uint).to_s)
	count(int)	count(uint)	group_key1	group_key2
0	         2	          1	         1	         1
1	         0	          1	         2	         1
2	         1	          1	         3	         1
3	         2	          2	         3	         2
      TABLE
    end
  end

  sub_test_case("#sum") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).sum.to_s)
	sum(group_key2)	sum(int)	sum(uint)	sum(float)	group_key1
0	              2	      -3	        1	  2.200000	         1
1	              1	  (null)	        3	  3.300000	         2
2	              5	     -15	       15	 16.500000	         3
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).sum.to_s)
	sum(int)	sum(uint)	sum(float)	group_key1	group_key2
0	      -3	        1	  2.200000	         1	         1
1	  (null)	        3	  3.300000	         2	         1
2	      -4	        4	  4.400000	         3	         1
3	     -11	       11	 12.100000	         3	         2
      TABLE
    end
  end

  sub_test_case("#mean") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).mean.to_s)
	mean(group_key2)	 mean(int)	mean(uint)	mean(float)	group_key1
0	        1.000000	 -1.500000	  1.000000	   2.200000	         1
1	        1.000000	    (null)	  3.000000	   3.300000	         2
2	        1.666667	 -5.000000	  5.000000	   5.500000	         3
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).mean.to_s)
	 mean(int)	mean(uint)	mean(float)	group_key1	group_key2
0	 -1.500000	  1.000000	   2.200000	         1	         1
1	    (null)	  3.000000	   3.300000	         2	         1
2	 -4.000000	  4.000000	   4.400000	         3	         1
3	 -5.500000	  5.500000	   6.050000	         3	         2
      TABLE
    end
  end

  sub_test_case("#min") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).min.to_s)
	min(group_key2)	min(int)	min(uint)	min(float)	group_key1
0	              1	      -2	        1	  2.200000	         1
1	              1	  (null)	        3	  3.300000	         2
2	              1	      -6	        4	  4.400000	         3
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).min.to_s)
	min(int)	min(uint)	min(float)	group_key1	group_key2
0	      -2	        1	  2.200000	         1	         1
1	  (null)	        3	  3.300000	         2	         1
2	      -4	        4	  4.400000	         3	         1
3	      -6	        5	  5.500000	         3	         2
      TABLE
    end
  end

  sub_test_case("#max") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).max.to_s)
	max(group_key2)	max(int)	max(uint)	max(float)	group_key1
0	              1	      -1	        1	  2.200000	         1
1	              1	  (null)	        3	  3.300000	         2
2	              2	      -4	        6	  6.600000	         3
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).max.to_s)
	max(int)	max(uint)	max(float)	group_key1	group_key2
0	      -1	        1	  2.200000	         1	         1
1	  (null)	        3	  3.300000	         2	         1
2	      -4	        4	  4.400000	         3	         1
3	      -5	        6	  6.600000	         3	         2
      TABLE
    end
  end

  sub_test_case("#aggregate") do
    test("function()") do
      group = @table.group(:group_key1, :group_key2)
      assert_equal(<<-TABLE, group.aggregate("count(int)", "sum(uint)").to_s)
	count(int)	sum(uint)	group_key1	group_key2
0	         2	        1	         1	         1
1	         0	        3	         2	         1
2	         1	        4	         3	         1
3	         2	       11	         3	         2
      TABLE
    end
  end
end
