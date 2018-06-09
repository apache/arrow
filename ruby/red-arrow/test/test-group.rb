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
	                     time	int
0	2018-01-29T00:00:00+09:00	  1
1	2018-01-30T00:00:00+09:00	  1
      TABLE
    end
  end

  sub_test_case("#count") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).count.to_s)
	group_key1	group_key2	int	uint	float	string
0	         1	         2	  2	   1	    1	     2
1	         2	         1	  0	   1	    1	     1
2	         3	         3	  3	   3	    3	     2
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).count.to_s)
	group_key1	group_key2	int	uint	float	string
0	         1	         1	  2	   1	    1	     2
1	         2	         1	  0	   1	    1	     1
2	         3	         1	  1	   1	    1	     0
3	         3	         2	  2	   2	    2	     2
      TABLE
    end
  end

  sub_test_case("#sum") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).sum.to_s)
	group_key1	group_key2	int	uint	     float
0	         1	         2	 -3	   1	  2.200000
1	         2	         1	  0	   3	  3.300000
2	         3	         5	-15	  15	 16.500000
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).sum.to_s)
	group_key1	group_key2	int	uint	     float
0	         1	         1	 -3	   1	  2.200000
1	         2	         1	  0	   3	  3.300000
2	         3	         1	 -4	   4	  4.400000
3	         3	         2	-11	  11	 12.100000
      TABLE
    end
  end

  sub_test_case("#average") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).average.to_s)
	group_key1	group_key2	       int	      uint	     float
0	         1	  1.000000	 -1.500000	  1.000000	  2.200000
1	         2	  1.000000	  0.000000	  3.000000	  3.300000
2	         3	  1.666667	 -5.000000	  5.000000	  5.500000
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).average.to_s)
	group_key1	group_key2	       int	      uint	     float
0	         1	         1	 -1.500000	  1.000000	  2.200000
1	         2	         1	  0.000000	  3.000000	  3.300000
2	         3	         1	 -4.000000	  4.000000	  4.400000
3	         3	         2	 -5.500000	  5.500000	  6.050000
      TABLE
    end
  end

  sub_test_case("#min") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).min.to_s)
	group_key1	group_key2	int	uint	     float
0	         1	         1	 -2	   1	  2.200000
1	         2	         1	   	   3	  3.300000
2	         3	         1	 -6	   4	  4.400000
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).min.to_s)
	group_key1	group_key2	int	uint	     float
0	         1	         1	 -2	   1	  2.200000
1	         2	         1	   	   3	  3.300000
2	         3	         1	 -4	   4	  4.400000
3	         3	         2	 -6	   5	  5.500000
      TABLE
    end
  end

  sub_test_case("#max") do
    test("single") do
      assert_equal(<<-TABLE, @table.group(:group_key1).max.to_s)
	group_key1	group_key2	int	uint	     float
0	         1	         1	 -1	   1	  2.200000
1	         2	         1	   	   3	  3.300000
2	         3	         2	 -4	   6	  6.600000
      TABLE
    end

    test("multiple") do
      assert_equal(<<-TABLE, @table.group(:group_key1, :group_key2).max.to_s)
	group_key1	group_key2	int	uint	     float
0	         1	         1	 -1	   1	  2.200000
1	         2	         1	   	   3	  3.300000
2	         3	         1	 -4	   4	  4.400000
3	         3	         2	 -5	   6	  6.600000
      TABLE
    end
  end
end
