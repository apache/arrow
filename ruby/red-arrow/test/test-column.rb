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

class ColumnTest < Test::Unit::TestCase
  def setup
    table = Arrow::Table.new("visible" => [true, nil, false])
    @column = table.visible
  end

  test("#name") do
    assert_equal("visible", @column.name)
  end

  test("#data_type") do
    assert_equal(Arrow::BooleanDataType.new, @column.data_type)
  end

  test("#null?") do
    assert do
      @column.null?(1)
    end
  end

  test("#valid?") do
    assert do
      @column.valid?(0)
    end
  end

  test("#each") do
    assert_equal([true, nil, false], @column.each.to_a)
  end

  test("#reverse_each") do
    assert_equal([false, nil, true], @column.reverse_each.to_a)
  end

  test("#n_rows") do
    assert_equal(3, @column.n_rows)
  end

  test("#n_nulls") do
    assert_equal(1, @column.n_nulls)
  end

  sub_test_case("#==") do
    test("same value") do
      table1 = Arrow::Table.new("visible" => [true, false])
      table2 = Arrow::Table.new("visible" => [true, false])
      assert do
        table1.visible == table2.visible
      end
    end

    test("different name") do
      table1 = Arrow::Table.new("visible" => [true, false])
      table2 = Arrow::Table.new("invisible" => [true, false])
      assert do
        not table1.visible == table2.invisible
      end
    end

    test("different value") do
      table1 = Arrow::Table.new("visible" => [true, false])
      table2 = Arrow::Table.new("visible" => [true, true])
      assert do
        not table1.visible == table2.visible
      end
    end

    test("not Arrow::Column") do
      table = Arrow::Table.new("visible" => [true, false])
      assert do
        not table.visible == 29
      end
    end
  end
end
