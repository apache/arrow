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

class TestTableBatchReader < Test::Unit::TestCase
  include Helper::Buildable

  def test_empty
    table = build_table("visible" => build_boolean_array([]))
    reader = Arrow::TableBatchReader.new(table)
    assert_nil(reader.read_next)
  end

  def test_have_record
    array = build_boolean_array([true])
    table = build_table("visible" => array)
    reader = Arrow::TableBatchReader.new(table)
    assert_equal(build_record_batch("visible" => array),
                 reader.read_next)
    assert_nil(reader.read_next)
  end

  def test_schema
    array = build_boolean_array([])
    table = build_table("visible" => array)
    reader = Arrow::TableBatchReader.new(table)
    assert_equal(table.schema, reader.schema)
  end
end
