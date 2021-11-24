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

class TestRecordBatchReader < Test::Unit::TestCase
  sub_test_case(".try_convert") do
    test("Arrow::RecordBatch") do
      record_batch =
        Arrow::RecordBatch.new("count" => [1, 2, 3],
                               "private" => [true, false, true])
      reader = Arrow::RecordBatchReader.try_convert(record_batch)
      assert_equal(record_batch,
                   reader.read_next)
    end

    test("[Arrow::RecordBatch]") do
      record_batch =
        Arrow::RecordBatch.new("count" => [1, 2, 3],
                               "private" => [true, false, true])
      reader = Arrow::RecordBatchReader.try_convert([record_batch])
      assert_equal(record_batch,
                   reader.read_next)
    end

    test("Arrow::Table") do
      table = Arrow::Table.new("count" => [1, 2, 3],
                               "private" => [true, false, true])
      reader = Arrow::RecordBatchReader.try_convert(table)
      assert_equal(table,
                   reader.read_all)
    end
  end
end
