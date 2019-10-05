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

class TestProjector < Test::Unit::TestCase
  def test_evaluate
    table = Arrow::Table.new(:field1 => Arrow::Int32Array.new([1, 13, 3, 17]),
                             :field2 => Arrow::Int32Array.new([11, 2, 15, 17]),
                             :field3 => Arrow::Int32Array.new([1, 10, 2, 2]))
    schema = table.schema

    expression1 = schema.build_expression do |record|
      record.field1 + record.field2
    end

    expression2 = schema.build_expression do |record, context|
      context.if(record.field1 > record.field2)
        .then(record.field1 + record.field2 * record.field3)
        .elsif(record.field1 == record.field2)
        .then(record.field1 - record.field2 / record.field3)
        .else(record.field2)
    end

    projector = Gandiva::Projector.new(schema,
                                       [expression1, expression2])

    table.each_record_batch do |record_batch|
      outputs = projector.evaluate(record_batch)
      assert_equal([
                     [12, 15, 18, 34],
                     [11, 33, 15, 9]
                   ],
                   outputs.collect(&:values))
    end
  end
end
