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

class TestRecordBatchDatum < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @array = build_boolean_array([true, false])
    @record_batch = build_record_batch("visible" => @array)
    @datum = Arrow::RecordBatchDatum.new(@record_batch)
  end

  def test_array?
    assert do
      not @datum.array?
    end
  end

  def test_array_like?
    assert do
      not @datum.array_like?
    end
  end

  sub_test_case("==") do
    def test_true
      assert_equal(Arrow::RecordBatchDatum.new(@record_batch),
                   Arrow::RecordBatchDatum.new(@record_batch))
    end

    def test_false
      assert_not_equal(@datum,
                       Arrow::ArrayDatum.new(@array))
    end
  end

  def test_to_string
    assert_equal("RecordBatch(visible:   [\n" + "    true,\n" + "    false\n" + "  ]\n" + ")", @datum.to_s)
  end

  def test_value
    assert_equal(@record_batch, @datum.value)
  end
end
