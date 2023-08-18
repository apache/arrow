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

class TestFlightSQLCreatePreparedStatementResult < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def setup
    omit("Arrow Flight SQL is required") unless defined?(ArrowFlightSQL)
    @result = ArrowFlightSQL::CreatePreparedStatementResult.new
  end

  def test_dataset_schema
    assert_nil(@result.dataset_schema)
    schema = build_schema(text: :string, number: :int32)
    @result.dataset_schema = schema
    assert_equal(schema, @result.dataset_schema)
  end

  def test_parameter_schema
    assert_nil(@result.parameter_schema)
    schema = build_schema(text: :string, number: :int32)
    @result.parameter_schema = schema
    assert_equal(schema, @result.parameter_schema)
  end

  def test_handle
    assert_equal("", @result.handle.to_s)
    @result.handle = "valid-handle"
    assert_equal("valid-handle".to_s,
                 @result.handle.to_s)
  end
end
