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

class TestTableConcatenateOptions < Test::Unit::TestCase
  def setup
    @options = Arrow::TableConcatenateOptions.new
  end

  def test_unify_schemas
    assert do
      not @options.unify_schemas?
    end
    @options.unify_schemas = true
    assert do
      @options.unify_schemas?
    end
  end

  def test_promote_nullability
    assert do
      @options.promote_nullability?
    end
    @options.promote_nullability = false
    assert do
      not @options.promote_nullability?
    end
  end
end
