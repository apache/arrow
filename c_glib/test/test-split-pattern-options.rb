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

class TestSplitPatternOptions < Test::Unit::TestCase
  def setup
    @options = Arrow::SplitPatternOptions.new
  end

  def test_pattern
    assert_equal("", @options.pattern)
    @options.pattern = "foo"
    assert_equal("foo", @options.pattern)
  end

  def test_max_splits
    assert_equal(-1, @options.max_splits)
    @options.max_splits = 1
    assert_equal(1, @options.max_splits)
  end

  def test_reverse
    assert do
      !@options.reverse?
    end
    @options.reverse = true
    assert do
      @options.reverse?
    end
  end
end
