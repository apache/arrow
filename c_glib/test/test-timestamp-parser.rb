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

class TestTimestampParser < Test::Unit::TestCase
  sub_test_case("strptime") do
    def setup
      @parser = Arrow::StrptimeTimestampParser.new("%Y-%m-%d")
    end

    def test_kind
      assert_equal("strptime", @parser.kind)
    end

    def test_format
      assert_equal("%Y-%m-%d", @parser.format)
    end
  end

  sub_test_case("ISO8601") do
    def setup
      @parser = Arrow::ISO8601TimestampParser.new
    end

    def test_kind
      assert_equal("iso8601", @parser.kind)
    end
  end
end
