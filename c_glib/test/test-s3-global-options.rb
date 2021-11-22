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

class TestS3GlobalOptions < Test::Unit::TestCase
  def setup
    omit("S3 enabled Apache Arrow C++ is needed") unless Arrow.s3_is_enabled?
    @options = Arrow::S3GlobalOptions.new
  end

  sub_test_case("#log_level") do
    test("default") do
      assert_equal(Arrow::S3LogLevel::FATAL,
                   @options.log_level)
    end
  end

  test("#log_level=") do
    @options.log_level = :trace
    assert_equal(Arrow::S3LogLevel::TRACE,
                 @options.log_level)
  end
end
