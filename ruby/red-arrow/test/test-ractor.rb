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

class RactorTest < Test::Unit::TestCase
  include Helper::Omittable

  ractor
  test("ChunkedArray") do
    require_ruby(3, 1, 0)
    array = Arrow::Array.new([1, 2, 3])
    chunked_array = Arrow::ChunkedArray.new([array])
    Ractor.make_shareable(chunked_array)
    ractor = Ractor.new do
      recived_chunked_array = Ractor.receive
      recived_chunked_array.chunks
    end
    ractor.send(chunked_array)
    assert_equal([array], ractor.take)
  end
end
