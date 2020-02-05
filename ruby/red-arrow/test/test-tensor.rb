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

class TensorTest < Test::Unit::TestCase
  sub_test_case("instance methods") do
    def setup
      raw_data = [
        1, 2,
        3, 4,

        5, 6,
        7, 8,

        9, 10,
        11, 12,
      ]
      data = Arrow::Buffer.new(raw_data.pack("c*"))
      shape = [3, 2, 2]
      strides = []
      names = ["a", "b", "c"]
      @tensor = Arrow::Tensor.new(Arrow::Int8DataType.new,
                                  data,
                                  shape,
                                  strides,
                                  names)
    end

    sub_test_case("#==") do
      test("Arrow::Tensor") do
        assert do
          @tensor == @tensor
        end
      end

      test("not Arrow::Tensor") do
        assert do
          not (@tensor == 29)
        end
      end
    end
  end
end
