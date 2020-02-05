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

class TestDate32Array < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_new
    after_epoch = 17406 # 2017-08-28
    raw_data = [0, after_epoch]
    assert_equal(build_date32_array([*raw_data, nil]),
                 Arrow::Date32Array.new(3,
                                        Arrow::Buffer.new(raw_data.pack("l*")),
                                        Arrow::Buffer.new([0b011].pack("C*")),
                                        -1))
  end

  def test_buffer
    before_epoch = -3653 # 1960-01-01
    after_epoch = 17406 # 2017-08-28

    builder = Arrow::Date32ArrayBuilder.new
    builder.append_value(0)
    builder.append_value(after_epoch)
    builder.append_value(before_epoch)
    array = builder.finish
    assert_equal([0, after_epoch, before_epoch].pack("l*"),
                 array.buffer.data.to_s)
  end

  def test_value
    after_epoch = 17406 # 2017-08-28

    builder = Arrow::Date32ArrayBuilder.new
    builder.append_value(after_epoch)
    array = builder.finish
    assert_equal(after_epoch, array.get_value(0))
  end

  def test_values
    before_epoch = -3653 # 1960-01-01
    after_epoch = 17406 # 2017-08-28

    builder = Arrow::Date32ArrayBuilder.new
    builder.append_value(0)
    builder.append_value(after_epoch)
    builder.append_value(before_epoch)
    array = builder.finish
    assert_equal([0, after_epoch, before_epoch], array.values)
  end
end
