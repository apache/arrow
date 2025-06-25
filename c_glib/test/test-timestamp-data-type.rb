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

class TestTimestampDataType < Test::Unit::TestCase
  def test_type
    data_type = Arrow::TimestampDataType.new(:micro)
    assert_equal(Arrow::Type::TIMESTAMP, data_type.id)
  end

  def test_name
    data_type = Arrow::TimestampDataType.new(:micro)
    assert_equal("timestamp", data_type.name)
  end

  sub_test_case("time_zone") do
    def test_nil
      data_type = Arrow::TimestampDataType.new(:micro)
      assert_nil(data_type.time_zone)
    end

    def test_time_zone
      data_type = Arrow::TimestampDataType.new(:micro, GLib::TimeZone.new("UTC"))
      time_zone = data_type.time_zone
      assert_not_nil(time_zone)
      # glib2 gem 4.2.1 or later is required
      if time_zone.respond_to?(:identifier)
        assert_equal("UTC", time_zone.identifier)
      end
    end
  end

  sub_test_case("second") do
    def setup
      @data_type = Arrow::TimestampDataType.new(:second)
    end

    def test_to_s
      assert_equal("timestamp[s]", @data_type.to_s)
    end

    def test_unit
      assert_equal(Arrow::TimeUnit::SECOND, @data_type.unit)
    end
  end

  sub_test_case("millisecond") do
    def setup
      @data_type = Arrow::TimestampDataType.new(:milli)
    end

    def test_to_s
      assert_equal("timestamp[ms]", @data_type.to_s)
    end

    def test_unit
      assert_equal(Arrow::TimeUnit::MILLI, @data_type.unit)
    end
  end

  sub_test_case("micro") do
    def setup
      @data_type = Arrow::TimestampDataType.new(:micro)
    end

    def test_to_s
      assert_equal("timestamp[us]", @data_type.to_s)
    end

    def test_unit
      assert_equal(Arrow::TimeUnit::MICRO, @data_type.unit)
    end
  end

  sub_test_case("nano") do
    def setup
      @data_type = Arrow::TimestampDataType.new(:nano)
    end

    def test_to_s
      assert_equal("timestamp[ns]", @data_type.to_s)
    end

    def test_unit
      assert_equal(Arrow::TimeUnit::NANO, @data_type.unit)
    end
  end
end
