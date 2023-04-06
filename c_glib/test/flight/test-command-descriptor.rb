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

class TestFlightCommandDescriptor < Test::Unit::TestCase
  def setup
    omit("Arrow Flight is required") unless defined?(ArrowFlight)
  end

  def test_to_s
    descriptor = ArrowFlight::CommandDescriptor.new("command")
    assert_equal("<FlightDescriptor cmd='command'>",
                 descriptor.to_s)
  end

  def test_command
    command = "command"
    descriptor = ArrowFlight::CommandDescriptor.new(command)
    assert_equal(command, descriptor.command)
  end

  sub_test_case("#==") do
    def test_true
      descriptor1 = ArrowFlight::CommandDescriptor.new("command")
      descriptor2 = ArrowFlight::CommandDescriptor.new("command")
      assert do
        descriptor1 == descriptor2
      end
    end

    def test_false
      descriptor1 = ArrowFlight::CommandDescriptor.new("command1")
      descriptor2 = ArrowFlight::CommandDescriptor.new("command2")
      assert do
        not (descriptor1 == descriptor2)
      end
    end
  end
end
