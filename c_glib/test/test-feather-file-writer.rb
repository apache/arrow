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

class TestFeatherFileWriter < Test::Unit::TestCase
  include Helper::Buildable

  def test_write
    messages = build_string_array(["Crash", "Error", "Shutdown"])
    is_criticals = build_boolean_array([true, true, false])
    table = build_table("message" => messages,
                        "is_critical" => is_criticals)

    tempfile = Tempfile.open("arrow-feather-file-writer")

    output = Arrow::FileOutputStream.new(tempfile.path, false)
    Arrow::feather_write_file(table, output)
    output.close

    input = Arrow::MemoryMappedInputStream.new(tempfile.path)
    reader = Arrow::FeatherFileReader.new(input)
    assert_equal(table, reader.read)
    input.close
  end
end
