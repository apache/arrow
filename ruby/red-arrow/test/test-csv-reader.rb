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

class CSVReaderTest < Test::Unit::TestCase
  include Helper::Fixture

  test("#read") do
    CSV.open(fixture_path("with-header.csv").to_s,
             headers: true,
             skip_lines: /^#/) do |csv|
      reader = Arrow::CSVReader.new(csv)
      assert_equal(<<-TABLE, reader.read.to_s)
	name	score
0	alice	10   
1	bob 	29   
2	chris	-1   
      TABLE
    end
  end
end
