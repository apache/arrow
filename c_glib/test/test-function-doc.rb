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

class TestFunctionDoc < Test::Unit::TestCase
  def setup
    @doc = Arrow::Function.find("or").doc
  end

  def test_summary
    assert_equal("Logical 'or' boolean values",
                 @doc.summary)
  end

  def test_description
    assert_equal(<<-DESCRIPTION.chomp, @doc.description)
When a null is encountered in either input, a null is output.
For a different null behavior, see function "or_kleene".
    DESCRIPTION
  end

  def test_arg_names
    assert_equal(["x", "y"], @doc.arg_names)
  end

  def test_options_class_name
    doc = Arrow::Function.find("cast").doc
    assert_equal("CastOptions", doc.options_class_name)
  end
end
