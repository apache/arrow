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

class TestMemoryPool < Test::Unit::TestCase
  include Helper::Omittable

  def setup
    @memory_pool = Arrow::MemoryPool.default
  end

  def test_bytes_allocated
    assert do
      @memory_pool.bytes_allocated.positive?
    end
  end

  def test_max_memory
    assert do
      @memory_pool.max_memory.positive?
    end
  end

  def test_backend_name
    assert do
      %w[system jemalloc mimalloc].include?(@memory_pool.backend_name)
    end
  end
end
