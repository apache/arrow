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

module ArrowFormat
  class UInt8Array
    attr_reader :size
    def initialize(size, validity_buffer, values_buffer)
      @size = size
      @validity_buffer = validity_buffer
      @values_buffer = values_buffer
    end

    def to_a
      # TODO: Check @validity_buffer
      @values_buffer.values(:U8, 0, @size)
    end
  end
end
