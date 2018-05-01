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

module Arrow
  class Column
    include Enumerable

    def null?(i)
      data.null?(i)
    end

    def valid?(i)
      data.valid?(i)
    end

    def [](i)
      data[i]
    end

    def each(&block)
      return to_enum(__method__) unless block_given?

      data.each(&block)
    end

    def reverse_each(&block)
      return to_enum(__method__) unless block_given?

      data.reverse_each(&block)
    end

    def pack
      self.class.new(field, data.pack)
    end
  end
end
