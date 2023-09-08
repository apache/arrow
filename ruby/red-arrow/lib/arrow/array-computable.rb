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
  module ArrayComputable
    def min(options: nil)
      compute("min", options: options).value
    end

    def max(options: nil)
      compute("max", options: options).value
    end

    def uniq
      unique.values
    end

    # Finds the index of the first occurrence of a given value.
    #
    # @param value [Object] The value to be compared.
    #
    # @return [Integer] The index of the first occurrence of a given
    #   value on found, -1 on not found.
    #
    # @since 12.0.0
    def index(value)
      value = Scalar.resolve(value, value_data_type)
      compute("index", options: {value: value}).value
    end

    private
    def compute(name, options: nil)
      Function.find(name).execute([self], options).value
    end
  end
end
