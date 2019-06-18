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
  class Decimal128
    alias_method :to_s_raw, :to_s

    # @overload to_s
    #
    #   @return [String]
    #     The string representation of the decimal.
    #
    # @overload to_s(scale)
    #
    #   @param scale [Integer] The scale of the decimal.
    #   @return [String]
    #      The string representation of the decimal including the scale.
    #
    # @since 0.13.0
    def to_s(scale=nil)
      if scale
        to_string_scale(scale)
      else
        to_s_raw
      end
    end
  end
end
