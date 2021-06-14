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
  class Scalar
    # @param other [Arrow::Scalar] The scalar to be compared.
    # @param options [Arrow::EqualOptions, Hash] (nil)
    #   The options to custom how to compare.
    #
    # @return [Boolean]
    #   `true` if both of them have the same data, `false` otherwise.
    #
    # @since 5.0.0
    def equal_scalar?(other, options=nil)
      equal_options(other, options)
    end
  end
end
