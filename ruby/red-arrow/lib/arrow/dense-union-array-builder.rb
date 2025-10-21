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
  class DenseUnionArrayBuilder
    alias_method :append_value_raw, :append_value

    # @overload append_value
    #
    #   Starts appending an union record. You need to append values of
    #   fields.
    #
    # @overload append_value(value)
    #
    #   Appends an union record including values of fields.
    #
    #   @param value [nil, Hash] The union record value.
    #
    #     If this is `nil`, the union record is null.
    #
    #     If this is `Hash`, it's  values of fields.
    #
    # @since 12.0.0
    def append_value(value)
      if value.nil?
        append_null
      else
        key = value.keys[0]
        child_info = child_infos[key]
        append_value_raw(child_info[:id])
        child_info[:builder].append(value.values[0])
      end
    end
  end
end
