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
  class ListSliceOptions
    alias_method :return_fixed_size_list_raw, :return_fixed_size_list
    private :return_fixed_size_list_raw

    RETURN_FIXED_SIZE_GLIB_TO_RUBY = {
      ListSliceReturnFixedSizeList::AUTO => nil,
      ListSliceReturnFixedSizeList::TRUE => true,
      ListSliceReturnFixedSizeList::FALSE => false,
    }.freeze

    RETURN_FIXED_SIZE_RUBY_TO_GLIB = RETURN_FIXED_SIZE_GLIB_TO_RUBY.invert.freeze

    # Whether to return a FixedSizeListArray. If true _and_ stop is after a
    # list element’s length, nil values will be appended to create the requested
    # slice size. The default of nil will return the same type which was passed in.
    def return_fixed_size_list
      RETURN_FIXED_SIZE_GLIB_TO_RUBY.fetch(
        return_fixed_size_list_raw,
        return_fixed_size_list_raw)
    end

    # Whether to return a FixedSizeListArray. If true _and_ stop is after a
    # list element’s length, nil values will be appended to create the requested
    # slice size. The default of nil will return the same type which was passed in.
    def return_fixed_size_list=(return_fixed_size_list)
      set_property(
        :return_fixed_size_list,
        RETURN_FIXED_SIZE_RUBY_TO_GLIB.fetch(
          return_fixed_size_list,
          return_fixed_size_list))
    end

    alias_method :stop_raw, :stop
    private :stop_raw

    # Optional stop of list slicing. If set to nil, then slice to end.
    def stop
      stop_raw == LIST_SLICE_OPTIONS_STOP_UNSPECIFIED ? nil : stop_raw
    end

    # Optional stop of list slicing. If set to nil, then slice to end.
    def stop=(stop)
      set_property(:stop, stop.nil? ? LIST_SLICE_OPTIONS_STOP_UNSPECIFIED : stop)
    end
  end
end
