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
  class SortOptions
    class << self
      # @api private
      def try_convert(value)
        case value
        when Symbol, String
          new(value)
        when ::Array
          new(*value)
        else
          nil
        end
      end
    end

    alias_method :initialize_raw, :initialize
    private :initialize_raw
    # @param sort_keys [::Array<String, Symbol, Arrow::SortKey>] The
    #   sort keys to be used. See {Arrow::SortKey.resolve} how to
    #   resolve each sort key in `sort_keys`.
    #
    #   You can add more sort keys by {#add_sort_key} later.
    #
    # @example No initial sort keys
    #   options = Arrow::SortOptions.new
    #   options.sort_keys # => []
    #
    # @example String sort keys
    #   options = Arrow::SortOptions.new("count", "-age")
    #   options.sort_keys.collect(&:to_s) # => ["+count", "-age"]
    #
    # @example Symbol sort keys
    #   options = Arrow::SortOptions.new(:count, :age)
    #   options.sort_keys.collect(&:to_s) # => ["+count", "+age"]
    #
    # @example Mixed sort keys
    #   options = Arrow::SortOptions.new(:count, "-age")
    #   options.sort_keys.collect(&:to_s) # => ["+count", "-age"]
    #
    # @since 4.0.0
    def initialize(*sort_keys)
      initialize_raw
      sort_keys.each do |sort_key|
        add_sort_key(sort_key)
      end
    end

    # @api private
    alias_method :add_sort_key_raw, :add_sort_key
    # Add a sort key.
    #
    # @return [void]
    #
    # @overload add_sort_key(key)
    #
    #   @param key [Arrow::SortKey] The sort key to be added.
    #
    #   @example Add a key to sort by "price" column in descending order
    #     options = Arrow::SortOptions.new
    #     options.add_sort_key(Arrow::SortKey.new(:price, :descending))
    #     options.sort_keys.collect(&:to_s) # => ["-price"]
    #
    # @overload add_sort_key(target)
    #
    #   @param target [Symbol, String] The sort key name or dot path
    #     to be added. See also {Arrow::SortKey#initialize} for the
    #     leading order mark for `String` target.
    #
    #   @example Add a key to sort by "price" column in descending order
    #     options = Arrow::SortOptions.new
    #     options.add_sort_key("-price")
    #     options.sort_keys.collect(&:to_s) # => ["-price"]
    #
    # @overload add_sort_key(target, order)
    #
    #   @param target [Symbol, String] The sort key name or dot path.
    #
    #   @param order [Symbol, String, Arrow::SortOrder] The sort
    #     order. See {Arrow::SortKey#initialize} for details.
    #
    #   @example Add a key to sort by "price" column in descending order
    #     options = Arrow::SortOptions.new
    #     options.add_sort_key("price", :desc)
    #     options.sort_keys.collect(&:to_s) # => ["-price"]
    #
    # @since 4.0.0
    def add_sort_key(target, order=nil)
      add_sort_key_raw(SortKey.resolve(target, order))
    end
  end
end
