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
  class SortKey
    class << self
      # Ensure returning suitable {Arrow::SortKey}.
      #
      # @overload resolve(sort_key)
      #
      #   Returns the given sort key itself. This is convenient to use
      #   this method as {Arrow::SortKey} converter.
      #
      #   @param sort_key [Arrow::SortKey] The sort key.
      #
      #   @return [Arrow::SortKey] The given sort key itself.
      #
      # @overload resolve(target)
      #
      #   Creates a new suitable sort key from column name or dot path
      #   with leading order mark. See {#initialize} for details about
      #   order mark.
      #
      #   @return [Arrow::SortKey] A new suitable sort key.
      #
      # @overload resolve(target, order)
      #
      #   Creates a new suitable sort key from column name or dot path
      #   without leading order mark and order. See {#initialize} for
      #   details.
      #
      #   @return [Arrow::SortKey] A new suitable sort key.
      #
      # @since 4.0.0
      def resolve(target, order=nil)
        return target if target.is_a?(self)
        new(target, order)
      end

      # @api private
      def try_convert(value)
        case value
        when Symbol, String
          new(value.to_s, :ascending)
        else
          nil
        end
      end
    end

    alias_method :initialize_raw, :initialize
    private :initialize_raw
    # Creates a new {Arrow::SortKey}.
    #
    # @overload initialize(target)
    #
    #   @param target [Symbol, String] The name or dot path of the
    #     sort column.
    #
    #     If `target` is a String, the first character may be
    #     processed as the "leading order mark". If the first
    #     character is `"+"` or `"-"`, they are processed as a leading
    #     order mark. If the first character is processed as a leading
    #     order mark, the first character is removed from sort column
    #     target and corresponding order is used. `"+"` uses ascending
    #     order and `"-"` uses ascending order.
    #
    #     If `target` is either not a String or `target` doesn't start
    #     with the leading order mark, sort column is `target` as-is
    #     and ascending order is used.
    #
    #   @example String without the leading order mark
    #     key = Arrow::SortKey.new("count")
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::ASCENDING
    #
    #   @example String with the "+" leading order mark
    #     key = Arrow::SortKey.new("+count")
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::ASCENDING
    #
    #   @example String with the "-" leading order mark
    #     key = Arrow::SortKey.new("-count")
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::DESCENDING
    #
    #   @example Symbol that starts with "-"
    #     key = Arrow::SortKey.new(:"-count")
    #     key.target # => "-count"
    #     key.order  # => Arrow::SortOrder::ASCENDING
    #
    # @overload initialize(target, order)
    #
    #   @param target [Symbol, String] The name or dot path of the
    #     sort column.
    #
    #     No leading order mark processing. The given `target` is used
    #     as-is.
    #
    #   @param order [Symbol, String, Arrow::SortOrder] How to order
    #     by this sort key.
    #
    #     If this is a Symbol or String, this must be `:ascending`,
    #     `"ascending"`, `:asc`, `"asc"`, `:descending`,
    #     `"descending"`, `:desc` or `"desc"`.
    #
    #   @example No leading order mark processing
    #     key = Arrow::SortKey.new("-count", :ascending)
    #     key.target # => "-count"
    #     key.order  # => Arrow::SortOrder::ASCENDING
    #
    #   @example Order by abbreviated target with Symbol
    #     key = Arrow::SortKey.new("count", :desc)
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::DESCENDING
    #
    #   @example Order by String
    #     key = Arrow::SortKey.new("count", "descending")
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::DESCENDING
    #
    #   @example Order by Arrow::SortOrder
    #     key = Arrow::SortKey.new("count", Arrow::SortOrder::DESCENDING)
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::DESCENDING
    #
    # @since 4.0.0
    def initialize(target, order=nil)
      target, order = normalize_target(target, order)
      order = normalize_order(order) || :ascending
      initialize_raw(target, order)
    end

    # @return [String] The string representation of this sort key. You
    #   can use recreate {Arrow::SortKey} by
    #   `Arrow::SortKey.new(key.to_s)`.
    #
    # @example Recreate Arrow::SortKey
    #   key = Arrow::SortKey.new("-count")
    #   key.to_s # => "-count"
    #   key == Arrow::SortKey.new(key.to_s) # => true
    #
    # @since 4.0.0
    def to_s
      if order == SortOrder::ASCENDING
        "+#{target}"
      else
        "-#{target}"
      end
    end

    # For backward compatibility
    alias_method :name, :target

    private
    def normalize_target(target, order)
      case target
      when Symbol
        return target.to_s, order
      when String
        return target, order if order
        if target.start_with?("-")
          return target[1..-1], order || :descending
        elsif target.start_with?("+")
          return target[1..-1], order || :ascending
        else
          return target, order
        end
      else
        return target, order
      end
    end

    def normalize_order(order)
      case order
      when :asc, "asc"
        :ascending
      when :desc, "desc"
        :descending
      else
        order
      end
    end
  end
end
