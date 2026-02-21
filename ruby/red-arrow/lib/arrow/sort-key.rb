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
      def resolve(target, order=nil, null_placement=nil)
        return target if target.is_a?(self)
        new(target, order, null_placement)
      end

      # @api private
      def try_convert(value)
        case value
        when Symbol, String
          new(value.to_s, :ascending, :at_end)
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
    #     If `target` is a String, it may have prefix markers that specify
    #     the sort order and null placement. The format is `[+/-][^/$]column`:
    #
    #     - `"+"` prefix means ascending order
    #     - `"-"` prefix means descending order
    #     - `"^"` prefix means nulls at start
    #     - `"$"` prefix means nulls at end
    #
    #     If `target` is a Symbol, it is converted to String and used as-is
    #     (no prefix processing).
    #
    #   @example String without any prefix
    #     key = Arrow::SortKey.new("count")
    #     key.target         # => "count"
    #     key.order          # => Arrow::SortOrder::ASCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_END
    #
    #   @example String with order prefix only
    #     key = Arrow::SortKey.new("-count")
    #     key.target         # => "count"
    #     key.order          # => Arrow::SortOrder::DESCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_END
    #
    #   @example String with order and null placement prefixes
    #     key = Arrow::SortKey.new("-^count")
    #     key.target         # => "count"
    #     key.order          # => Arrow::SortOrder::DESCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_START
    #
    #   @example String with null placement prefix only
    #     key = Arrow::SortKey.new("^count")
    #     key.target         # => "count"
    #     key.order          # => Arrow::SortOrder::ASCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_START
    #
    #   @example Symbol (no prefix processing)
    #     key = Arrow::SortKey.new(:"-count")
    #     key.target         # => "-count"
    #     key.order          # => Arrow::SortOrder::ASCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_END
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
    #     key.null_placement # => Arrow::NullPlacement::AT_END
    #
    #   @example Order by abbreviated target with Symbol
    #     key = Arrow::SortKey.new("count", :desc)
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::DESCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_END
    #
    #   @example Order by String
    #     key = Arrow::SortKey.new("count", "descending")
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::DESCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_END
    #
    #   @example Order by Arrow::SortOrder, give null_placement with target
    #     key = Arrow::SortKey.new("^count", Arrow::SortOrder::DESCENDING)
    #     key.target # => "count"
    #     key.order  # => Arrow::SortOrder::DESCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_START
    #
    # @overload initialize(target, order, null_placement)
    #
    #   @param target [Symbol, String] The name or dot path of the
    #     sort column.
    #
    #   @param order [Symbol, String, Arrow::SortOrder] How to order
    #     by this sort key.
    #
    #     If this is a Symbol or String, this must be `:ascending`,
    #     `"ascending"`, `:asc`, `"asc"`, `:descending`,
    #     `"descending"`, `:desc` or `"desc"`.
    #
    #   @param null_placement [Symbol, String, Arrow::NullPlacement]
    #     Where to place nulls and NaNs. Must be `:at_start`, `"at_start"`,
    #     `:at_end`, or `"at_end"`.
    #
    #   @example With all explicit parameters
    #     key = Arrow::SortKey.new("count", :desc, :at_start)
    #     key.target         # => "count"
    #     key.order          # => Arrow::SortOrder::DESCENDING
    #     key.null_placement # => Arrow::NullPlacement::AT_START
    #
    # @since 4.0.0
    def initialize(target, order=nil, null_placement=nil)
      target, order, null_placement = normalize_target(target, order, null_placement)
      order = normalize_order(order) || :ascending
      null_placement = normalize_null_placement(null_placement) || :at_end
      initialize_raw(target, order, null_placement)
    end

    # @return [String] The string representation of this sort key. You
    #   can use recreate {Arrow::SortKey} by
    #   `Arrow::SortKey.new(key.to_s)`.
    #
    # @example Recreate Arrow::SortKey
    #   key = Arrow::SortKey.new("-count")
    #   key.to_s # => "-$count"
    #   key == Arrow::SortKey.new(key.to_s) # => true
    #
    # @since 4.0.0
    def to_s
      result = ""
      if order == SortOrder::ASCENDING
        result += "+"
      else
        result += "-"
      end
      if null_placement == NullPlacement::AT_START
        result += "^"
      else
        result += "$"
      end
      result += target
      result
    end

    # For backward compatibility
    alias_method :name, :target

    private
    # Parse prefix format: [+/-][^/$]column
    # Examples: -$column, +^column, ^column, -column
    #
    # Only strips prefixes if the corresponding parameter is not already set.
    # This preserves backward compatibility where specifying order explicitly
    # means the target is used as-is for order prefixes.
    def normalize_target(target, order, null_placement)
      case target
      when Symbol
        return target.to_s, order, null_placement
      when String
        remaining = target

        unless order
          if remaining.start_with?("-")
            order = :descending
            remaining = remaining[1..-1]
          elsif remaining.start_with?("+")
            order = :ascending
            remaining = remaining[1..-1]
          end
        end

        unless null_placement
          if remaining.start_with?("^")
            null_placement = :at_start
            remaining = remaining[1..-1]
          elsif remaining.start_with?("$")
            null_placement = :at_end
            remaining = remaining[1..-1]
          end
        end

        return remaining, order, null_placement
      else
        return target, order, null_placement
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
    
    def normalize_null_placement(null_placement)
      case null_placement
      when :at_end, "at_end"
        :at_end
      when :at_start, "at_start"
        :at_start
      else
        null_placement
      end
    end
  end
end
