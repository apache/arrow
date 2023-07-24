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
  class HalfFloat
    MAX = 65504
    MIN = -65504
    EXPONENT_N_BITS = 5
    EXPONENT_MASK = (2 ** EXPONENT_N_BITS) - 1
    EXPONENT_BIAS = 15
    FRACTION_N_BITS = 10
    FRACTION_MASK = (2 ** FRACTION_N_BITS) - 1
    FRACTION_DENOMINATOR = 2.0 ** FRACTION_N_BITS

    attr_reader :sign
    attr_reader :exponent
    attr_reader :fraction
    def initialize(*args)
      n_args = args.size
      case n_args
      when 1
        if args[0].is_a?(Float)
          @sign, @exponent, @fraction = deconstruct_float(args[0])
        else
          @sign, @exponent, @fraction = deconstruct_uint16(args[0])
        end
      when 3
        @sign, @exponent, @fraction = *args
      else
        message = "wrong number of arguments (given #{n_args}, expected 1 or 3)"
        raise ArgumentError, message
      end
    end

    def to_f
      if @exponent == EXPONENT_MASK
        if @sign.zero?
          Float::INFINITY
        else
          -Float::INFINITY
        end
      else
        if @exponent.zero?
          implicit_fraction = 0
        else
          implicit_fraction = 1
        end
        ((-1) ** @sign) *
          (2 ** (@exponent - EXPONENT_BIAS)) *
          (implicit_fraction + @fraction / FRACTION_DENOMINATOR)
      end
    end

    def to_uint16
      (@sign << (EXPONENT_N_BITS + FRACTION_N_BITS)) ^
        (@exponent << FRACTION_N_BITS) ^
        @fraction
    end

    def pack
      [to_uint16].pack("S")
    end

    private
    def deconstruct_float(float)
      if float > MAX
        float = Float::INFINITY
      elsif float < MIN
        float = -Float::INFINITY
      end
      is_infinite = float.infinite?
      if is_infinite
        sign = (is_infinite == 1) ? 0 : 1
        exponent = EXPONENT_MASK
        fraction = 0
      elsif float.zero?
        sign = 0
        exponent = 0
        fraction = 0
      else
        sign = (float.positive? ? 0 : 1)
        float_abs = float.abs
        1.upto(EXPONENT_MASK) do |e|
          next_exponent_value = 2 ** (e + 1 - EXPONENT_BIAS)
          next if float_abs > next_exponent_value
          exponent = e
          exponent_value = 2 ** (e - EXPONENT_BIAS)
          fraction =
            ((float_abs / exponent_value - 1) * FRACTION_DENOMINATOR).round
          break
        end
      end
      [sign, exponent, fraction]
    end

    def deconstruct_uint16(uint16)
      # | sign (1 bit) | exponent (5 bit) | fraction (10 bit) |
      sign = (uint16 >> (EXPONENT_N_BITS + FRACTION_N_BITS))
      exponent = ((uint16 >> FRACTION_N_BITS) & EXPONENT_MASK)
      fraction = (uint16 & FRACTION_MASK)
      [sign, exponent, fraction]
    end
  end
end
