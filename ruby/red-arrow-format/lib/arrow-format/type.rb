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

module ArrowFormat
  class Type
    attr_reader :name
    def initialize(name)
      @name = name
    end
  end

  class NullType < Type
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("Null")
    end

    def build_array(size)
      NullArray.new(self, size)
    end
  end

  class BooleanType < Type
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("Boolean")
    end

    def build_array(size, validity_buffer, values_buffer)
      BooleanArray.new(self, size, validity_buffer, values_buffer)
    end
  end

  class IntType < Type
    attr_reader :bit_width
    attr_reader :signed
    def initialize(name, bit_width, signed)
      super(name)
      @bit_width = bit_width
      @signed = signed
    end
  end

  class Int8Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("Int8", 8, true)
    end

    def build_array(size, validity_buffer, values_buffer)
      Int8Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class UInt8Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("UInt8", 8, false)
    end

    def build_array(size, validity_buffer, values_buffer)
      UInt8Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class BinaryType < Type
    class << self
      def singleton
        @singleton ||= new
      end
    end

    attr_reader :name
    def initialize
      super("Binary")
    end

    def build_array(size, validity_buffer, offsets_buffer, values_buffer)
      BinaryArray.new(self, size, validity_buffer, offsets_buffer, values_buffer)
    end
  end

  class UTF8Type < Type
    class << self
      def singleton
        @singleton ||= new
      end
    end

    attr_reader :name
    def initialize
      super("UTF8")
    end

    def build_array(size, validity_buffer, offsets_buffer, values_buffer)
      UTF8Array.new(self, size, validity_buffer, offsets_buffer, values_buffer)
    end
  end
end
