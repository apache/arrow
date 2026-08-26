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

module ArrowFormat
  module Integration
    class Options
      class << self
        def singleton
          @singleton ||= new
        end
      end

      attr_reader :command
      attr_reader :arrow
      attr_reader :arrows
      attr_reader :json
      def initialize
        @command = ENV["COMMAND"]
        @arrow = ENV["ARROW"]
        @arrows = ENV["ARROWS"]
        @json = ENV["JSON"]
        @validate_date64 = ENV["QUIRK_NO_DATE64_VALIDATE"] != "true"
        @validate_decimal = ENV["QUIRK_NO_DECIMAL_VALIDATE"] != "true"
        @validate_time = ENV["QUIRK_NO_TIMES_VALIDATE"] != "true"
      end

      def validate_date64?
        @validate_date64
      end

      def validate_decimal?
        @validate_decimal
      end

      def validate_time?
        @validate_time
      end
    end
  end
end
