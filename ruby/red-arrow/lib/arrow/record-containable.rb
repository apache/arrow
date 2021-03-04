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
  module RecordContainable
    def each_record(reuse_record: false)
      unless block_given?
        return to_enum(__method__, reuse_record: reuse_record)
      end

      if reuse_record
        record = Record.new(self, nil)
        n_rows.times do |i|
          record.index = i
          yield(record)
        end
      else
        n_rows.times do |i|
          yield(Record.new(self, i))
        end
      end
    end
  end
end
