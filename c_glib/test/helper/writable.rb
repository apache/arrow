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

module Helper
  module Writable
    def write_table(table, path, type: :file)
      output = Arrow::FileOutputStream.new(path, false)
      begin
        if type == :file
          writer_class = Arrow::RecordBatchFileWriter
        else
          writer_class = Arrow::RecordBatchStreamWriter
        end
        writer = writer_class.new(output, table.schema)
        begin
          writer.write_table(table)
        ensure
          writer.close
        end
      ensure
        output.close
      end
    end
  end
end
