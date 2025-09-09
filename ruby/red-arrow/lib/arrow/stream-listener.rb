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
  class StreamListener < StreamListenerRaw
    type_register

    def on_eos
    end

    def on_record_batch_decoded(record_batch, metadata)
    end

    def on_schema(schema, filtered_schema)
    end

    private
    def virtual_do_on_eos
      on_eos
      true
    end

    def virtual_do_on_record_batch_decoded(record_batch, metadata)
      on_record_batch_decoded(record_batch, metadata)
      true
    end

    def virtual_do_on_schema_decoded(schema, filtered_schema)
      on_schema_decoded(schema, filtered_schema)
      true
    end
  end
end
