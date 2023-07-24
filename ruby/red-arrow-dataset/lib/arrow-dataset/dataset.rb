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

module ArrowDataset
  class Dataset
    class << self
      def build(*args)
        factory_class = ArrowDataset.const_get("#{name}Factory")
        factory = factory_class.new(*args)
        options = yield(factory)
        unless options.is_a?(FinishOptions)
          options = FinishOptions.try_convert(options)
        end
        factory.finish(options)
      end
    end
  end
end
