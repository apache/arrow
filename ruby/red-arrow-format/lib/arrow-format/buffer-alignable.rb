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

require_relative "flatbuffers"

module ArrowFormat
  module BufferAlignable
    include FlatBuffers::Alignable

    BUFFER_ALIGNMENT_SIZE = 64

    private
    def buffer_padding_size(buffer)
      compute_padding_size(buffer.size, BUFFER_ALIGNMENT_SIZE)
    end

    def aligned_buffer_size(buffer)
      buffer.size + buffer_padding_size(buffer)
    end
  end
end
