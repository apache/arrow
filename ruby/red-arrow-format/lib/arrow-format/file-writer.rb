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

require_relative "streaming-writer"

module ArrowFormat
  class FileWriter < StreamingWriter
    MAGIC = "ARROW1".b
    MAGIC_PADDING = "\x00\x00"

    def start(schema)
      @fb_schema = schema.to_flatbuffers
      write_data(MAGIC)
      write_data(MAGIC_PADDING)
      super
    end

    def finish
      super
      write_footer
      write_data(MAGIC)
      @output
    end

    private
    def build_footer
      fb_footer = FB::Footer::Data.new
      fb_footer.version = FB::MetadataVersion::V5
      fb_footer.schema = @fb_schema
      # fb_footer.dictionaries = ... # TODO
      fb_footer.record_batches = @fb_record_batch_blocks
      # fb_footer.custom_metadata = ... # TODO
      FB::Footer.serialize(fb_footer)
    end

    def write_footer
      footer = build_footer
      write_data(footer)
      write_data([footer.bytesize].pack("l<"))
    end
  end
end
