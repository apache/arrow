// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/compression_internal.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/plugin.h"
#include "arrow/util/plugin_manager.h"

namespace arrow {
namespace util {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// plugin codec implementation

class PluginCodec : public Codec {
 public:
  explicit PluginCodec(int compression_level, std::string plugin_name)
      : plugin_name_(plugin_name), plugin_(nullptr) {
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kGZipDefaultCompressionLevel
                             : compression_level;
  }

  Status Init() override {
    ARROW_ASSIGN_OR_RAISE(auto plugin_manager, PluginManager::GetPluginManager());
    ARROW_ASSIGN_OR_RAISE(plugin_, plugin_manager->GetPlugin(plugin_name_));
    return Status::OK();
  }

  Result<int64_t> Decompress(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_length, uint8_t* output) override {
    ArrowPluginCompressionContext* context =
        static_cast<ArrowPluginCompressionContext*>(plugin_->GetPluginPrivContext());
    int64_t decompressed_size =
        context->decompressCallback(input_length, input, output_buffer_length, output);
    if (decompressed_size == 0) {
      return Status::IOError("");
    } else {
      return decompressed_size;
    }
  }

  int64_t MaxCompressedLen(int64_t input_length, const uint8_t* input) override {
    ArrowPluginCompressionContext* context =
        static_cast<ArrowPluginCompressionContext*>(plugin_->GetPluginPrivContext());
    return context->maxCompressedLenCallback(input_length, input);
  }

  Result<int64_t> Compress(int64_t input_length, const uint8_t* input,
                           int64_t output_buffer_length, uint8_t* output) override {
    ArrowPluginCompressionContext* context =
        static_cast<ArrowPluginCompressionContext*>(plugin_->GetPluginPrivContext());
    int64_t compressed_size =
        context->compressCallback(input_length, input, output_buffer_length, output);
    return compressed_size;
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented("Streaming compression unsupported with plugin");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented("Streaming decompression unsupported with plugin");
  }

  const char* name() const override {
    ArrowPluginCompressionContext* context =
        static_cast<ArrowPluginCompressionContext*>(plugin_->GetPluginPrivContext());
    return context->getNameCallback();
  }

 private:
  std::string plugin_name_;
  int compression_level_;
  std::shared_ptr<Plugin> plugin_;
};

}  // namespace

std::unique_ptr<Codec> MakePluginCodec(std::string plugin_name, int compression_level) {
  return std::unique_ptr<Codec>(new PluginCodec(compression_level, plugin_name));
}

}  // namespace internal
}  // namespace util
}  // namespace arrow
