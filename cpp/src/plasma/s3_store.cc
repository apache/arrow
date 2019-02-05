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

#include <aws/core/utils/stream/SimpleStreamBuf.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <string>

#include "arrow/util/logging.h"
#include "plasma/s3_store.h"

namespace plasma {

static Aws::String GetRegion() {
  auto region = std::getenv("AWS_REGION");
  if (region == nullptr) {
    return Aws::String("us-east-1");  // Default region
  }
  return Aws::String(region);
}

Status S3Store::ExtractBucketAndKeyPrefix(const std::string& endpoint) {
  std::string separator = ":/";
  std::size_t pos = endpoint.find(separator);
  if (pos == std::string::npos) {
    return Status::KeyError("Malformed endpoint " + endpoint);
  }
  // Decompose endpoint into URI (s3) and path elements
  std::string uri = endpoint.substr(0, pos);
  std::size_t path_pos = pos + separator.length();
  std::size_t path_len = endpoint.length() - separator.length() - uri.length();
  std::string path = endpoint.substr(path_pos, path_len);

  // Decompose path into bucket and key-prefix elements
  auto bucket_end = std::find(path.begin() + 1, path.end(), '/');
  bucket_name_ = Aws::String(path.begin() + 1, bucket_end);
  key_prefix_ = bucket_end == path.end() ? "" : Aws::String(bucket_end + 1, path.end());
  return Status::OK();
}

S3Store::S3Store() { Aws::InitAPI(options_); }

S3Store::~S3Store() { Aws::ShutdownAPI(options_); }

Status S3Store::Connect(const std::string& endpoint) {
  RETURN_NOT_OK(ExtractBucketAndKeyPrefix(endpoint));
  ARROW_LOG(INFO) << "Connecting to s3 bucket \"" << bucket_name_
                  << "\" with key-prefix \"" << key_prefix_ << "\"";
  config_.region = GetRegion();
  client_ = std::make_shared<Aws::S3::S3Client>(config_);
  return Status::OK();
}

Status S3Store::Put(const std::vector<ObjectID>& ids,
                    const std::vector<std::shared_ptr<Buffer>>& data) {
  std::vector<Aws::S3::Model::PutObjectOutcomeCallable> put_callables;
  for (size_t i = 0; i < ids.size(); ++i) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucket_name_).WithKey(key_prefix_ + ids[i].hex().data());
    auto objectStream = Aws::MakeShared<Aws::StringStream>("DataStream");
    objectStream->write(reinterpret_cast<const char*>(data[i]->data()), data[i]->size());
    objectStream->flush();
    request.SetBody(objectStream);
    put_callables.push_back(client_->PutObjectCallable(request));
  }

  std::string err_msg;
  for (auto& put_callable : put_callables) {
    auto outcome = put_callable.get();
    if (!outcome.IsSuccess())
      err_msg += std::string(outcome.GetError().GetMessage().data()) + "\n";
  }
  return err_msg.empty() ? Status::OK() : Status::IOError(err_msg);
}

Status S3Store::Get(const std::vector<ObjectID>& ids,
                    std::vector<std::shared_ptr<Buffer>> buffers) {
  buffers.resize(ids.size());
  std::vector<Aws::S3::Model::GetObjectOutcomeCallable> get_callables;
  for (const auto& id : ids) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name_).WithKey(key_prefix_ + id.hex().data());
    get_callables.push_back(client_->GetObjectCallable(request));
  }

  std::string err_msg;
  for (size_t i = 0; i < get_callables.size(); ++i) {
    auto outcome = get_callables[i].get();
    if (!outcome.IsSuccess())
      err_msg += std::string(outcome.GetError().GetMessage().data()) + "\n";
    auto in = std::make_shared<Aws::IOStream>(outcome.GetResult().GetBody().rdbuf());
    std::string object_data((std::istreambuf_iterator<char>(*in)),
                            (std::istreambuf_iterator<char>()));
    std::memcpy(buffers[i]->mutable_data(), object_data.data(), object_data.size());
  }
  return err_msg.empty() ? Status::OK() : Status::IOError(err_msg);
}

REGISTER_EXTERNAL_STORE("s3", S3Store);

}  // namespace plasma
