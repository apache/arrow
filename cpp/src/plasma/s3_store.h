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

#ifndef S3_STORE_H
#define S3_STORE_H

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <memory>
#include <string>
#include <vector>

#include "plasma/external_store.h"

namespace plasma {

// This provides the implementation of S3 as an external store to plasma
// Note: Populate the AWS_REGION environment variable to set the region for the s3
// bucket (the default is us-east-1).

class S3Store : public ExternalStore {
 public:
  S3Store();
  ~S3Store() override;
  Status Connect(const std::string& endpoint) override;
  Status Put(const std::vector<ObjectID>& ids,
             const std::vector<std::shared_ptr<Buffer>>& data) override;
  Status Get(const std::vector<ObjectID>& ids,
             std::vector<std::shared_ptr<Buffer>> buffers) override;

 private:
  Status ExtractBucketAndKeyPrefix(const std::string& endpoint);

  Aws::String bucket_name_;
  Aws::String key_prefix_;
  std::shared_ptr<Aws::S3::S3Client> client_;
  Aws::SDKOptions options_;
  Aws::Client::ClientConfiguration config_;
};

}  // namespace plasma

#endif  // PLASMA_S3_STORE_H
