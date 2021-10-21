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

#include "arrow/filesystem/gcsfs_internal.h"

#include <google/cloud/storage/client.h>

#include <sstream>

namespace arrow {
namespace fs {
namespace internal {

Status ToArrowStatus(const google::cloud::Status& s) {
  std::ostringstream os;
  os << "google::cloud::Status(" << s << ")";
  switch (s.code()) {
    case google::cloud::StatusCode::kOk:
      break;
    case google::cloud::StatusCode::kCancelled:
      return Status::Cancelled(os.str());
    case google::cloud::StatusCode::kUnknown:
      return Status::UnknownError(os.str());
    case google::cloud::StatusCode::kInvalidArgument:
      return Status::Invalid(os.str());
    case google::cloud::StatusCode::kDeadlineExceeded:
    case google::cloud::StatusCode::kNotFound:
      return Status::IOError(os.str());
    case google::cloud::StatusCode::kAlreadyExists:
      return Status::AlreadyExists(os.str());
    case google::cloud::StatusCode::kPermissionDenied:
    case google::cloud::StatusCode::kUnauthenticated:
      return Status::IOError(os.str());
    case google::cloud::StatusCode::kResourceExhausted:
      return Status::CapacityError(os.str());
    case google::cloud::StatusCode::kFailedPrecondition:
    case google::cloud::StatusCode::kAborted:
      return Status::IOError(os.str());
    case google::cloud::StatusCode::kOutOfRange:
      return Status::Invalid(os.str());
    case google::cloud::StatusCode::kUnimplemented:
      return Status::NotImplemented(os.str());
    case google::cloud::StatusCode::kInternal:
    case google::cloud::StatusCode::kUnavailable:
    case google::cloud::StatusCode::kDataLoss:
      return Status::IOError(os.str());
  }
  return Status::OK();
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow
