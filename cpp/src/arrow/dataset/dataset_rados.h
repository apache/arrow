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

// This API is EXPERIMENTAL.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/macros.h"
#include "arrow/util/mutex.h"
#include "arrow/dataset/dataset.h"

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT RadosFragment : public Fragment {
  public:
    RadosFragment(std::shared_ptr<Schema> schema, std::string object_id);

    Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                  std::shared_ptr<ScanContext> context) override;

    bool splittable() const override { return false; }

    std::string type_name() const override { return "rados"; }

  protected:
    Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override;
    std::string object_id_ = "";
};

class ARROW_DS_EXPORT RadosDataset : public Dataset {
 public:
  FragmentIterator GetFragments();

  RadosDataset(std::shared_ptr<Schema> schema, std::string ceph_config_path, std::string pool_name);

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  std::string type_name() const override { return "rados"; }
  
  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

  virtual ~RadosDataset() = default;

 protected:
  std::string ceph_config_path_;
  std::string pool_name_;
};

class ARROW_DS_EXPORT RadosScanTask : public ScanTask {
  public: 
    RadosScanTask(std::string object_id, 
                  std::shared_ptr<ScanOptions> options, 
                  std::shared_ptr<ScanContext> context)
      : ScanTask(std::move(options), std::move(context)),
        object_id_(std::move(object_id)) {}

    Result<RecordBatchIterator> Execute() override;

  protected:
    std::string object_id_;
};

}  // namespace dataset
}  // namespace arrow
