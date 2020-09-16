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

#include "arrow/dataset/dataset_rados.h"

#include <memory>
#include <utility>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/table.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/dataset/librados_interface.h"

namespace arrow {
namespace dataset {

RadosFragment::RadosFragment(std::shared_ptr<Schema> schema, 
                             std::string object_id)
    : object_id_(std::move(object_id)) {}

Result<ScanTaskIterator> RadosFragment::Scan(std::shared_ptr<ScanOptions> options,
                                                std::shared_ptr<ScanContext> context) {
  auto ranges_it = MakeVectorIterator(std::vector<std::string>{object_id_});
  auto fn = [=](std::shared_ptr<std::string> object_id_) -> std::shared_ptr<ScanTask> {
    return ::arrow::internal::make_unique<RadosScanTask>(
        std::move(object_id_), std::move(options), std::move(context));
  };
  return MakeMapIterator(fn, std::move(ranges_it));
}

RadosDataset::RadosDataset(std::shared_ptr<Schema> schema, std::string pool_name, std::string ceph_config_path)
    : Dataset(std::move(schema)), pool_name_(std::move(pool_name), ceph_config_path_(ceph_config_path)) {

  // connect to the cluster
  RadosInterface rintf;
  rintf.init2("ceph", "client.admin", 0);
  rintf.conf_read_file(ceph_config_path);
  rintf.connect();
}

FragmentIterator RadosDataset::GetFragments(uint32_t start_object, uint32_t num_objects) {
  auto schema = this->schema();
  auto create_fragment =
      [schema](std::string object_id) -> Result<std::shared_ptr<Fragment>> {
        return std::make_shared<RadosFragment>(schema, object_id);
    };

  std::vector<std::string> object_ids;
  for (uint32_t i = 0; i < num_objects; i++) {
    object_ids.push_back(pool_name_ + "." + (start_object + i))
  }

  return MakeMaybeMapIterator(std::move(create_fragment), MakeVectorIterator(object_ids));
}

Result<RecordBatchIterator> RadosScanTask::Execute() {
  rados_ioctx_t io;
  librados::ceph::buffer::list in, out;
  uint32_t e = io.exec(object_id_, "arrow", "read_object", in, out);
  if (e != 0) {
    std::cout << "Failed to read from object";
    exit(EXIT_FAILURE);
  } else {
    using RecordBatchVector = std::vector<std::shared_ptr<RecordBatch>>;
    return MakeVectorIterator(RecordBatchVector{out});
  }
}

}  // namespace dataset
}  // namespace arrow
