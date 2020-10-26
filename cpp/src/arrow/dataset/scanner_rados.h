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

#include "arrow/dataset/scanner.h"
#include "arrow/dataset/dataset_rados.h"

namespace arrow {
namespace dataset {

/// \brief A ScanTask to push down operations to the CLS for 
/// performing an InMemory Scan of a RadosObject.
class ARROW_DS_EXPORT RadosScanTask : public ScanTask {
  public: 
    /// \brief Construct a RadosScanTask object.
    ///
    /// \param[in] options the ScanOptions.
    /// \param[in] context the ScanContext.
    /// \param[in] object the RadosObject to apply the operations.
    /// \param[in] rados_options the connection information to the RADOS interface.
    RadosScanTask(std::shared_ptr<ScanOptions> options, 
                  std::shared_ptr<ScanContext> context,
                  std::shared_ptr<RadosObject> object,
                  std::shared_ptr<RadosOptions> rados_options)
        : ScanTask(std::move(options), std::move(context)), 
          object_(std::move(object)), 
          rados_options_(rados_options) {}

    Result<RecordBatchIterator> Execute() override;

  protected:
    std::shared_ptr<RadosObject> object_;
    std::shared_ptr<RadosOptions> rados_options_;
};

} // namespace dataset
} // namespace arrow