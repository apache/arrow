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

#include "arrow/compute/exec/asof_join.h"
#include <iostream>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
//#include <arrow/io/util_internal.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/options.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/counting_semaphore.h>  // so we don't need to require C++20
#include <arrow/util/optional.h>
#include <arrow/util/thread_pool.h>
#include <algorithm>
#include <atomic>
#include <future>
#include <mutex>
#include <optional>
#include <thread>

#include <omp.h>

#include "concurrent_bounded_queue.h"

namespace arrow {
namespace compute {

class AsofJoinBasicImpl : public AsofJoinImpl {};

Result<std::unique_ptr<AsofJoinImpl>> AsofJoinImpl::MakeBasic() {
  std::unique_ptr<AsofJoinImpl> impl{new AsofJoinBasicImpl()};
  return std::move(impl);
}

}  // namespace compute
}  // namespace arrow
