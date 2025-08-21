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

#pragma once
#include "arrow/acero/options.h"

#include <mutex>
namespace arrow::acero {

// Provides infrastructure of combining multiple backpressure sources and propagate the
// result into BackpressureControl There are two types of Source: strong - pause on any
// strong Source within controller
class ARROW_ACERO_EXPORT BackpressureCombiner {
 public:
  explicit BackpressureCombiner(
      std::unique_ptr<BackpressureControl> backpressure_control);

  // Instances of Source can be used as usual BackpresureControl.
  // Source can be connected with one or more BackpressureCombiner
  class ARROW_ACERO_EXPORT Source : public BackpressureControl {
   public:
    // strong - strong_connection=true
    // weak - strong_connection=false
    explicit Source(BackpressureCombiner* ctrl = nullptr, bool strong_connection = true);
    void AddController(BackpressureCombiner* ctrl, bool strong_connection = true);
    void Pause() override;
    void Resume() override;

   private:
    struct Connection {
      BackpressureCombiner* ctrl;
      bool strong;
    };
    std::vector<Connection> connections_;
  };

 private:
  friend class Source;
  void Pause(Source* output, bool strong_connection);
  void Resume(Source* output, bool strong_connection);

  void UpdatePauseStateUnlocked();
  std::unique_ptr<BackpressureControl> backpressure_control_;
  std::mutex mutex_;
  std::unordered_map<Source*, bool> strong_paused_;
  std::unordered_map<Source*, bool> weak_paused_;
  size_t strong_paused_count_{0};
  size_t weak_paused_count_{0};
  bool paused{false};
};

}  // namespace arrow::acero
