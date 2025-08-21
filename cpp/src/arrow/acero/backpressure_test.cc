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

#include <gtest/gtest.h>

#include "arrow/acero/backpressure.h"

namespace arrow {
namespace acero {

class MonitorBackpressureControl : public acero::BackpressureControl {
 public:
  explicit MonitorBackpressureControl(std::atomic<bool>& paused) : paused(paused) {}
  virtual void Pause() { paused = true; }
  virtual void Resume() { paused = false; }
  std::atomic<bool>& paused;
};

TEST(BackpressureCombiner, Basic) {
  std::atomic<bool> paused{false};
  BackpressureCombiner combiner(std::make_unique<MonitorBackpressureControl>(paused));

  BackpressureCombiner::Source weak_source1(&combiner, false);
  BackpressureCombiner::Source weak_source2;
  weak_source2.AddController(&combiner, false);
  BackpressureCombiner::Source strong_source1(&combiner);
  BackpressureCombiner::Source strong_source2;
  strong_source2.AddController(&combiner);

  // Any strong causes pause
  ASSERT_FALSE(paused);
  strong_source1.Pause();
  ASSERT_TRUE(paused);
  strong_source2.Pause();
  ASSERT_TRUE(paused);
  strong_source1.Resume();
  ASSERT_TRUE(paused);
  strong_source2.Resume();
  ASSERT_FALSE(paused);

  // All weak cause pause
  ASSERT_FALSE(paused);
  weak_source1.Pause();
  ASSERT_FALSE(paused);
  weak_source2.Pause();
  ASSERT_TRUE(paused);
  weak_source1.Resume();
  ASSERT_FALSE(paused);
  weak_source2.Resume();
  ASSERT_FALSE(paused);

  // mixed use
  strong_source1.Pause();
  ASSERT_TRUE(paused);

  ASSERT_TRUE(paused);
  weak_source1.Pause();
  ASSERT_TRUE(paused);
  weak_source2.Pause();

  strong_source1.Resume();
  ASSERT_TRUE(paused);

  weak_source1.Resume();
  ASSERT_FALSE(paused);
  weak_source2.Resume();
  ASSERT_FALSE(paused);
}

}  // namespace acero
}  // namespace arrow
