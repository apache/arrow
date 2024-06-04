/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;


/**
 * Child allocator class. Only slightly different from the {@see RootAllocator},
 * in that these can't be created directly, but must be obtained from
 * {@link BufferAllocator#newChildAllocator(String, AllocationListener, long, long)}.
 *
 * <p>Child allocators can only be created by the root, or other children, so
 * this class is package private.</p>
 */
class ChildAllocator extends BaseAllocator {

  /**
   * Constructor.
   *
   * @param parentAllocator parent allocator -- the one creating this child
   * @param name            the name of this child allocator
   * @param config          configuration of this child allocator
   */
  ChildAllocator(
          BaseAllocator parentAllocator,
          String name,
          Config config) {
    super(parentAllocator, name, config);
  }
}
