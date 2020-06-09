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
 * The default Allocation Manager Factory for a module.
 *
 * This will be split out to the arrow-memory-netty/arrow-memory-unsafe modules
 * as part of ARROW-8230. This is currently a placeholder which defaults to Netty.
 *
 */
public class DefaultAllocationManagerFactory implements AllocationManager.Factory {

  public static final AllocationManager.Factory FACTORY = new DefaultAllocationManagerFactory();

  @Override
  public AllocationManager create(BaseAllocator accountingAllocator, long size) {
    return new NettyAllocationManager(accountingAllocator, size);
  }
}
