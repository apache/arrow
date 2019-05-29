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

import java.util.Optional;

/**
 * Describes the type of outcome that occurred when trying to account for allocation of memory.
 */
public class AllocationOutcome {
  private final Status status;
  private final AllocationOutcomeDetails details;
  static final AllocationOutcome SUCCESS_INSTANCE = new AllocationOutcome(Status.SUCCESS);

  AllocationOutcome(Status status, AllocationOutcomeDetails details) {
    this.status = status;
    this.details = details;
  }

  AllocationOutcome(Status status) {
    this(status, null);
  }

  /**
   * Get the status of the allocation.
   * @return status code.
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Get additional details of the allocation (like the status at each allocator in the hierarchy).
   * @return details of allocation
   */
  public Optional<AllocationOutcomeDetails> getDetails() {
    return Optional.ofNullable(details);
  }

  /**
   * Returns true if the allocation was a success.
   * @return true if allocation was successful, false otherwise.
   */
  public boolean isOk() {
    return status.isOk();
  }

  /**
   * Allocation status code.
   */
  public enum Status {
    /**
     * Allocation succeeded.
     */
    SUCCESS(true),

    /**
     * Allocation succeeded but only because the allocator was forced to move beyond a limit.
     */
    FORCED_SUCCESS(true),

    /**
     * Allocation failed because the local allocator's limits were exceeded.
     */
    FAILED_LOCAL(false),

    /**
     * Allocation failed because a parent allocator's limits were exceeded.
     */
    FAILED_PARENT(false);

    private final boolean ok;

    Status(boolean ok) {
      this.ok = ok;
    }

    public boolean isOk() {
      return ok;
    }
  }
}
