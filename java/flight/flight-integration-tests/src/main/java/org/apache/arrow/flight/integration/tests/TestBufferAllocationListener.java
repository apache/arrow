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
package org.apache.arrow.flight.integration.tests;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.AllocationListener;

class TestBufferAllocationListener implements AllocationListener {
  static class Entry {
    StackTraceElement[] stackTrace;
    long size;
    boolean forAllocation;

    public Entry(StackTraceElement[] stackTrace, long size, boolean forAllocation) {
      this.stackTrace = stackTrace;
      this.size = size;
      this.forAllocation = forAllocation;
    }
  }

  List<Entry> trail = new ArrayList<>();

  public void onAllocation(long size) {
    trail.add(new Entry(Thread.currentThread().getStackTrace(), size, true));
  }

  public void onRelease(long size) {
    trail.add(new Entry(Thread.currentThread().getStackTrace(), size, false));
  }

  public void reThrowWithAddedAllocatorInfo(Exception e) {
    StringBuilder sb = new StringBuilder();
    sb.append(e.getMessage());
    sb.append("\n");
    sb.append("[[Buffer allocation and release trail during the test execution: \n");
    for (Entry trailEntry : trail) {
      sb.append(
          String.format(
              "%s: %d: %n%s",
              trailEntry.forAllocation ? "allocate" : "release",
              trailEntry.size,
              getStackTraceAsString(trailEntry.stackTrace)));
    }
    sb.append("]]");
    throw new IllegalStateException(sb.toString(), e);
  }

  private String getStackTraceAsString(StackTraceElement[] elements) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < elements.length; i++) {
      StackTraceElement s = elements[i];
      sb.append("\t");
      sb.append(s);
      sb.append("\n");
    }
    return sb.toString();
  }
}
