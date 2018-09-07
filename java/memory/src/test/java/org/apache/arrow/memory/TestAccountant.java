/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

public class TestAccountant {

  @Test
  public void basic() {
    ensureAccurateReservations(null);
  }

  @Test
  public void nested() {
    final Accountant parent = new Accountant(null, 0, Long.MAX_VALUE);
    ensureAccurateReservations(parent);
    assertEquals(0, parent.getAllocatedMemory());
    assertEquals(parent.getLimit() - parent.getAllocatedMemory(), parent.getHeadroom());
  }

  @Test
  public void multiThread() throws InterruptedException {
    final Accountant parent = new Accountant(null, 0, Long.MAX_VALUE);

    final int numberOfThreads = 32;
    final int loops = 100;
    Thread[] threads = new Thread[numberOfThreads];

    for (int i = 0; i < numberOfThreads; i++) {
      Thread t = new Thread() {

        @Override
        public void run() {
          try {
            for (int i = 0; i < loops; i++) {
              ensureAccurateReservations(parent);
            }
          } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
          }
        }

      };
      threads[i] = t;
      t.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    assertEquals(0, parent.getAllocatedMemory());
    assertEquals(parent.getLimit() - parent.getAllocatedMemory(), parent.getHeadroom());
  }

  private void ensureAccurateReservations(Accountant outsideParent) {
    final Accountant parent = new Accountant(outsideParent, 0, 10);
    assertEquals(0, parent.getAllocatedMemory());

    final Accountant child = new Accountant(parent, 2, Long.MAX_VALUE);
    assertEquals(2, parent.getAllocatedMemory());
    assertEquals(10, child.getHeadroom());
    {
      AllocationOutcome first = child.allocateBytes(1);
      assertEquals(AllocationOutcome.SUCCESS, first);
    }

    // child will have new allocation
    assertEquals(1, child.getAllocatedMemory());

    // root has no change since within reservation
    assertEquals(2, parent.getAllocatedMemory());

    {
      AllocationOutcome first = child.allocateBytes(1);
      assertEquals(AllocationOutcome.SUCCESS, first);
    }

    // child will have new allocation
    assertEquals(2, child.getAllocatedMemory());

    // root has no change since within reservation
    assertEquals(2, parent.getAllocatedMemory());

    child.releaseBytes(1);

    // child will have new allocation
    assertEquals(1, child.getAllocatedMemory());

    // root has no change since within reservation
    assertEquals(2, parent.getAllocatedMemory());

    {
      AllocationOutcome first = child.allocateBytes(2);
      assertEquals(AllocationOutcome.SUCCESS, first);
    }

    // child will have new allocation
    assertEquals(3, child.getAllocatedMemory());

    // went beyond reservation, now in parent accountant
    assertEquals(3, parent.getAllocatedMemory());

    assertEquals(7, child.getHeadroom());
    assertEquals(7, parent.getHeadroom());

    {
      AllocationOutcome first = child.allocateBytes(7);
      assertEquals(AllocationOutcome.SUCCESS, first);
    }

    // child will have new allocation
    assertEquals(10, child.getAllocatedMemory());

    // went beyond reservation, now in parent accountant
    assertEquals(10, parent.getAllocatedMemory());

    child.releaseBytes(9);

    assertEquals(1, child.getAllocatedMemory());
    assertEquals(9, child.getHeadroom());

    // back to reservation size
    assertEquals(2, parent.getAllocatedMemory());
    assertEquals(8, parent.getHeadroom());

    AllocationOutcome first = child.allocateBytes(10);
    assertEquals(AllocationOutcome.FAILED_PARENT, first);

    // unchanged
    assertEquals(1, child.getAllocatedMemory());
    assertEquals(2, parent.getAllocatedMemory());

    boolean withinLimit = child.forceAllocate(10);
    assertEquals(false, withinLimit);

    // at new limit
    assertEquals(child.getAllocatedMemory(), 11);
    assertEquals(parent.getAllocatedMemory(), 11);
    assertEquals(-1, child.getHeadroom());
    assertEquals(-1, parent.getHeadroom());

    child.releaseBytes(11);
    assertEquals(child.getAllocatedMemory(), 0);
    assertEquals(parent.getAllocatedMemory(), 2);
    assertEquals(10, child.getHeadroom());
    assertEquals(8, parent.getHeadroom());

    child.close();
    parent.close();
  }
}
