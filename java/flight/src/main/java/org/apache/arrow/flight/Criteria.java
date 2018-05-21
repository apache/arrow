/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.flight;

import org.apache.arrow.flight.impl.Flight;

public class Criteria {

  private static final long SLICE_TARGET = 10_000_000;
  private final long slice_target;

  public Criteria() {
    slice_target = SLICE_TARGET;
  }

  public Criteria(long slice_target) {
    super();
    this.slice_target = slice_target;
  }

  Criteria(Flight.Criteria criteria){
    slice_target = criteria.getSliceTarget() != 0 ? criteria.getSliceTarget() : SLICE_TARGET;
  }

  Flight.Criteria asCriteria(){
    return Flight.Criteria.newBuilder().setSliceTarget(slice_target).build();
  }
}
