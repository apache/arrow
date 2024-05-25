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

package org.apache.arrow.algorithm.sort;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test cases for out-of-place sorters.
 */
@RunWith(Parameterized.class)
public abstract class TestOutOfPlaceVectorSorter {

  protected final boolean generalSorter;

  public TestOutOfPlaceVectorSorter(boolean generalSorter) {
    this.generalSorter = generalSorter;
  }

  @Parameterized.Parameters(name = "general sorter = {0}")
  public static Collection<Object[]> getParameter() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[] {true});
    params.add(new Object[] {false});
    return params;
  }
}
