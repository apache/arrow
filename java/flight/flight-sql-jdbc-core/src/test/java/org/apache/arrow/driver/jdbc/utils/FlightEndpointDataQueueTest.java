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
package org.apache.arrow.driver.jdbc.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletionService;
import org.apache.arrow.driver.jdbc.client.CloseableEndpointStreamPair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests for {@link FlightEndpointDataQueue}. */
@ExtendWith(MockitoExtension.class)
public class FlightEndpointDataQueueTest {

  @Mock private CompletionService<CloseableEndpointStreamPair> mockedService;
  private FlightEndpointDataQueue queue;

  @BeforeEach
  public void setUp() {
    queue = new FlightEndpointDataQueue(mockedService);
  }

  @Test
  public void testNextShouldRetrieveNullIfEmpty() throws Exception {
    assertThat(queue.next(), is(nullValue()));
  }

  @Test
  public void testNextShouldThrowExceptionUponClose() throws Exception {
    queue.close();
    ThrowableAssertionUtils.simpleAssertThrowableClass(
        IllegalStateException.class, () -> queue.next());
  }

  @Test
  public void testEnqueueShouldThrowExceptionUponClose() throws Exception {
    queue.close();
    ThrowableAssertionUtils.simpleAssertThrowableClass(
        IllegalStateException.class, () -> queue.enqueue(mock(CloseableEndpointStreamPair.class)));
  }

  @Test
  public void testCheckOpen() throws Exception {
    assertDoesNotThrow(
        () -> {
          queue.checkOpen();
          return true;
        });
    queue.close();
    ThrowableAssertionUtils.simpleAssertThrowableClass(
        IllegalStateException.class, () -> queue.checkOpen());
  }

  @Test
  public void testShouldCloseQueue() throws Exception {
    assertThat(queue.isClosed(), is(false));
    queue.close();
    assertThat(queue.isClosed(), is(true));
  }
}
