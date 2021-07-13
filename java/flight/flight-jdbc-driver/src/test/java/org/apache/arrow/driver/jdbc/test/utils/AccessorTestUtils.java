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

package org.apache.arrow.driver.jdbc.test.utils;

import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.vector.ValueVector;

public class AccessorTestUtils {


  public static class Cursor {
    int currentRow = 0;
    int limit;

    public Cursor(int limit) {
      this.limit = limit;
    }

    void next() {
      currentRow++;
    }

    boolean hasNext() {
      return currentRow < limit;
    }

    public int getCurrentRow() {
      return currentRow;
    }
  }

  public interface AccessorSupplier<T extends ArrowFlightJdbcAccessor> {
    T supply(ValueVector vector, IntSupplier getCurrentRow);
  }

  public interface AccessorConsumer<T extends ArrowFlightJdbcAccessor> {
    void accept(T accessor, int currentRow) throws Exception;
  }

  public static <T extends ArrowFlightJdbcAccessor> void iterateOnAccessor(
      ValueVector vector, AccessorSupplier<T> accessorSupplier, AccessorConsumer<T> accessorConsumer)
      throws Exception {
    Cursor cursor = new Cursor(vector.getValueCount());
    T accessor = accessorSupplier.supply(vector, cursor::getCurrentRow);

    while (cursor.hasNext()) {
      accessorConsumer.accept(accessor, cursor.getCurrentRow());
      cursor.next();
    }
  }
}
