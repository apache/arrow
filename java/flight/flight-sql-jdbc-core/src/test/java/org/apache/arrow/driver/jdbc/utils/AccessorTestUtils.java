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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.vector.ValueVector;
import org.hamcrest.Matcher;
import org.junit.rules.ErrorCollector;

public class AccessorTestUtils {
  @FunctionalInterface
  public interface CheckedFunction<T, R> {
    R apply(T t) throws SQLException;
  }

  public interface AccessorSupplier<T extends ArrowFlightJdbcAccessor> {
    T supply(ValueVector vector, IntSupplier getCurrentRow);
  }

  public interface AccessorConsumer<T extends ArrowFlightJdbcAccessor> {
    void accept(T accessor, int currentRow) throws Exception;
  }

  public interface MatcherGetter<T extends ArrowFlightJdbcAccessor, R> {
    Matcher<R> get(T accessor, int currentRow);
  }

  public static class Cursor {
    int currentRow = 0;
    int limit;

    public Cursor(int limit) {
      this.limit = limit;
    }

    public void next() {
      currentRow++;
    }

    boolean hasNext() {
      return currentRow < limit;
    }

    public int getCurrentRow() {
      return currentRow;
    }
  }

  public static class AccessorIterator<T extends ArrowFlightJdbcAccessor> {
    private final ErrorCollector collector;
    private final AccessorSupplier<T> accessorSupplier;

    public AccessorIterator(ErrorCollector collector, AccessorSupplier<T> accessorSupplier) {
      this.collector = collector;
      this.accessorSupplier = accessorSupplier;
    }

    public void iterate(ValueVector vector, AccessorConsumer<T> accessorConsumer) throws Exception {
      int valueCount = vector.getValueCount();
      if (valueCount == 0) {
        throw new IllegalArgumentException("Vector is empty");
      }

      Cursor cursor = new Cursor(valueCount);
      T accessor = accessorSupplier.supply(vector, cursor::getCurrentRow);

      while (cursor.hasNext()) {
        accessorConsumer.accept(accessor, cursor.getCurrentRow());
        cursor.next();
      }
    }

    public void iterate(ValueVector vector, Consumer<T> accessorConsumer) throws Exception {
      iterate(vector, (accessor, currentRow) -> accessorConsumer.accept(accessor));
    }

    public List<Object> toList(ValueVector vector) throws Exception {
      List<Object> result = new ArrayList<>();
      iterate(vector, (accessor, currentRow) -> result.add(accessor.getObject()));

      return result;
    }

    public <R> void assertAccessorGetter(ValueVector vector, CheckedFunction<T, R> getter,
                                         MatcherGetter<T, R> matcherGetter) throws Exception {
      iterate(vector, (accessor, currentRow) -> {
        R object = getter.apply(accessor);
        boolean wasNull = accessor.wasNull();

        collector.checkThat(object, matcherGetter.get(accessor, currentRow));
        collector.checkThat(wasNull, is(accessor.getObject() == null));
      });
    }

    public <R> void assertAccessorGetterThrowingException(ValueVector vector, CheckedFunction<T, R> getter)
        throws Exception {
      iterate(vector, (accessor, currentRow) ->
          ThrowableAssertionUtils.simpleAssertThrowableClass(SQLException.class, () -> getter.apply(accessor)));
    }

    public <R> void assertAccessorGetter(ValueVector vector, CheckedFunction<T, R> getter,
                                         Function<T, Matcher<R>> matcherGetter) throws Exception {
      assertAccessorGetter(vector, getter, (accessor, currentRow) -> matcherGetter.apply(accessor));
    }

    public <R> void assertAccessorGetter(ValueVector vector, CheckedFunction<T, R> getter,
                                         Supplier<Matcher<R>> matcherGetter) throws Exception {
      assertAccessorGetter(vector, getter, (accessor, currentRow) -> matcherGetter.get());
    }

    public <R> void assertAccessorGetter(ValueVector vector, CheckedFunction<T, R> getter,
                                         Matcher<R> matcher) throws Exception {
      assertAccessorGetter(vector, getter, (accessor, currentRow) -> matcher);
    }
  }
}
