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

package org.apache.arrow.dataset.jni;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

public class TestReservationListener extends TestDataset {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  public static final String AVRO_SCHEMA_USER = "user.avsc";

  @Test
  public void testDirectReservationListener() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");
    NativeMemoryPool pool = NativeMemoryPool.createListenable(DirectReservationListener.instance());
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        pool, FileFormat.PARQUET,
        writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(100);
    long initReservation = DirectReservationListener.instance().getCurrentDirectMemReservation();
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    long reservation = DirectReservationListener.instance().getCurrentDirectMemReservation();
    AutoCloseables.close(datum);
    AutoCloseables.close(pool);
    long finalReservation = DirectReservationListener.instance().getCurrentDirectMemReservation();
    Assert.assertTrue(reservation >= initReservation);
    Assert.assertEquals(initReservation, finalReservation);
  }

  @Test
  public void testCustomReservationListener() throws Exception {
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");
    final AtomicLong reserved = new AtomicLong(0L);
    ReservationListener listener = new ReservationListener() {
      @Override
      public void reserve(long size) {
        reserved.getAndAdd(size);
      }

      @Override
      public void unreserve(long size) {
        reserved.getAndAdd(-size);
      }
    };
    NativeMemoryPool pool = NativeMemoryPool.createListenable(listener);
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        pool, FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(100);
    long initReservation = reserved.get();
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    long reservation = reserved.get();
    AutoCloseables.close(datum);
    AutoCloseables.close(pool);
    long finalReservation = reserved.get();
    Assert.assertTrue(reservation >= initReservation);
    Assert.assertEquals(initReservation, finalReservation);
  }

  @Test
  public void testErrorThrownFromReservationListener() throws Exception {
    final String errorMessage = "ERROR_MESSAGE";
    ParquetWriteSupport writeSupport = ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP.newFolder(), 1, "a");
    final AtomicLong reserved = new AtomicLong(0L);
    ReservationListener listener = new ReservationListener() {
      @Override
      public void reserve(long size) {
        throw new IllegalArgumentException(errorMessage);
      }

      @Override
      public void unreserve(long size) {
        // no-op
      }
    };
    NativeMemoryPool pool = NativeMemoryPool.createListenable(listener);
    FileSystemDatasetFactory factory = new FileSystemDatasetFactory(rootAllocator(),
        pool, FileFormat.PARQUET, writeSupport.getOutputURI());
    ScanOptions options = new ScanOptions(100);
    long initReservation = reserved.get();
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      collectResultFromFactory(factory, options);
    }, errorMessage);
    long reservation = reserved.get();
    AutoCloseables.close(pool);
    long finalReservation = reserved.get();
    Assert.assertEquals(initReservation, reservation);
    Assert.assertEquals(initReservation, finalReservation);
  }
}
