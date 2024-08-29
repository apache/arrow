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

package org.apache.arrow.vector.ipc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for {@link WriteChannel}.
 */
public class WriteChannelBenchmark {

  /**
   * State object for align benchmark.
   */
  @State(Scope.Benchmark)
  public static class AlignState {

    private ByteArrayOutputStream baos;

    private WriteChannel writeChannel;

    @Param({"1", "2", "3", "4", "5", "6", "7"})
    public int alignSize;

    @Setup(Level.Invocation)
    public void prepareInvoke() throws IOException {
      baos = new ByteArrayOutputStream(8);
      writeChannel = new WriteChannel(Channels.newChannel(baos));
      writeChannel.write(new byte[8 - alignSize]);
    }

    @TearDown(Level.Invocation)
    public void tearDownInvoke() throws IOException {
      writeChannel.close();
      baos.close();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void alignBenchmark(AlignState state) throws IOException {
    state.writeChannel.align();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(WriteChannelBenchmark.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
