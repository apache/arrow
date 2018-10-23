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

package org.apache.arrow.plasma;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PlasmaClientTest {

  private String storeSuffix = "/tmp/store";

  private Process storeProcess;

  private int storePort;

  private ObjectStoreLink pLink;


  public PlasmaClientTest() throws Exception {
    try {
      String plasmaStorePath = System.getenv("PLASMA_STORE");
      if (plasmaStorePath == null) {
        throw new Exception("Please set plasma store path in env PLASMA_STORE");
      }

      this.startObjectStore(plasmaStorePath);
      System.loadLibrary("plasma_java");
      pLink = new PlasmaClient(this.getStoreAddress(), "", 0);
    }
    catch (Throwable t) {
      cleanup();
      throw t;
    }

  }

  private Process startProcess(String[] cmd) {
    ProcessBuilder builder;
    List<String> newCmd = Arrays.stream(cmd).filter(s -> s.length() > 0).collect(Collectors.toList());
    builder = new ProcessBuilder(newCmd);
    builder.inheritIO();
    Process p = null;
    try {
      p = builder.start();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    System.out.println("Start process " + p.hashCode() + " OK, cmd = " + Arrays.toString(cmd).replace(',', ' '));
    return p;
  }

  private void startObjectStore(String plasmaStorePath) {
    int occupiedMemoryMB = 10;
    long memoryBytes = occupiedMemoryMB * 1000000;
    int numRetries = 10;
    Process p = null;
    while (numRetries-- > 0) {
      int currentPort = java.util.concurrent.ThreadLocalRandom.current().nextInt(0, 100000);
      String name = storeSuffix + currentPort;
      String cmd = plasmaStorePath + " -s " + name + " -m " + memoryBytes;

      p = startProcess(cmd.split(" "));

      if (p != null && p.isAlive()) {
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (p.isAlive()) {
          storePort = currentPort;
          break;
        }
      }
    }


    if (p == null || !p.isAlive()) {
      throw new RuntimeException("Start object store failed ...");
    } else {
      storeProcess = p;
      System.out.println("Start object store success");
    }
  }

  private void cleanup() {
    if (storeProcess != null && killProcess(storeProcess)) {
      System.out.println("Kill plasma store process forcely");
    }
  }

  private static boolean killProcess(Process p) {
    if (p.isAlive()) {
      p.destroyForcibly();
      return true;
    } else {
      return false;
    }
  }

  public void doTest() {
    System.out.println("Start test.");
    int timeoutMs = 3000;
    byte[] id1 =  new byte[20];
    Arrays.fill(id1, (byte)1);
    byte[] value1 =  new byte[20];
    Arrays.fill(value1, (byte)11);
    pLink.put(id1, value1, null);

    byte[] id2 =  new byte[20];
    Arrays.fill(id2, (byte)2);
    byte[] value2 =  new byte[20];
    Arrays.fill(value2, (byte)12);
    pLink.put(id2, value2, null);
    System.out.println("Plasma java client put test success.");
    byte[] getValue1 = pLink.get(id1, timeoutMs, false);
    assert Arrays.equals(value1, getValue1);

    byte[] getValue2 = pLink.get(id2, timeoutMs, false);
    assert Arrays.equals(value2, getValue2);
    System.out.println("Plasma java client get single object test success.");
    byte[][] ids = {id1, id2};
    List<byte[]> values = pLink.get(ids, timeoutMs, false);
    assert Arrays.equals(values.get(0), value1);
    assert Arrays.equals(values.get(1), value2);
    System.out.println("Plasma java client get multi-object test success.");
    pLink.put(id1, value1, null);
    System.out.println("Plasma java client put same object twice exception test success.");
    byte[] id1Hash = pLink.hash(id1);
    assert id1Hash != null;
    System.out.println("Plasma java client hash test success.");
    boolean exsit = pLink.contains(id2);
    assert exsit;
    byte[] id3 =  new byte[20];
    Arrays.fill(id3, (byte)3);
    boolean notExsit = pLink.contains(id3);
    assert !notExsit;
    System.out.println("Plasma java client contains test success.");
    cleanup();
    System.out.println("All test success.");

  }

  public String getStoreAddress() {
    return storeSuffix + storePort;
  }

  public static void main(String[] args) throws Exception {

    PlasmaClientTest plasmaClientTest = new PlasmaClientTest();
    plasmaClientTest.doTest();

  }

}
