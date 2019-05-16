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

package org.apache.arrow.tools;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Simple server that echoes back data received.
 */
public class EchoServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(EchoServer.class);
  private final ServerSocket serverSocket;
  private boolean closed = false;

  /**
   * Constructs a new instance that binds to the given port.
   */
  public EchoServer(int port) throws IOException {
    LOGGER.debug("Starting echo server.");
    serverSocket = new ServerSocket(port);
    LOGGER.debug("Running echo server on port: " + port());
  }

  /**
   * Main method to run the server, the first argument is an optional port number.
   */
  public static void main(String[] args) throws Exception {
    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    } else {
      port = 8080;
    }
    new EchoServer(port).run();
  }

  public int port() {
    return serverSocket.getLocalPort();
  }

  /**
   * Starts the main server event loop.
   */
  public void run() throws IOException {
    try {
      while (!closed) {
        LOGGER.debug("Waiting to accept new client connection.");
        Socket clientSocket = serverSocket.accept();
        LOGGER.debug("Accepted new client connection.");
        try (ClientConnection client = new ClientConnection(clientSocket)) {
          try {
            client.run();
          } catch (IOException e) {
            LOGGER.warn("Error handling client connection.", e);
          }
        }
        LOGGER.debug("Closed connection with client");
      }
    } catch (java.net.SocketException ex) {
      if (!closed) {
        throw ex;
      }
    } finally {
      serverSocket.close();
      LOGGER.debug("Server closed.");
    }
  }

  public void close() throws IOException {
    closed = true;
    serverSocket.close();
  }

  public static class ClientConnection implements AutoCloseable {
    public final Socket socket;

    public ClientConnection(Socket socket) {
      this.socket = socket;
    }

    /**
     * Reads a record batch off the socket and writes it back out.
     */
    public void run() throws IOException {
      // Read the entire input stream and write it back
      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
           ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), allocator)) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // load the first batch before instantiating the writer so that we have any dictionaries
        reader.loadNextBatch();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, reader, socket
            .getOutputStream())) {
          writer.start();
          int echoed = 0;
          while (true) {
            int rowCount = reader.getVectorSchemaRoot().getRowCount();
            if (rowCount == 0) {
              break;
            } else {
              writer.writeBatch();
              echoed += rowCount;
              reader.loadNextBatch();
            }
          }
          writer.end();
          Preconditions.checkState(reader.bytesRead() == writer.bytesWritten());
          LOGGER.debug(String.format("Echoed %d records", echoed));
        }
      }
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }
}
