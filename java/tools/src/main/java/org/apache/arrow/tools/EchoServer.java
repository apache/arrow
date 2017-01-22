package org.apache.arrow.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class EchoServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(EchoServer.class);

  private boolean closed = false;
  private final ServerSocket serverSocket;

  public EchoServer(int port) throws IOException {
    LOGGER.info("Starting echo server.");
    serverSocket = new ServerSocket(port);
    LOGGER.info("Running echo server on port: " + port());
  }

  public int port() { return serverSocket.getLocalPort(); }

  public static class ClientConnection implements AutoCloseable {
    public final Socket socket;
    public ClientConnection(Socket socket) {
      this.socket = socket;
    }

    public void run() throws IOException {
      BufferAllocator  allocator = new RootAllocator(Long.MAX_VALUE);
      List<ArrowRecordBatch> batches = new ArrayList<ArrowRecordBatch>();
      try (
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
      ) {
        // Read the entire input stream.
        reader.init();
        while (true) {
          ArrowRecordBatch batch = reader.nextRecordBatch();
          if (batch == null) break;
          batches.add(batch);
        }
        LOGGER.info(String.format("Received %d batches", batches.size()));

        // Write it back
        try (ArrowStreamWriter writer = new ArrowStreamWriter(out, reader.getSchema())) {
          for (ArrowRecordBatch batch: batches) {
            writer.writeRecordBatch(batch);
          }
          writer.end();
          Preconditions.checkState(reader.bytesRead() == writer.bytesWritten());
        }
        LOGGER.info("Done writing stream back.");
      }
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }

  public void run() throws IOException {
    try {
      while (!closed) {
        LOGGER.info("Waiting to accept new client connection.");
        Socket clientSocket = serverSocket.accept();
        LOGGER.info("Accepted new client connection.");
        try (ClientConnection client = new ClientConnection(clientSocket)) {
          try {
            client.run();
          } catch (IOException e) {
            LOGGER.warn("Error handling client connection.", e);
          }
        }
        LOGGER.info("Closed connection with client");
      }
    } catch (java.net.SocketException ex) {
      if (!closed) throw ex;
    } finally {
      serverSocket.close();
      LOGGER.info("Server closed.");
    }
  }

  public void close() throws IOException {
    closed = true;
    serverSocket.close();
  }

  public static void main(String[] args) throws Exception {
    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    } else {
      port = 8080;
    }
    new EchoServer(port).run();
  }
}
