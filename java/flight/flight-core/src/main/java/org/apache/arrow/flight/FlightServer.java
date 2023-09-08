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

package org.apache.arrow.flight;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.auth.ServerAuthInterceptor;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.ServerCallHeaderAuthMiddleware;
import org.apache.arrow.flight.grpc.ServerInterceptorAdapter;
import org.apache.arrow.flight.grpc.ServerInterceptorAdapter.KeyFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;

/**
 * Generic server of flight data that is customized via construction with delegate classes for the
 * actual logic.  The server currently uses GRPC as its transport mechanism.
 */
public class FlightServer implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlightServer.class);

  private final Location location;
  private final Server server;
  // The executor used by the gRPC server. We don't use it here, but we do need to clean it up with the server.
  // May be null, if a user-supplied executor was provided (as we do not want to clean that up)
  @VisibleForTesting
  final ExecutorService grpcExecutor;

  /** The maximum size of an individual gRPC message. This effectively disables the limit. */
  static final int MAX_GRPC_MESSAGE_SIZE = Integer.MAX_VALUE;

  /** Create a new instance from a gRPC server. For internal use only. */
  private FlightServer(Location location, Server server, ExecutorService grpcExecutor) {
    this.location = location;
    this.server = server;
    this.grpcExecutor = grpcExecutor;
  }

  /** Start the server. */
  public FlightServer start() throws IOException {
    server.start();
    return this;
  }

  /** Get the port the server is running on (if applicable). */
  public int getPort() {
    return server.getPort();
  }

  /** Get the location for this server. */
  public Location getLocation() {
    if (location.getUri().getPort() == 0) {
      // If the server was bound to port 0, replace the port in the location with the real port.
      final URI uri = location.getUri();
      try {
        return new Location(new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), getPort(),
            uri.getPath(), uri.getQuery(), uri.getFragment()));
      } catch (URISyntaxException e) {
        // We don't expect this to happen
        throw new RuntimeException(e);
      }
    }
    return location;
  }

  /** Block until the server shuts down. */
  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  /** Request that the server shut down. */
  public void shutdown() {
    server.shutdown();
    if (grpcExecutor != null) {
      grpcExecutor.shutdown();
    }
  }

  /**
   * Wait for the server to shut down with a timeout.
   * @return true if the server shut down successfully.
   */
  public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
    return server.awaitTermination(timeout, unit);
  }

  /** Shutdown the server, waits for up to 6 seconds for successful shutdown before returning. */
  public void close() throws InterruptedException {
    shutdown();
    final boolean terminated = awaitTermination(3000, TimeUnit.MILLISECONDS);
    if (terminated) {
      logger.debug("Server was terminated within 3s");
      return;
    }

    // get more aggressive in termination.
    server.shutdownNow();

    int count = 0;
    while (!server.isTerminated() & count < 30) {
      count++;
      logger.debug("Waiting for termination");
      Thread.sleep(100);
    }

    if (!server.isTerminated()) {
      logger.warn("Couldn't shutdown server, resources likely will be leaked.");
    }
  }

  /** Create a builder for a Flight server. */
  public static Builder builder() {
    return new Builder();
  }

  /** Create a builder for a Flight server. */
  public static Builder builder(BufferAllocator allocator, Location location, FlightProducer producer) {
    return new Builder(allocator, location, producer);
  }

  /** A builder for Flight servers. */
  public static final class Builder {
    private BufferAllocator allocator;
    private Location location;
    private FlightProducer producer;
    private final Map<String, Object> builderOptions;
    private ServerAuthHandler authHandler = ServerAuthHandler.NO_OP;
    private CallHeaderAuthenticator headerAuthenticator = CallHeaderAuthenticator.NO_OP;
    private ExecutorService executor = null;
    private int maxInboundMessageSize = MAX_GRPC_MESSAGE_SIZE;
    private InputStream certChain;
    private InputStream key;
    private final List<KeyFactory<?>> interceptors;
    // Keep track of inserted interceptors
    private final Set<String> interceptorKeys;

    Builder() {
      builderOptions = new HashMap<>();
      interceptors = new ArrayList<>();
      interceptorKeys = new HashSet<>();
    }

    Builder(BufferAllocator allocator, Location location, FlightProducer producer) {
      this();
      this.allocator = Preconditions.checkNotNull(allocator);
      this.location = Preconditions.checkNotNull(location);
      this.producer = Preconditions.checkNotNull(producer);
    }

    /** Create the server for this builder. */
    public FlightServer build() {
      // Add the auth middleware if applicable.
      if (headerAuthenticator != CallHeaderAuthenticator.NO_OP) {
        this.middleware(FlightServerMiddleware.Key.of(Auth2Constants.AUTHORIZATION_HEADER),
            new ServerCallHeaderAuthMiddleware.Factory(headerAuthenticator));
      }

      this.middleware(FlightConstants.HEADER_KEY, new ServerHeaderMiddleware.Factory());

      final NettyServerBuilder builder;
      switch (location.getUri().getScheme()) {
        case LocationSchemes.GRPC_DOMAIN_SOCKET: {
          // The implementation is platform-specific, so we have to find the classes at runtime
          builder = NettyServerBuilder.forAddress(location.toSocketAddress());
          try {
            try {
              // Linux
              builder.channelType(
                  (Class<? extends ServerChannel>) Class
                      .forName("io.netty.channel.epoll.EpollServerDomainSocketChannel"));
              final EventLoopGroup elg = (EventLoopGroup) Class.forName("io.netty.channel.epoll.EpollEventLoopGroup")
                  .newInstance();
              builder.bossEventLoopGroup(elg).workerEventLoopGroup(elg);
            } catch (ClassNotFoundException e) {
              // BSD
              builder.channelType(
                  (Class<? extends ServerChannel>) Class
                      .forName("io.netty.channel.kqueue.KQueueServerDomainSocketChannel"));
              final EventLoopGroup elg = (EventLoopGroup) Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                  .newInstance();
              builder.bossEventLoopGroup(elg).workerEventLoopGroup(elg);
            }
          } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new UnsupportedOperationException(
                "Could not find suitable Netty native transport implementation for domain socket address.");
          }
          break;
        }
        case LocationSchemes.GRPC:
        case LocationSchemes.GRPC_INSECURE: {
          builder = NettyServerBuilder.forAddress(location.toSocketAddress());
          break;
        }
        case LocationSchemes.GRPC_TLS: {
          if (certChain == null) {
            throw new IllegalArgumentException("Must provide a certificate and key to serve gRPC over TLS");
          }
          builder = NettyServerBuilder.forAddress(location.toSocketAddress());
          break;
        }
        default:
          throw new IllegalArgumentException("Scheme is not supported: " + location.getUri().getScheme());
      }

      if (certChain != null) {
        builder.useTransportSecurity(certChain, key);
      }

      // Share one executor between the gRPC service, DoPut, and Handshake
      final ExecutorService exec;
      // We only want to have FlightServer close the gRPC executor if we created it here. We should not close
      // user-supplied executors.
      final ExecutorService grpcExecutor;
      if (executor != null) {
        exec = executor;
        grpcExecutor = null;
      } else {
        exec = Executors.newCachedThreadPool(
            // Name threads for better debuggability
            new ThreadFactoryBuilder().setNameFormat("flight-server-default-executor-%d").build());
        grpcExecutor = exec;
      }

      final FlightBindingService flightService = new FlightBindingService(allocator, producer, authHandler, exec);
      builder
          .executor(exec)
          .maxInboundMessageSize(maxInboundMessageSize)
          .addService(
              ServerInterceptors.intercept(
                  flightService,
                  new ServerAuthInterceptor(authHandler)));

      // Allow hooking into the gRPC builder. This is not guaranteed to be available on all Arrow versions or
      // Flight implementations.
      builderOptions.computeIfPresent("grpc.builderConsumer", (key, builderConsumer) -> {
        final Consumer<NettyServerBuilder> consumer = (Consumer<NettyServerBuilder>) builderConsumer;
        consumer.accept(builder);
        return null;
      });

      // Allow explicitly setting some Netty-specific options
      builderOptions.computeIfPresent("netty.channelType", (key, channelType) -> {
        builder.channelType((Class<? extends ServerChannel>) channelType);
        return null;
      });
      builderOptions.computeIfPresent("netty.bossEventLoopGroup", (key, elg) -> {
        builder.bossEventLoopGroup((EventLoopGroup) elg);
        return null;
      });
      builderOptions.computeIfPresent("netty.workerEventLoopGroup", (key, elg) -> {
        builder.workerEventLoopGroup((EventLoopGroup) elg);
        return null;
      });

      builder.intercept(new ServerInterceptorAdapter(interceptors));
      return new FlightServer(location, builder.build(), grpcExecutor);
    }

    /**
     * Set the maximum size of a message. Defaults to "unlimited", depending on the underlying transport.
     */
    public Builder maxInboundMessageSize(int maxMessageSize) {
      this.maxInboundMessageSize = maxMessageSize;
      return this;
    }

    /**
     * Enable TLS on the server.
     * @param certChain The certificate chain to use.
     * @param key The private key to use.
     */
    public Builder useTls(final File certChain, final File key) throws IOException {
      this.certChain = new FileInputStream(certChain);
      this.key = new FileInputStream(key);
      return this;
    }

    /**
     * Enable TLS on the server.
     * @param certChain The certificate chain to use.
     * @param key The private key to use.
     */
    public Builder useTls(final InputStream certChain, final InputStream key) {
      this.certChain = certChain;
      this.key = key;
      return this;
    }

    /**
     * Set the executor used by the server.
     *
     * <p>Flight will NOT take ownership of the executor. The application must clean it up if one is provided. (If not
     * provided, Flight will use a default executor which it will clean up.)
     */
    public Builder executor(ExecutorService executor) {
      this.executor = executor;
      return this;
    }

    /**
     * Set the authentication handler.
     */
    public Builder authHandler(ServerAuthHandler authHandler) {
      this.authHandler = authHandler;
      return this;
    }

    /**
     * Set the header-based authentication mechanism.
     */
    public Builder headerAuthenticator(CallHeaderAuthenticator headerAuthenticator) {
      this.headerAuthenticator = headerAuthenticator;
      return this;
    }

    /**
     * Provide a transport-specific option. Not guaranteed to have any effect.
     */
    public Builder transportHint(final String key, Object option) {
      builderOptions.put(key, option);
      return this;
    }

    /**
     * Add a Flight middleware component to inspect and modify requests to this service.
     *
     * @param key An identifier for this middleware component. Service implementations can retrieve the middleware
     *     instance for the current call using {@link org.apache.arrow.flight.FlightProducer.CallContext}.
     * @param factory A factory for the middleware.
     * @param <T> The middleware type.
     * @throws IllegalArgumentException if the key already exists
     */
    public <T extends FlightServerMiddleware> Builder middleware(final FlightServerMiddleware.Key<T> key,
        final FlightServerMiddleware.Factory<T> factory) {
      if (interceptorKeys.contains(key.key)) {
        throw new IllegalArgumentException("Key already exists: " + key.key);
      }
      interceptors.add(new KeyFactory<>(key, factory));
      interceptorKeys.add(key.key);
      return this;
    }

    public Builder allocator(BufferAllocator allocator) {
      this.allocator = Preconditions.checkNotNull(allocator);
      return this;
    }

    public Builder location(Location location) {
      this.location = Preconditions.checkNotNull(location);
      return this;
    }

    public Builder producer(FlightProducer producer) {
      this.producer = Preconditions.checkNotNull(producer);
      return this;
    }
  }
}
