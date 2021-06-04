package org.apache.arrow.driver.jdbc.test;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;
import java.util.function.Function;

public class FlightTestUtils {

    private static final Random RANDOM = new Random();

    public static final String LOCALHOST = "localhost";
    private static final String USERNAME_1 = "flight1";
    private static final String USERNAME_2 = "flight2";
    private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);


    /**
     * Returns a a FlightServer (actually anything that is startable)
     * that has been started bound to a random port.
     */
    public static <T> T getStartedServer(Function<Location, T> newServerFromLocation) throws IOException {
        IOException lastThrown = null;
        T server = null;
        for (int x = 0; x < 3; x++) {
            final int port = 49152 + RANDOM.nextInt(5000);
            final Location location = Location.forGrpcInsecure(LOCALHOST, port);
            lastThrown = null;
            try {
                server = newServerFromLocation.apply(location);
                try {
                    server.getClass().getMethod("start").invoke(server);
                } catch (NoSuchMethodException | IllegalAccessException e) {
                    throw new IllegalArgumentException("Couldn't call start method on object.", e);
                }
                break;
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof IOException) {
                    lastThrown = (IOException) e.getTargetException();
                } else {
                    throw (RuntimeException) e.getTargetException();
                }
            }
        }
        if (lastThrown != null) {
            throw lastThrown;
        }
        return server;
    }

    public static FlightProducer getFlightProducer() {
        return new NoOpFlightProducer() {
            @Override
            public void listFlights(CallContext context, Criteria criteria,
                                    StreamListener<FlightInfo> listener) {
                if (!context.peerIdentity().equals(USERNAME_1) && !context.peerIdentity().equals(USERNAME_2)) {
                    listener.onError(new IllegalArgumentException("Invalid username"));
                    return;
                }
                listener.onCompleted();
            }

            @Override
            public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                if (!context.peerIdentity().equals(USERNAME_1) && !context.peerIdentity().equals(USERNAME_2)) {
                    listener.error(new IllegalArgumentException("Invalid username"));
                    return;
                }
                final Schema pojoSchema = new Schema(ImmutableList.of(Field.nullable("a",
                        Types.MinorType.BIGINT.getType())));
                try (VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, allocator)) {
                    listener.start(root);
                    root.allocateNew();
                    root.setRowCount(4095);
                    listener.putNext();
                    listener.completed();
                }
            }
        };
    }
}
