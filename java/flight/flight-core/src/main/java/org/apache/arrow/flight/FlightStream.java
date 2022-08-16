package org.apache.arrow.flight;

import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.concurrent.CompletableFuture;

public interface FlightStream extends AutoCloseable {
    Schema getSchema();
    DictionaryProvider getDictionaryProvider();
    DictionaryProvider takeDictionaryOwnership();
    FlightDescriptor getDescriptor();
    void close() throws Exception;
    boolean next();
    VectorSchemaRoot getRoot();
    boolean hasRoot();
    ArrowBuf getLatestMetadata();
    void cancel(String message, Throwable exception);

    CompletableFuture<Void> cancelled();
    CompletableFuture<Void> completed();

    StreamObserver<ArrowMessage> asObserver();

    /**
     * Provides a callback to cancel a process that is in progress.
     */
    @FunctionalInterface
    interface Cancellable {
        void cancel(String message, Throwable exception);
    }

    /**
     * Provides a interface to request more items from a stream producer.
     */
    @FunctionalInterface
    interface Requestor {
        /**
         * Requests <code>count</code> more messages from the instance of this object.
         */
        void request(int count);
    }
}
