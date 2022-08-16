package org.apache.arrow.driver.jdbc;

import org.apache.arrow.driver.jdbc.utils.FlightStreamQueue;
import org.apache.arrow.flight.ArrowMessage;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class FlightClientCloser implements FlightStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightClientCloser.class);

    private final FlightSqlClient client;
    private final FlightStream stream;

    public FlightClientCloser(FlightSqlClient client, FlightStream stream) {
        this.client = client;
        this.stream = stream;
    }

    @Override
    public Schema getSchema() {
        return stream.getSchema();
    }

    @Override
    public DictionaryProvider getDictionaryProvider() {
        return stream.getDictionaryProvider();
    }

    @Override
    public DictionaryProvider takeDictionaryOwnership() {
        return stream.takeDictionaryOwnership();
    }

    @Override
    public FlightDescriptor getDescriptor() {
        return stream.getDescriptor();
    }

    @Override
    public void close() throws Exception {
        Exception e = null;
        try {
            stream.close();
            LOGGER.debug("Closed stream");
        } catch (Exception ex) {
            e = ex;
        }
        try {
            client.close();
            LOGGER.debug("Closed client");
        } catch (Exception ex) {
            e = ex;
        }
        if(e != null) {
            throw e;
        }
    }

    @Override
    public boolean next() {
        return stream.next();
    }

    @Override
    public VectorSchemaRoot getRoot() {
        return stream.getRoot();
    }

    @Override
    public boolean hasRoot() {
        return stream.hasRoot();
    }

    @Override
    public ArrowBuf getLatestMetadata() {
        return stream.getLatestMetadata();
    }

    @Override
    public void cancel(String message, Throwable exception) {
        stream.cancel(message, exception);
    }

    @Override
    public CompletableFuture<Void> cancelled() {
        return stream.cancelled();
    }

    @Override
    public CompletableFuture<Void> completed() {
        return stream.completed();
    }

    @Override
    public StreamObserver<ArrowMessage> asObserver() {
        return stream.asObserver();
    }
}
