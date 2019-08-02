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

package org.apache.arrow.flight.grpc;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

/**
 * Utilities to adapt gRPC and Flight status objects.
 *
 * <p>NOT A PUBLIC CLASS, interface is not guaranteed to remain stable.
 */
public class StatusUtils {

  private StatusUtils() {
    throw new AssertionError("Do not instantiate this class.");
  }

  /**
   * Convert from a Flight status code to a gRPC status code.
   */
  public static Status.Code toGrpcStatusCode(FlightStatusCode code) {
    switch (code) {
      case UNKNOWN:
        return Code.UNKNOWN;
      case INTERNAL:
        return Code.INTERNAL;
      case INVALID_ARGUMENT:
        return Code.INVALID_ARGUMENT;
      case TIMED_OUT:
        return Code.DEADLINE_EXCEEDED;
      case NOT_FOUND:
        return Code.NOT_FOUND;
      case ALREADY_EXISTS:
        return Code.ALREADY_EXISTS;
      case CANCELLED:
        return Code.CANCELLED;
      case UNAUTHENTICATED:
        return Code.UNAUTHENTICATED;
      case UNAUTHORIZED:
        return Code.PERMISSION_DENIED;
      case UNIMPLEMENTED:
        return Code.UNIMPLEMENTED;
      case UNAVAILABLE:
        return Code.UNAVAILABLE;
      default:
        return Code.UNKNOWN;
    }
  }

  /**
   * Convert from a gRPC status code to a Flight status code.
   */
  public static FlightStatusCode fromGrpcStatusCode(Status.Code code) {
    switch (code) {
      case CANCELLED:
        return FlightStatusCode.CANCELLED;
      case UNKNOWN:
        return FlightStatusCode.UNKNOWN;
      case INVALID_ARGUMENT:
        return FlightStatusCode.INVALID_ARGUMENT;
      case DEADLINE_EXCEEDED:
        return FlightStatusCode.TIMED_OUT;
      case NOT_FOUND:
        return FlightStatusCode.NOT_FOUND;
      case ALREADY_EXISTS:
        return FlightStatusCode.ALREADY_EXISTS;
      case PERMISSION_DENIED:
        return FlightStatusCode.UNAUTHORIZED;
      case RESOURCE_EXHAUSTED:
        return FlightStatusCode.INVALID_ARGUMENT;
      case FAILED_PRECONDITION:
        return FlightStatusCode.INVALID_ARGUMENT;
      case ABORTED:
        return FlightStatusCode.INTERNAL;
      case OUT_OF_RANGE:
        return FlightStatusCode.INVALID_ARGUMENT;
      case UNIMPLEMENTED:
        return FlightStatusCode.UNIMPLEMENTED;
      case INTERNAL:
        return FlightStatusCode.INTERNAL;
      case UNAVAILABLE:
        return FlightStatusCode.UNAVAILABLE;
      case DATA_LOSS:
        return FlightStatusCode.INTERNAL;
      case UNAUTHENTICATED:
        return FlightStatusCode.UNAUTHENTICATED;
      default:
        return FlightStatusCode.UNKNOWN;
    }
  }

  /** Convert from a gRPC status to a Flight status. */
  public static CallStatus fromGrpcStatus(Status status) {
    return new CallStatus(fromGrpcStatusCode(status.getCode()), status.getCause(), status.getDescription());
  }

  /** Convert from a Flight status to a gRPC status. */
  public static Status toGrpcStatus(CallStatus status) {
    return toGrpcStatusCode(status.code()).toStatus().withDescription(status.description()).withCause(status.cause());
  }

  /** Convert from a gRPC exception to a Flight exception. */
  public static FlightRuntimeException fromGrpcRuntimeException(StatusRuntimeException sre) {
    return fromGrpcStatus(sre.getStatus()).toRuntimeException();
  }

  /**
   * Convert arbitrary exceptions to a {@link FlightRuntimeException}.
   */
  public static FlightRuntimeException fromThrowable(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      return fromGrpcRuntimeException((StatusRuntimeException) t);
    } else if (t instanceof FlightRuntimeException) {
      return (FlightRuntimeException) t;
    }
    return CallStatus.UNKNOWN.withCause(t).withDescription(t.getMessage()).toRuntimeException();
  }

  /**
   * Convert arbitrary exceptions to a {@link StatusRuntimeException} or {@link StatusException}.
   *
   * <p>Such exceptions can be passed to {@link io.grpc.stub.StreamObserver#onError(Throwable)} and will give the client
   * a reasonable error message.
   */
  public static Throwable toGrpcException(Throwable ex) {
    if (ex instanceof StatusRuntimeException) {
      return ex;
    } else if (ex instanceof StatusException) {
      return ex;
    } else if (ex instanceof FlightRuntimeException) {
      final FlightRuntimeException fre = (FlightRuntimeException) ex;
      return toGrpcStatus(fre.status()).asRuntimeException();
    }
    return Status.INTERNAL.withCause(ex).withDescription("There was an error servicing your request.")
        .asRuntimeException();
  }

  /**
   * Maps a transformation function to the elements of an iterator, while wrapping exceptions in {@link
   * FlightRuntimeException}.
   */
  public static <FROM, TO> Iterator<TO> wrapIterator(Iterator<FROM> fromIterator,
      Function<? super FROM, ? extends TO> transformer) {
    Objects.requireNonNull(fromIterator);
    Objects.requireNonNull(transformer);
    return new Iterator<TO>() {
      @Override
      public boolean hasNext() {
        try {
          return fromIterator.hasNext();
        } catch (StatusRuntimeException e) {
          throw fromGrpcRuntimeException(e);
        }
      }

      @Override
      public TO next() {
        try {
          return transformer.apply(fromIterator.next());
        } catch (StatusRuntimeException e) {
          throw fromGrpcRuntimeException(e);
        }
      }
    };
  }
}
