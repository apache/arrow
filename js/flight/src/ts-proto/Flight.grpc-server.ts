// @generated by protobuf-ts 2.2.2 with parameter server_grpc1,client_grpc1
// @generated from protobuf file "Flight.proto" (package "arrow.flight.protocol", syntax proto3)
// tslint:disable
//
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// <p>
// http://www.apache.org/licenses/LICENSE-2.0
// <p>
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
import { ActionType } from "./Flight";
import { Empty } from "./Flight";
import { Result } from "./Flight";
import { Action } from "./Flight";
import { PutResult } from "./Flight";
import { FlightData } from "./Flight";
import { Ticket } from "./Flight";
import { SchemaResult } from "./Flight";
import { FlightDescriptor } from "./Flight";
import { FlightInfo } from "./Flight";
import { Criteria } from "./Flight";
import { HandshakeResponse } from "./Flight";
import { HandshakeRequest } from "./Flight";
import * as grpc from "@grpc/grpc-js";
/**
 *
 * A flight service is an endpoint for retrieving or storing Arrow data. A
 * flight service can expose one or more predefined endpoints that can be
 * accessed using the Arrow Flight Protocol. Additionally, a flight service
 * can expose a set of actions that are available.
 *
 * @generated from protobuf service arrow.flight.protocol.FlightService
 */
export interface IFlightService extends grpc.UntypedServiceImplementation {
    /**
     *
     * Handshake between client and server. Depending on the server, the
     * handshake may be required to determine the token that should be used for
     * future operations. Both request and response are streams to allow multiple
     * round-trips depending on auth mechanism.
     *
     * @generated from protobuf rpc: Handshake(stream arrow.flight.protocol.HandshakeRequest) returns (stream arrow.flight.protocol.HandshakeResponse);
     */
    handshake: grpc.handleBidiStreamingCall<HandshakeRequest, HandshakeResponse>;
    /**
     *
     * Get a list of available streams given a particular criteria. Most flight
     * services will expose one or more streams that are readily available for
     * retrieval. This api allows listing the streams available for
     * consumption. A user can also provide a criteria. The criteria can limit
     * the subset of streams that can be listed via this interface. Each flight
     * service allows its own definition of how to consume criteria.
     *
     * @generated from protobuf rpc: ListFlights(arrow.flight.protocol.Criteria) returns (stream arrow.flight.protocol.FlightInfo);
     */
    listFlights: grpc.handleServerStreamingCall<Criteria, FlightInfo>;
    /**
     *
     * For a given FlightDescriptor, get information about how the flight can be
     * consumed. This is a useful interface if the consumer of the interface
     * already can identify the specific flight to consume. This interface can
     * also allow a consumer to generate a flight stream through a specified
     * descriptor. For example, a flight descriptor might be something that
     * includes a SQL statement or a Pickled Python operation that will be
     * executed. In those cases, the descriptor will not be previously available
     * within the list of available streams provided by ListFlights but will be
     * available for consumption for the duration defined by the specific flight
     * service.
     *
     * @generated from protobuf rpc: GetFlightInfo(arrow.flight.protocol.FlightDescriptor) returns (arrow.flight.protocol.FlightInfo);
     */
    getFlightInfo: grpc.handleUnaryCall<FlightDescriptor, FlightInfo>;
    /**
     *
     * For a given FlightDescriptor, get the Schema as described in Schema.fbs::Schema
     * This is used when a consumer needs the Schema of flight stream. Similar to
     * GetFlightInfo this interface may generate a new flight that was not previously
     * available in ListFlights.
     *
     * @generated from protobuf rpc: GetSchema(arrow.flight.protocol.FlightDescriptor) returns (arrow.flight.protocol.SchemaResult);
     */
    getSchema: grpc.handleUnaryCall<FlightDescriptor, SchemaResult>;
    /**
     *
     * Retrieve a single stream associated with a particular descriptor
     * associated with the referenced ticket. A Flight can be composed of one or
     * more streams where each stream can be retrieved using a separate opaque
     * ticket that the flight service uses for managing a collection of streams.
     *
     * @generated from protobuf rpc: DoGet(arrow.flight.protocol.Ticket) returns (stream arrow.flight.protocol.FlightData);
     */
    doGet: grpc.handleServerStreamingCall<Ticket, FlightData>;
    /**
     *
     * Push a stream to the flight service associated with a particular
     * flight stream. This allows a client of a flight service to upload a stream
     * of data. Depending on the particular flight service, a client consumer
     * could be allowed to upload a single stream per descriptor or an unlimited
     * number. In the latter, the service might implement a 'seal' action that
     * can be applied to a descriptor once all streams are uploaded.
     *
     * @generated from protobuf rpc: DoPut(stream arrow.flight.protocol.FlightData) returns (stream arrow.flight.protocol.PutResult);
     */
    doPut: grpc.handleBidiStreamingCall<FlightData, PutResult>;
    /**
     *
     * Open a bidirectional data channel for a given descriptor. This
     * allows clients to send and receive arbitrary Arrow data and
     * application-specific metadata in a single logical stream. In
     * contrast to DoGet/DoPut, this is more suited for clients
     * offloading computation (rather than storage) to a Flight service.
     *
     * @generated from protobuf rpc: DoExchange(stream arrow.flight.protocol.FlightData) returns (stream arrow.flight.protocol.FlightData);
     */
    doExchange: grpc.handleBidiStreamingCall<FlightData, FlightData>;
    /**
     *
     * Flight services can support an arbitrary number of simple actions in
     * addition to the possible ListFlights, GetFlightInfo, DoGet, DoPut
     * operations that are potentially available. DoAction allows a flight client
     * to do a specific action against a flight service. An action includes
     * opaque request and response objects that are specific to the type action
     * being undertaken.
     *
     * @generated from protobuf rpc: DoAction(arrow.flight.protocol.Action) returns (stream arrow.flight.protocol.Result);
     */
    doAction: grpc.handleServerStreamingCall<Action, Result>;
    /**
     *
     * A flight service exposes all of the available action types that it has
     * along with descriptions. This allows different flight consumers to
     * understand the capabilities of the flight service.
     *
     * @generated from protobuf rpc: ListActions(arrow.flight.protocol.Empty) returns (stream arrow.flight.protocol.ActionType);
     */
    listActions: grpc.handleServerStreamingCall<Empty, ActionType>;
}
/**
 * @grpc/grpc-js definition for the protobuf service arrow.flight.protocol.FlightService.
 *
 * Usage: Implement the interface IFlightService and add to a grpc server.
 *
 * ```typescript
 * const server = new grpc.Server();
 * const service: IFlightService = ...
 * server.addService(flightServiceDefinition, service);
 * ```
 */
export const flightServiceDefinition: grpc.ServiceDefinition<IFlightService> = {
    handshake: {
        path: "/arrow.flight.protocol.FlightService/Handshake",
        originalName: "Handshake",
        requestStream: true,
        responseStream: true,
        responseDeserialize: bytes => HandshakeResponse.fromBinary(bytes),
        requestDeserialize: bytes => HandshakeRequest.fromBinary(bytes),
        responseSerialize: value => Buffer.from(HandshakeResponse.toBinary(value)),
        requestSerialize: value => Buffer.from(HandshakeRequest.toBinary(value))
    },
    listFlights: {
        path: "/arrow.flight.protocol.FlightService/ListFlights",
        originalName: "ListFlights",
        requestStream: false,
        responseStream: true,
        responseDeserialize: bytes => FlightInfo.fromBinary(bytes),
        requestDeserialize: bytes => Criteria.fromBinary(bytes),
        responseSerialize: value => Buffer.from(FlightInfo.toBinary(value)),
        requestSerialize: value => Buffer.from(Criteria.toBinary(value))
    },
    getFlightInfo: {
        path: "/arrow.flight.protocol.FlightService/GetFlightInfo",
        originalName: "GetFlightInfo",
        requestStream: false,
        responseStream: false,
        responseDeserialize: bytes => FlightInfo.fromBinary(bytes),
        requestDeserialize: bytes => FlightDescriptor.fromBinary(bytes),
        responseSerialize: value => Buffer.from(FlightInfo.toBinary(value)),
        requestSerialize: value => Buffer.from(FlightDescriptor.toBinary(value))
    },
    getSchema: {
        path: "/arrow.flight.protocol.FlightService/GetSchema",
        originalName: "GetSchema",
        requestStream: false,
        responseStream: false,
        responseDeserialize: bytes => SchemaResult.fromBinary(bytes),
        requestDeserialize: bytes => FlightDescriptor.fromBinary(bytes),
        responseSerialize: value => Buffer.from(SchemaResult.toBinary(value)),
        requestSerialize: value => Buffer.from(FlightDescriptor.toBinary(value))
    },
    doGet: {
        path: "/arrow.flight.protocol.FlightService/DoGet",
        originalName: "DoGet",
        requestStream: false,
        responseStream: true,
        responseDeserialize: bytes => FlightData.fromBinary(bytes),
        requestDeserialize: bytes => Ticket.fromBinary(bytes),
        responseSerialize: value => Buffer.from(FlightData.toBinary(value)),
        requestSerialize: value => Buffer.from(Ticket.toBinary(value))
    },
    doPut: {
        path: "/arrow.flight.protocol.FlightService/DoPut",
        originalName: "DoPut",
        requestStream: true,
        responseStream: true,
        responseDeserialize: bytes => PutResult.fromBinary(bytes),
        requestDeserialize: bytes => FlightData.fromBinary(bytes),
        responseSerialize: value => Buffer.from(PutResult.toBinary(value)),
        requestSerialize: value => Buffer.from(FlightData.toBinary(value))
    },
    doExchange: {
        path: "/arrow.flight.protocol.FlightService/DoExchange",
        originalName: "DoExchange",
        requestStream: true,
        responseStream: true,
        responseDeserialize: bytes => FlightData.fromBinary(bytes),
        requestDeserialize: bytes => FlightData.fromBinary(bytes),
        responseSerialize: value => Buffer.from(FlightData.toBinary(value)),
        requestSerialize: value => Buffer.from(FlightData.toBinary(value))
    },
    doAction: {
        path: "/arrow.flight.protocol.FlightService/DoAction",
        originalName: "DoAction",
        requestStream: false,
        responseStream: true,
        responseDeserialize: bytes => Result.fromBinary(bytes),
        requestDeserialize: bytes => Action.fromBinary(bytes),
        responseSerialize: value => Buffer.from(Result.toBinary(value)),
        requestSerialize: value => Buffer.from(Action.toBinary(value))
    },
    listActions: {
        path: "/arrow.flight.protocol.FlightService/ListActions",
        originalName: "ListActions",
        requestStream: false,
        responseStream: true,
        responseDeserialize: bytes => ActionType.fromBinary(bytes),
        requestDeserialize: bytes => Empty.fromBinary(bytes),
        responseSerialize: value => Buffer.from(ActionType.toBinary(value)),
        requestSerialize: value => Buffer.from(Empty.toBinary(value))
    }
};
