import * as grpc from '@grpc/grpc-js';
import { MessageType } from "@protobuf-ts/runtime";
import * as Arrow from "apache-arrow/src/Arrow.node";
import { Schema } from "apache-arrow/src/Arrow.node";
import { Builder } from "flatbuffers";
import { Any } from './ts-proto/any.js';
import { Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightDescriptor_DescriptorType, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, Result, SchemaResult, Ticket } from './ts-proto/Flight';
import { FlightServiceClient } from "./ts-proto/Flight.grpc-client.js";
import { flightServiceDefinition, IFlightService } from "./ts-proto/Flight.grpc-server.js";
import { ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTables, CommandGetTableTypes, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate } from './ts-proto/FlightSql.js';


const GRPC_ENDPOINT = "0.0.0.0:50051";

function schemaToFlatbuffer(schema: Schema): Uint8Array {
    const builder = new Builder();
    const schemaOffset = Arrow.Schema.encode(builder, schema);
    builder.finish(schemaOffset)
    const byteBuffer = builder.dataBuffer()
    return byteBuffer.bytes()
}

const service: IFlightService = {
    handshake: function (call: grpc.ServerDuplexStream<HandshakeRequest, HandshakeResponse>): void {
        throw new Error("Function not implemented.");
    },
    listFlights: function (call: grpc.ServerWritableStream<Criteria, FlightInfo>): void {
        throw new Error("Function not implemented.");
    },
    getFlightInfo: function (call: grpc.ServerUnaryCall<FlightDescriptor, FlightInfo>, callback: grpc.sendUnaryData<FlightInfo>): void {
        console.log("GetFlightInfo called");

        const flightDescriptor = call.request;
        console.log("FlightDescriptor", flightDescriptor);

        function flightDescriptorIs<T extends object>(clazz: MessageType<T>) {
            return Any.contains(Any.fromBinary(flightDescriptor.cmd), clazz);
        }

        function unpackFlightDescriptor<T extends object>(clazz: MessageType<T>) {
            return Any.unpack(Any.fromBinary(flightDescriptor.cmd), clazz);
        }

        if (flightDescriptorIs(CommandGetCatalogs)) {
            const cmd = unpackFlightDescriptor(CommandGetCatalogs);
            console.log("CommandGetCatalogs", cmd);
        }

        if (flightDescriptorIs(CommandGetCrossReference)) {
            const cmd = unpackFlightDescriptor(CommandGetCrossReference);
            console.log("CommandGetCrossReference", cmd);
        }

        if (flightDescriptorIs(CommandGetDbSchemas)) {
            const cmd = unpackFlightDescriptor(CommandGetDbSchemas);
            console.log("CommandGetDbSchemas", cmd);
        }

        if (flightDescriptorIs(CommandGetExportedKeys)) {
            const cmd = unpackFlightDescriptor(CommandGetExportedKeys);
            console.log("CommandGetExportedKeys", cmd);
        }

        if (flightDescriptorIs(CommandGetImportedKeys)) {
            const cmd = unpackFlightDescriptor(CommandGetImportedKeys);
            console.log("CommandGetImportedKeys", cmd);
        }

        if (flightDescriptorIs(CommandGetPrimaryKeys)) {
            const cmd = unpackFlightDescriptor(CommandGetPrimaryKeys);
            console.log("CommandGetPrimaryKeys", cmd);
        }

        if (flightDescriptorIs(CommandGetSqlInfo)) {
            const cmd = unpackFlightDescriptor(CommandGetSqlInfo);
            console.log("CommandGetSqlInfo", cmd);
        }

        if (flightDescriptorIs(CommandGetTables)) {
            const cmd = unpackFlightDescriptor(CommandGetTables);
            console.log("CommandGetTables", cmd);

            const schema = new Arrow.Schema([
                new Arrow.Field("table_cat", new Arrow.Utf8()),
            ])

            callback(null, {
                endpoint: [{
                    location: [{ uri: GRPC_ENDPOINT }],
                    ticket: {
                        ticket: Buffer.from("1234567890"),
                    }
                }],
                schema: schemaToFlatbuffer(schema),
                totalBytes: BigInt(-1),
                totalRecords: BigInt(-1),
            })
        }

        if (flightDescriptorIs(CommandGetTableTypes)) {
            const cmd = unpackFlightDescriptor(CommandGetTableTypes);
            console.log("CommandGetTableTypes", cmd);
        }

        if (flightDescriptorIs(CommandStatementQuery)) {
            const cmd = unpackFlightDescriptor(CommandStatementQuery);
            console.log("CommandStatementQuery", cmd);
        }

        if (flightDescriptorIs(CommandStatementUpdate)) {
            const cmd = unpackFlightDescriptor(CommandStatementUpdate);
            console.log("CommandStatementUpdate", cmd);
        }

        if (flightDescriptorIs(ActionClosePreparedStatementRequest)) {
            const cmd = unpackFlightDescriptor(ActionClosePreparedStatementRequest);
            console.log("ActionClosePreparedStatementRequest", cmd);
        }

        if (flightDescriptorIs(ActionCreatePreparedStatementRequest)) {
            const cmd = unpackFlightDescriptor(ActionCreatePreparedStatementRequest);
            console.log("ActionCreatePreparedStatementRequest", cmd);
        }

        if (flightDescriptorIs(CommandPreparedStatementQuery)) {
            const cmd = unpackFlightDescriptor(CommandPreparedStatementQuery);
            console.log("CommandPreparedStatementQuery", cmd);
        }

        if (flightDescriptorIs(CommandPreparedStatementUpdate)) {
            const cmd = unpackFlightDescriptor(CommandPreparedStatementUpdate);
            console.log("CommandPreparedStatementUpdate", cmd);
        }
    },
    getSchema: function (call: grpc.ServerUnaryCall<FlightDescriptor, SchemaResult>, callback: grpc.sendUnaryData<SchemaResult>): void {
        throw new Error("Function not implemented.");
    },
    doGet: function (call: grpc.ServerWritableStream<Ticket, FlightData>): void {
        throw new Error("Function not implemented.");
    },
    doPut: function (call: grpc.ServerDuplexStream<FlightData, PutResult>): void {
        throw new Error("Function not implemented.");
    },
    doExchange: function (call: grpc.ServerDuplexStream<FlightData, FlightData>): void {
        throw new Error("Function not implemented.");
    },
    doAction: function (call: grpc.ServerWritableStream<Action, Result>): void {
        throw new Error("Function not implemented.");
    },
    listActions: function (call: grpc.ServerWritableStream<Empty, ActionType>): void {
        throw new Error("Function not implemented.");
    }
}

async function main() {
    const server = new grpc.Server();
    server.addService(flightServiceDefinition, service);
    server.bindAsync(GRPC_ENDPOINT, grpc.ServerCredentials.createInsecure(), () => {
        console.log("Started gRPC server on " + GRPC_ENDPOINT);
        server.start();
    });

    const client = new FlightServiceClient(GRPC_ENDPOINT, grpc.credentials.createInsecure());

    const commandGetTables: CommandGetTables = {
        includeSchema: true,
        tableTypes: ["TABLE", "VIEW"],
        catalog: "my-catalog",
    }

    client.getFlightInfo({
        path: [],
        type: FlightDescriptor_DescriptorType.CMD,
        cmd: Any.toBinary(Any.pack(commandGetTables, CommandGetTables))
    }, {}, (err, response) => {
        if (err) {
            console.error("Got error:", err);
        } else {
            console.log("Got response", response);

            const ticket = response?.endpoint[0].ticket;
            console.log("Got ticket", ticket?.ticket.toString());

            const schema = response?.schema;
            console.log("Got schema", schema?.toString());

            // Get stream for ticket
            const stream = client.doGet(ticket!!);
            stream.on("data", (data) => {
                console.log("Got data", data);
            }).on('end', () => {
                console.log("Stream ended");
            }).on('error', (err) => {
                console.error("Stream error", err);
            })
        }
    });


}

main().catch(console.error);

