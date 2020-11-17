﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Flight.Writer;
using Grpc.Core;
using Grpc.Net.Client;

namespace Apache.Arrow.Flight.Client
{
    public class FlightClient
    {
        private readonly FlightService.FlightServiceClient _client;
        public FlightClient(GrpcChannel grpcChannel)
        {
            _client = new FlightService.FlightServiceClient(grpcChannel);
        }

        public IAsyncStreamReader<FlightInfo> ListFlights(Criteria criteria = null, Metadata headers = null)
        {
            if(criteria == null)
            {
                criteria = new Criteria();
            }
            
            var response = _client.ListFlights(criteria.ToProtocol(), headers);
            return new StreamReader<Protocol.FlightInfo, FlightInfo>(response.ResponseStream, inFlight => new FlightInfo(inFlight));
        }

        public IAsyncStreamReader<ActionType> ListActions(Metadata headers = null)
        {
            var response = _client.ListActions(new Empty(), headers);
            return new StreamReader<Protocol.ActionType, ActionType>(response.ResponseStream, actionType => new ActionType(actionType));
        }

        public RecordBatchStreamReader GetStream(Ticket ticket, Metadata headers = null)
        {
            var stream = _client.DoGet(ticket.ToProtocol(),  headers);
            return new RecordBatchStreamReader(stream.ResponseStream);
        }

        public async Task<FlightInfo> GetInfo(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            var flightInfoResult = await _client.GetFlightInfoAsync(flightDescriptor.ToProtocol(), headers);
            return new FlightInfo(flightInfoResult);
        }

        public AsyncDuplexStreamingCall<RecordBatch, PutResult> StartPut(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            var channels = _client.DoPut(headers);
            var requestStream = new ClientRecordBatchStreamWriter(channels.RequestStream, flightDescriptor);
            var readStream = new StreamReader<Protocol.PutResult, PutResult>(channels.ResponseStream, putResult => new PutResult(putResult));
            return new AsyncDuplexStreamingCall<RecordBatch, PutResult>(
                requestStream,
                readStream,
                channels.ResponseHeadersAsync,
                channels.GetStatus,
                channels.GetTrailers,
                channels.Dispose);
        }

        public AsyncServerStreamingCall<Result> DoAction(Action action, Metadata headers = null)
        {
            var stream = _client.DoAction(action.ToProtocol(), headers);
            var streamReader = new StreamReader<Protocol.Result, Result>(stream.ResponseStream, result => new Result(result));
            return new AsyncServerStreamingCall<Result>(streamReader, stream.ResponseHeadersAsync, stream.GetStatus, stream.GetTrailers, stream.Dispose);
        }

        public async Task<Schema> GetSchema(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            var schemaResult = await _client.GetSchemaAsync(flightDescriptor.ToProtocol(), headers);
            return FlightMessageSerializer.DecodeSchema(schemaResult.Schema.Memory);
        }
    }
}
