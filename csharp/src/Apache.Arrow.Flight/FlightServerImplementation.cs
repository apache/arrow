using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Flight.Writer;
using Grpc.Core;

namespace Apache.Arrow.Flight.Client
{
    public class FlightServerImplementation : FlightService.FlightServiceBase
    {
        private readonly IFlightServer _flightServer;
        public FlightServerImplementation(IFlightServer flightServer)
        {
            _flightServer = flightServer;
        }

        public override async Task DoPut(IAsyncStreamReader<FlightData> requestStream, IServerStreamWriter<Protocol.PutResult> responseStream, ServerCallContext context)
        {
            var readStream = new RecordBatchStreamReader(requestStream);
            var writeStream = new StreamWriter<PutResult, Protocol.PutResult>(responseStream, putResult => putResult.ToProtocol());
            await _flightServer.DoPut(readStream, writeStream, context);
        }

        public override Task DoGet(Protocol.Ticket request, IServerStreamWriter<FlightData> responseStream, ServerCallContext context)
        {
            return _flightServer.DoGet(new Ticket(request.Ticket_), new ServerRecordBatchStreamWriter(responseStream), context);
        }

        public override Task ListFlights(Protocol.Criteria request, IServerStreamWriter<Protocol.FlightInfo> responseStream, ServerCallContext context)
        {
            var writeStream = new StreamWriter<FlightInfo, Protocol.FlightInfo>(responseStream, flightInfo => flightInfo.ToProtocol());
            return _flightServer.ListFlights(new Criteria(request), writeStream, context);
        }

        public override Task DoAction(Protocol.Action request, IServerStreamWriter<Protocol.Result> responseStream, ServerCallContext context)
        {
            var action = new Action(request);
            var writeStream = new StreamWriter<Result, Protocol.Result>(responseStream, result => result.ToProtocol());
            return _flightServer.DoAction(action, writeStream, context);
        }

        public override async Task<SchemaResult> GetSchema(Protocol.FlightDescriptor request, ServerCallContext context)
        {
            var flightDescriptor = FlightDescriptor.FromProtocol(request);
            var schema = await _flightServer.GetSchema(flightDescriptor, context);

            return new SchemaResult()
            {
                Schema = SchemaWriter.SerializeSchema(schema)
            };
        }

        public override async Task<Protocol.FlightInfo> GetFlightInfo(Protocol.FlightDescriptor request, ServerCallContext context)
        {
            var flightDescriptor = FlightDescriptor.FromProtocol(request);
            var flightInfo = await _flightServer.GetFlightInfo(flightDescriptor, context);

            return flightInfo.ToProtocol();
        }

        public override Task DoExchange(IAsyncStreamReader<FlightData> requestStream, IServerStreamWriter<FlightData> responseStream, ServerCallContext context)
        {
            //Exchange is not yet implemented
            throw new NotImplementedException();
        }

        public override Task Handshake(IAsyncStreamReader<HandshakeRequest> requestStream, IServerStreamWriter<HandshakeResponse> responseStream, ServerCallContext context)
        {
            //Handshake is not yet implemented
            throw new NotImplementedException();
        }

        public override Task ListActions(Empty request, IServerStreamWriter<Protocol.ActionType> responseStream, ServerCallContext context)
        {
            var writeStream = new StreamWriter<ActionType, Protocol.ActionType>(responseStream, (actionType) => actionType.ToProtocol());
            return _flightServer.ListActions(writeStream, context);
        }
    }
}
