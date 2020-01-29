use std::pin::Pin;

use futures::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use datafusion::execution::context::ExecutionContext;

use arrow::record_batch::RecordBatch;
use flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};

#[derive(Clone)]
pub struct FlightServiceImpl {}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = Pin<
        Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>,
    >;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream = Pin<
        Box<dyn Stream<Item = Result<flight::Result, Status>> + Send + Sync + 'static>,
    >;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        match String::from_utf8(ticket.ticket.to_vec()) {
            Ok(sql) => {
                println!("do_get: {}", sql);

                // create local execution context
                let mut ctx = ExecutionContext::new();

                ctx.register_parquet("alltypes_plain", "alltypes_plain.snappy.parquet")
                    .unwrap();

                // create the query plan
                let plan = ctx
                    .create_logical_plan(&sql)
                    .map_err(|e| to_tonic_err(&e))?;
                let plan = ctx.optimize(&plan).map_err(|e| to_tonic_err(&e))?;
                let plan = ctx
                    .create_physical_plan(&plan, 1024 * 1024)
                    .map_err(|e| to_tonic_err(&e))?;

                //TODO make this async

                // execute the query
                let results = ctx.collect(plan.as_ref()).map_err(|e| to_tonic_err(&e))?;

                let flights: Vec<Result<FlightData, Status>> =
                    results.iter().map(|batch| to_flight_data(batch)).collect();

                let output = futures::stream::iter(flights);

                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            Err(e) => Err(Status::unimplemented(format!("Invalid ticket: {:?}", e))),
        }
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

fn to_flight_data(_batch: &RecordBatch) -> Result<FlightData, Status> {
    //TODO implement .. need help on how to encode the batches using IPC here

    Ok(FlightData {
        flight_descriptor: None,
        app_metadata: vec![],
        /// Header for message data as described in Message.fbs::Message
        data_header: vec![],
        /// The actual batch of Arrow data. Preferably handled with minimal-copies
        /// coming last in the definition to help with sidecar patterns (it is
        /// expected that some implementations will fetch this field off the wire
        /// with specialized code to avoid extra memory copies).
        ///
        data_body: vec![],
    })
}

fn to_tonic_err(e: &datafusion::error::ExecutionError) -> Status {
    Status::unimplemented(format!("{:?}", e))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = FlightServiceImpl {};

    let svc = FlightServiceServer::new(service);

    println!("Listening on {:?}", addr);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
