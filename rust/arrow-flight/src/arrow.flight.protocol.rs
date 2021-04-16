// This file was automatically generated through the build.rs script, and should not be edited.

///
/// The request that a client provides to a server on handshake.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HandshakeRequest {
    ///
    /// A defined protocol version
    #[prost(uint64, tag = "1")]
    pub protocol_version: u64,
    ///
    /// Arbitrary auth/handshake info.
    #[prost(bytes = "vec", tag = "2")]
    pub payload: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HandshakeResponse {
    ///
    /// A defined protocol version
    #[prost(uint64, tag = "1")]
    pub protocol_version: u64,
    ///
    /// Arbitrary auth/handshake info.
    #[prost(bytes = "vec", tag = "2")]
    pub payload: ::prost::alloc::vec::Vec<u8>,
}
///
/// A message for doing simple auth.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BasicAuth {
    #[prost(string, tag = "2")]
    pub username: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub password: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Empty {}
///
/// Describes an available action, including both the name used for execution
/// along with a short description of the purpose of the action.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionType {
    #[prost(string, tag = "1")]
    pub r#type: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
}
///
/// A service specific expression that can be used to return a limited set
/// of available Arrow Flight streams.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Criteria {
    #[prost(bytes = "vec", tag = "1")]
    pub expression: ::prost::alloc::vec::Vec<u8>,
}
///
/// An opaque action specific for the service.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Action {
    #[prost(string, tag = "1")]
    pub r#type: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
///
/// An opaque result returned after executing an action.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Result {
    #[prost(bytes = "vec", tag = "1")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
///
/// Wrap the result of a getSchema call
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaResult {
    /// schema of the dataset as described in Schema.fbs::Schema.
    #[prost(bytes = "vec", tag = "1")]
    pub schema: ::prost::alloc::vec::Vec<u8>,
}
///
/// The name or tag for a Flight. May be used as a way to retrieve or generate
/// a flight or be used to expose a set of previously defined flights.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightDescriptor {
    #[prost(enumeration = "flight_descriptor::DescriptorType", tag = "1")]
    pub r#type: i32,
    ///
    /// Opaque value used to express a command. Should only be defined when
    /// type = CMD.
    #[prost(bytes = "vec", tag = "2")]
    pub cmd: ::prost::alloc::vec::Vec<u8>,
    ///
    /// List of strings identifying a particular dataset. Should only be defined
    /// when type = PATH.
    #[prost(string, repeated, tag = "3")]
    pub path: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Nested message and enum types in `FlightDescriptor`.
pub mod flight_descriptor {
    ///
    /// Describes what type of descriptor is defined.
    #[derive(
        Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration,
    )]
    #[repr(i32)]
    pub enum DescriptorType {
        /// Protobuf pattern, not used.
        Unknown = 0,
        ///
        /// A named path that identifies a dataset. A path is composed of a string
        /// or list of strings describing a particular dataset. This is conceptually
        ///  similar to a path inside a filesystem.
        Path = 1,
        ///
        /// An opaque command to generate a dataset.
        Cmd = 2,
    }
}
///
/// The access coordinates for retrieval of a dataset. With a FlightInfo, a
/// consumer is able to determine how to retrieve a dataset.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightInfo {
    /// schema of the dataset as described in Schema.fbs::Schema.
    #[prost(bytes = "vec", tag = "1")]
    pub schema: ::prost::alloc::vec::Vec<u8>,
    ///
    /// The descriptor associated with this info.
    #[prost(message, optional, tag = "2")]
    pub flight_descriptor: ::core::option::Option<FlightDescriptor>,
    ///
    /// A list of endpoints associated with the flight. To consume the whole
    /// flight, all endpoints must be consumed.
    #[prost(message, repeated, tag = "3")]
    pub endpoint: ::prost::alloc::vec::Vec<FlightEndpoint>,
    /// Set these to -1 if unknown.
    #[prost(int64, tag = "4")]
    pub total_records: i64,
    #[prost(int64, tag = "5")]
    pub total_bytes: i64,
}
///
/// A particular stream or split associated with a flight.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightEndpoint {
    ///
    /// Token used to retrieve this stream.
    #[prost(message, optional, tag = "1")]
    pub ticket: ::core::option::Option<Ticket>,
    ///
    /// A list of URIs where this ticket can be redeemed. If the list is
    /// empty, the expectation is that the ticket can only be redeemed on the
    /// current service where the ticket was generated.
    #[prost(message, repeated, tag = "2")]
    pub location: ::prost::alloc::vec::Vec<Location>,
}
///
/// A location where a Flight service will accept retrieval of a particular
/// stream given a ticket.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Location {
    #[prost(string, tag = "1")]
    pub uri: ::prost::alloc::string::String,
}
///
/// An opaque identifier that the service can use to retrieve a particular
/// portion of a stream.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ticket {
    #[prost(bytes = "vec", tag = "1")]
    pub ticket: ::prost::alloc::vec::Vec<u8>,
}
///
/// A batch of Arrow data as part of a stream of batches.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightData {
    ///
    /// The descriptor of the data. This is only relevant when a client is
    /// starting a new DoPut stream.
    #[prost(message, optional, tag = "1")]
    pub flight_descriptor: ::core::option::Option<FlightDescriptor>,
    ///
    /// Header for message data as described in Message.fbs::Message.
    #[prost(bytes = "vec", tag = "2")]
    pub data_header: ::prost::alloc::vec::Vec<u8>,
    ///
    /// Application-defined metadata.
    #[prost(bytes = "vec", tag = "3")]
    pub app_metadata: ::prost::alloc::vec::Vec<u8>,
    ///
    /// The actual batch of Arrow data. Preferably handled with minimal-copies
    /// coming last in the definition to help with sidecar patterns (it is
    /// expected that some implementations will fetch this field off the wire
    /// with specialized code to avoid extra memory copies).
    #[prost(bytes = "vec", tag = "1000")]
    pub data_body: ::prost::alloc::vec::Vec<u8>,
}
///*
/// The response message associated with the submission of a DoPut.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutResult {
    #[prost(bytes = "vec", tag = "1")]
    pub app_metadata: ::prost::alloc::vec::Vec<u8>,
}
#[doc = r" Generated client implementations."]
pub mod flight_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = ""]
    #[doc = " A flight service is an endpoint for retrieving or storing Arrow data. A"]
    #[doc = " flight service can expose one or more predefined endpoints that can be"]
    #[doc = " accessed using the Arrow Flight Protocol. Additionally, a flight service"]
    #[doc = " can expose a set of actions that are available."]
    pub struct FlightServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl FlightServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> FlightServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(
            inner: T,
            interceptor: impl Into<tonic::Interceptor>,
        ) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = ""]
        #[doc = " Handshake between client and server. Depending on the server, the"]
        #[doc = " handshake may be required to determine the token that should be used for"]
        #[doc = " future operations. Both request and response are streams to allow multiple"]
        #[doc = " round-trips depending on auth mechanism."]
        pub async fn handshake(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::HandshakeRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::HandshakeResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/Handshake",
            );
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = ""]
        #[doc = " Get a list of available streams given a particular criteria. Most flight"]
        #[doc = " services will expose one or more streams that are readily available for"]
        #[doc = " retrieval. This api allows listing the streams available for"]
        #[doc = " consumption. A user can also provide a criteria. The criteria can limit"]
        #[doc = " the subset of streams that can be listed via this interface. Each flight"]
        #[doc = " service allows its own definition of how to consume criteria."]
        pub async fn list_flights(
            &mut self,
            request: impl tonic::IntoRequest<super::Criteria>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::FlightInfo>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/ListFlights",
            );
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        #[doc = ""]
        #[doc = " For a given FlightDescriptor, get information about how the flight can be"]
        #[doc = " consumed. This is a useful interface if the consumer of the interface"]
        #[doc = " already can identify the specific flight to consume. This interface can"]
        #[doc = " also allow a consumer to generate a flight stream through a specified"]
        #[doc = " descriptor. For example, a flight descriptor might be something that"]
        #[doc = " includes a SQL statement or a Pickled Python operation that will be"]
        #[doc = " executed. In those cases, the descriptor will not be previously available"]
        #[doc = " within the list of available streams provided by ListFlights but will be"]
        #[doc = " available for consumption for the duration defined by the specific flight"]
        #[doc = " service."]
        pub async fn get_flight_info(
            &mut self,
            request: impl tonic::IntoRequest<super::FlightDescriptor>,
        ) -> Result<tonic::Response<super::FlightInfo>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/GetFlightInfo",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = ""]
        #[doc = " For a given FlightDescriptor, get the Schema as described in Schema.fbs::Schema"]
        #[doc = " This is used when a consumer needs the Schema of flight stream. Similar to"]
        #[doc = " GetFlightInfo this interface may generate a new flight that was not previously"]
        #[doc = " available in ListFlights."]
        pub async fn get_schema(
            &mut self,
            request: impl tonic::IntoRequest<super::FlightDescriptor>,
        ) -> Result<tonic::Response<super::SchemaResult>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/GetSchema",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = ""]
        #[doc = " Retrieve a single stream associated with a particular descriptor"]
        #[doc = " associated with the referenced ticket. A Flight can be composed of one or"]
        #[doc = " more streams where each stream can be retrieved using a separate opaque"]
        #[doc = " ticket that the flight service uses for managing a collection of streams."]
        pub async fn do_get(
            &mut self,
            request: impl tonic::IntoRequest<super::Ticket>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::FlightData>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/DoGet",
            );
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        #[doc = ""]
        #[doc = " Push a stream to the flight service associated with a particular"]
        #[doc = " flight stream. This allows a client of a flight service to upload a stream"]
        #[doc = " of data. Depending on the particular flight service, a client consumer"]
        #[doc = " could be allowed to upload a single stream per descriptor or an unlimited"]
        #[doc = " number. In the latter, the service might implement a 'seal' action that"]
        #[doc = " can be applied to a descriptor once all streams are uploaded."]
        pub async fn do_put(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::FlightData>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::PutResult>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/DoPut",
            );
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = ""]
        #[doc = " Open a bidirectional data channel for a given descriptor. This"]
        #[doc = " allows clients to send and receive arbitrary Arrow data and"]
        #[doc = " application-specific metadata in a single logical stream. In"]
        #[doc = " contrast to DoGet/DoPut, this is more suited for clients"]
        #[doc = " offloading computation (rather than storage) to a Flight service."]
        pub async fn do_exchange(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::FlightData>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::FlightData>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/DoExchange",
            );
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = ""]
        #[doc = " Flight services can support an arbitrary number of simple actions in"]
        #[doc = " addition to the possible ListFlights, GetFlightInfo, DoGet, DoPut"]
        #[doc = " operations that are potentially available. DoAction allows a flight client"]
        #[doc = " to do a specific action against a flight service. An action includes"]
        #[doc = " opaque request and response objects that are specific to the type action"]
        #[doc = " being undertaken."]
        pub async fn do_action(
            &mut self,
            request: impl tonic::IntoRequest<super::Action>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::Result>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/DoAction",
            );
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        #[doc = ""]
        #[doc = " A flight service exposes all of the available action types that it has"]
        #[doc = " along with descriptions. This allows different flight consumers to"]
        #[doc = " understand the capabilities of the flight service."]
        pub async fn list_actions(
            &mut self,
            request: impl tonic::IntoRequest<super::Empty>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::ActionType>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/arrow.flight.protocol.FlightService/ListActions",
            );
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for FlightServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for FlightServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlightServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod flight_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with FlightServiceServer."]
    #[async_trait]
    pub trait FlightService: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Handshake method."]
        type HandshakeStream: futures_core::Stream<Item = Result<super::HandshakeResponse, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = ""]
        #[doc = " Handshake between client and server. Depending on the server, the"]
        #[doc = " handshake may be required to determine the token that should be used for"]
        #[doc = " future operations. Both request and response are streams to allow multiple"]
        #[doc = " round-trips depending on auth mechanism."]
        async fn handshake(
            &self,
            request: tonic::Request<tonic::Streaming<super::HandshakeRequest>>,
        ) -> Result<tonic::Response<Self::HandshakeStream>, tonic::Status>;
        #[doc = "Server streaming response type for the ListFlights method."]
        type ListFlightsStream: futures_core::Stream<Item = Result<super::FlightInfo, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = ""]
        #[doc = " Get a list of available streams given a particular criteria. Most flight"]
        #[doc = " services will expose one or more streams that are readily available for"]
        #[doc = " retrieval. This api allows listing the streams available for"]
        #[doc = " consumption. A user can also provide a criteria. The criteria can limit"]
        #[doc = " the subset of streams that can be listed via this interface. Each flight"]
        #[doc = " service allows its own definition of how to consume criteria."]
        async fn list_flights(
            &self,
            request: tonic::Request<super::Criteria>,
        ) -> Result<tonic::Response<Self::ListFlightsStream>, tonic::Status>;
        #[doc = ""]
        #[doc = " For a given FlightDescriptor, get information about how the flight can be"]
        #[doc = " consumed. This is a useful interface if the consumer of the interface"]
        #[doc = " already can identify the specific flight to consume. This interface can"]
        #[doc = " also allow a consumer to generate a flight stream through a specified"]
        #[doc = " descriptor. For example, a flight descriptor might be something that"]
        #[doc = " includes a SQL statement or a Pickled Python operation that will be"]
        #[doc = " executed. In those cases, the descriptor will not be previously available"]
        #[doc = " within the list of available streams provided by ListFlights but will be"]
        #[doc = " available for consumption for the duration defined by the specific flight"]
        #[doc = " service."]
        async fn get_flight_info(
            &self,
            request: tonic::Request<super::FlightDescriptor>,
        ) -> Result<tonic::Response<super::FlightInfo>, tonic::Status>;
        #[doc = ""]
        #[doc = " For a given FlightDescriptor, get the Schema as described in Schema.fbs::Schema"]
        #[doc = " This is used when a consumer needs the Schema of flight stream. Similar to"]
        #[doc = " GetFlightInfo this interface may generate a new flight that was not previously"]
        #[doc = " available in ListFlights."]
        async fn get_schema(
            &self,
            request: tonic::Request<super::FlightDescriptor>,
        ) -> Result<tonic::Response<super::SchemaResult>, tonic::Status>;
        #[doc = "Server streaming response type for the DoGet method."]
        type DoGetStream: futures_core::Stream<Item = Result<super::FlightData, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = ""]
        #[doc = " Retrieve a single stream associated with a particular descriptor"]
        #[doc = " associated with the referenced ticket. A Flight can be composed of one or"]
        #[doc = " more streams where each stream can be retrieved using a separate opaque"]
        #[doc = " ticket that the flight service uses for managing a collection of streams."]
        async fn do_get(
            &self,
            request: tonic::Request<super::Ticket>,
        ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status>;
        #[doc = "Server streaming response type for the DoPut method."]
        type DoPutStream: futures_core::Stream<Item = Result<super::PutResult, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = ""]
        #[doc = " Push a stream to the flight service associated with a particular"]
        #[doc = " flight stream. This allows a client of a flight service to upload a stream"]
        #[doc = " of data. Depending on the particular flight service, a client consumer"]
        #[doc = " could be allowed to upload a single stream per descriptor or an unlimited"]
        #[doc = " number. In the latter, the service might implement a 'seal' action that"]
        #[doc = " can be applied to a descriptor once all streams are uploaded."]
        async fn do_put(
            &self,
            request: tonic::Request<tonic::Streaming<super::FlightData>>,
        ) -> Result<tonic::Response<Self::DoPutStream>, tonic::Status>;
        #[doc = "Server streaming response type for the DoExchange method."]
        type DoExchangeStream: futures_core::Stream<Item = Result<super::FlightData, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = ""]
        #[doc = " Open a bidirectional data channel for a given descriptor. This"]
        #[doc = " allows clients to send and receive arbitrary Arrow data and"]
        #[doc = " application-specific metadata in a single logical stream. In"]
        #[doc = " contrast to DoGet/DoPut, this is more suited for clients"]
        #[doc = " offloading computation (rather than storage) to a Flight service."]
        async fn do_exchange(
            &self,
            request: tonic::Request<tonic::Streaming<super::FlightData>>,
        ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status>;
        #[doc = "Server streaming response type for the DoAction method."]
        type DoActionStream: futures_core::Stream<Item = Result<super::Result, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = ""]
        #[doc = " Flight services can support an arbitrary number of simple actions in"]
        #[doc = " addition to the possible ListFlights, GetFlightInfo, DoGet, DoPut"]
        #[doc = " operations that are potentially available. DoAction allows a flight client"]
        #[doc = " to do a specific action against a flight service. An action includes"]
        #[doc = " opaque request and response objects that are specific to the type action"]
        #[doc = " being undertaken."]
        async fn do_action(
            &self,
            request: tonic::Request<super::Action>,
        ) -> Result<tonic::Response<Self::DoActionStream>, tonic::Status>;
        #[doc = "Server streaming response type for the ListActions method."]
        type ListActionsStream: futures_core::Stream<Item = Result<super::ActionType, tonic::Status>>
            + Send
            + Sync
            + 'static;
        #[doc = ""]
        #[doc = " A flight service exposes all of the available action types that it has"]
        #[doc = " along with descriptions. This allows different flight consumers to"]
        #[doc = " understand the capabilities of the flight service."]
        async fn list_actions(
            &self,
            request: tonic::Request<super::Empty>,
        ) -> Result<tonic::Response<Self::ListActionsStream>, tonic::Status>;
    }
    #[doc = ""]
    #[doc = " A flight service is an endpoint for retrieving or storing Arrow data. A"]
    #[doc = " flight service can expose one or more predefined endpoints that can be"]
    #[doc = " accessed using the Arrow Flight Protocol. Additionally, a flight service"]
    #[doc = " can expose a set of actions that are available."]
    #[derive(Debug)]
    pub struct FlightServiceServer<T: FlightService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: FlightService> FlightServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(
            inner: T,
            interceptor: impl Into<tonic::Interceptor>,
        ) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for FlightServiceServer<T>
    where
        T: FlightService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/arrow.flight.protocol.FlightService/Handshake" => {
                    #[allow(non_camel_case_types)]
                    struct HandshakeSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::StreamingService<super::HandshakeRequest>
                        for HandshakeSvc<T>
                    {
                        type Response = super::HandshakeResponse;
                        type ResponseStream = T::HandshakeStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::HandshakeRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).handshake(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = HandshakeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/arrow.flight.protocol.FlightService/ListFlights" => {
                    #[allow(non_camel_case_types)]
                    struct ListFlightsSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::ServerStreamingService<super::Criteria>
                        for ListFlightsSvc<T>
                    {
                        type Response = super::FlightInfo;
                        type ResponseStream = T::ListFlightsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::Criteria>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).list_flights(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = ListFlightsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/arrow.flight.protocol.FlightService/GetFlightInfo" => {
                    #[allow(non_camel_case_types)]
                    struct GetFlightInfoSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::UnaryService<super::FlightDescriptor>
                        for GetFlightInfoSvc<T>
                    {
                        type Response = super::FlightInfo;
                        type Future =
                            BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FlightDescriptor>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut =
                                async move { (*inner).get_flight_info(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = GetFlightInfoSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/arrow.flight.protocol.FlightService/GetSchema" => {
                    #[allow(non_camel_case_types)]
                    struct GetSchemaSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::UnaryService<super::FlightDescriptor>
                        for GetSchemaSvc<T>
                    {
                        type Response = super::SchemaResult;
                        type Future =
                            BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FlightDescriptor>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_schema(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = GetSchemaSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/arrow.flight.protocol.FlightService/DoGet" => {
                    #[allow(non_camel_case_types)]
                    struct DoGetSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::ServerStreamingService<super::Ticket>
                        for DoGetSvc<T>
                    {
                        type Response = super::FlightData;
                        type ResponseStream = T::DoGetStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::Ticket>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).do_get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = DoGetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/arrow.flight.protocol.FlightService/DoPut" => {
                    #[allow(non_camel_case_types)]
                    struct DoPutSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::StreamingService<super::FlightData>
                        for DoPutSvc<T>
                    {
                        type Response = super::PutResult;
                        type ResponseStream = T::DoPutStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::FlightData>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).do_put(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = DoPutSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/arrow.flight.protocol.FlightService/DoExchange" => {
                    #[allow(non_camel_case_types)]
                    struct DoExchangeSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::StreamingService<super::FlightData>
                        for DoExchangeSvc<T>
                    {
                        type Response = super::FlightData;
                        type ResponseStream = T::DoExchangeStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::FlightData>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).do_exchange(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = DoExchangeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/arrow.flight.protocol.FlightService/DoAction" => {
                    #[allow(non_camel_case_types)]
                    struct DoActionSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::ServerStreamingService<super::Action>
                        for DoActionSvc<T>
                    {
                        type Response = super::Result;
                        type ResponseStream = T::DoActionStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::Action>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).do_action(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = DoActionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/arrow.flight.protocol.FlightService/ListActions" => {
                    #[allow(non_camel_case_types)]
                    struct ListActionsSvc<T: FlightService>(pub Arc<T>);
                    impl<T: FlightService>
                        tonic::server::ServerStreamingService<super::Empty>
                        for ListActionsSvc<T>
                    {
                        type Response = super::ActionType;
                        type ResponseStream = T::ListActionsStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::Empty>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).list_actions(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = ListActionsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: FlightService> Clone for FlightServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: FlightService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: FlightService> tonic::transport::NamedService for FlightServiceServer<T> {
        const NAME: &'static str = "arrow.flight.protocol.FlightService";
    }
}
