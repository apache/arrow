// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/flight/transport/grpc/grpc_client.h"

#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "arrow/util/config.h"
#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
#include <grpcpp/security/tls_credentials_options.h>
#endif
#else
#include <grpc++/grpc++.h>
#endif

#include <grpc/grpc_security_constants.h>

#include "arrow/buffer.h"
#include "arrow/device.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/base64.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"

#include "arrow/flight/client.h"
#include "arrow/flight/client_auth.h"
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/cookie_internal.h"
#include "arrow/flight/middleware.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/serialization_internal.h"
#include "arrow/flight/transport/grpc/util_internal.h"
#include "arrow/flight/types.h"

namespace arrow {

using internal::EndsWith;

namespace flight {
namespace transport {
namespace grpc {

namespace {
namespace pb = arrow::flight::protocol;

struct ClientRpc {
  ::grpc::ClientContext context;

  explicit ClientRpc(const FlightCallOptions& options) {
    if (options.timeout.count() >= 0) {
      std::chrono::system_clock::time_point deadline =
          std::chrono::time_point_cast<std::chrono::system_clock::time_point::duration>(
              std::chrono::system_clock::now() + options.timeout);
      context.set_deadline(deadline);
    }
    for (auto header : options.headers) {
      context.AddMetadata(header.first, header.second);
    }
  }

  /// \brief Add an auth token via an auth handler
  Status SetToken(ClientAuthHandler* auth_handler) {
    if (auth_handler) {
      std::string token;
      RETURN_NOT_OK(auth_handler->GetToken(&token));
      context.AddMetadata(kGrpcAuthHeader, token);
    }
    return Status::OK();
  }
};

class GrpcAddClientHeaders : public AddCallHeaders {
 public:
  explicit GrpcAddClientHeaders(std::multimap<::grpc::string, ::grpc::string>* metadata)
      : metadata_(metadata) {}
  ~GrpcAddClientHeaders() override = default;

  void AddHeader(const std::string& key, const std::string& value) override {
    metadata_->insert(std::make_pair(key, value));
  }

 private:
  std::multimap<::grpc::string, ::grpc::string>* metadata_;
};

class GrpcClientInterceptorAdapter : public ::grpc::experimental::Interceptor {
 public:
  explicit GrpcClientInterceptorAdapter(
      std::vector<std::unique_ptr<ClientMiddleware>> middleware)
      : middleware_(std::move(middleware)), received_headers_(false) {}

  void Intercept(::grpc::experimental::InterceptorBatchMethods* methods) {
    using InterceptionHookPoints = ::grpc::experimental::InterceptionHookPoints;
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      GrpcAddClientHeaders add_headers(methods->GetSendInitialMetadata());
      for (const auto& middleware : middleware_) {
        middleware->SendingHeaders(&add_headers);
      }
    }

    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
      if (!methods->GetRecvInitialMetadata()->empty()) {
        ReceivedHeaders(*methods->GetRecvInitialMetadata());
      }
    }

    if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::POST_RECV_STATUS)) {
      DCHECK_NE(nullptr, methods->GetRecvStatus());
      DCHECK_NE(nullptr, methods->GetRecvTrailingMetadata());
      ReceivedHeaders(*methods->GetRecvTrailingMetadata());
      const Status status = FromGrpcStatus(*methods->GetRecvStatus());
      for (const auto& middleware : middleware_) {
        middleware->CallCompleted(status);
      }
    }

    methods->Proceed();
  }

 private:
  void ReceivedHeaders(
      const std::multimap<::grpc::string_ref, ::grpc::string_ref>& metadata) {
    if (received_headers_) {
      return;
    }
    received_headers_ = true;
    CallHeaders headers;
    for (const auto& entry : metadata) {
      headers.insert({std::string_view(entry.first.data(), entry.first.length()),
                      std::string_view(entry.second.data(), entry.second.length())});
    }
    for (const auto& middleware : middleware_) {
      middleware->ReceivedHeaders(headers);
    }
  }

  std::vector<std::unique_ptr<ClientMiddleware>> middleware_;
  // When communicating with a gRPC-Java server, the server may not
  // send back headers if the call fails right away. Instead, the
  // headers will be consolidated into the trailers. We don't want to
  // call the client middleware callback twice, so instead track
  // whether we saw headers - if not, then we need to check trailers.
  bool received_headers_;
};

class GrpcClientInterceptorAdapterFactory
    : public ::grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  GrpcClientInterceptorAdapterFactory(
      std::vector<std::shared_ptr<ClientMiddlewareFactory>> middleware)
      : middleware_(middleware) {}

  ::grpc::experimental::Interceptor* CreateClientInterceptor(
      ::grpc::experimental::ClientRpcInfo* info) override {
    std::vector<std::unique_ptr<ClientMiddleware>> middleware;

    FlightMethod flight_method = FlightMethod::Invalid;
    std::string_view method(info->method());
    if (EndsWith(method, "/Handshake")) {
      flight_method = FlightMethod::Handshake;
    } else if (EndsWith(method, "/ListFlights")) {
      flight_method = FlightMethod::ListFlights;
    } else if (EndsWith(method, "/GetFlightInfo")) {
      flight_method = FlightMethod::GetFlightInfo;
    } else if (EndsWith(method, "/GetSchema")) {
      flight_method = FlightMethod::GetSchema;
    } else if (EndsWith(method, "/DoGet")) {
      flight_method = FlightMethod::DoGet;
    } else if (EndsWith(method, "/DoPut")) {
      flight_method = FlightMethod::DoPut;
    } else if (EndsWith(method, "/DoExchange")) {
      flight_method = FlightMethod::DoExchange;
    } else if (EndsWith(method, "/DoAction")) {
      flight_method = FlightMethod::DoAction;
    } else if (EndsWith(method, "/ListActions")) {
      flight_method = FlightMethod::ListActions;
    } else {
      ARROW_LOG(WARNING) << "Unknown Flight method: " << info->method();
      flight_method = FlightMethod::Invalid;
    }

    const CallInfo flight_info{flight_method};
    for (auto& factory : middleware_) {
      std::unique_ptr<ClientMiddleware> instance;
      factory->StartCall(flight_info, &instance);
      if (instance) {
        middleware.push_back(std::move(instance));
      }
    }
    return new GrpcClientInterceptorAdapter(std::move(middleware));
  }

 private:
  std::vector<std::shared_ptr<ClientMiddlewareFactory>> middleware_;
};

class GrpcClientAuthSender : public ClientAuthSender {
 public:
  explicit GrpcClientAuthSender(
      std::shared_ptr<
          ::grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
          stream)
      : stream_(stream) {}

  Status Write(const std::string& token) override {
    pb::HandshakeRequest response;
    response.set_payload(token);
    if (stream_->Write(response)) {
      return Status::OK();
    }
    return FromGrpcStatus(stream_->Finish());
  }

 private:
  std::shared_ptr<::grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
      stream_;
};

class GrpcClientAuthReader : public ClientAuthReader {
 public:
  explicit GrpcClientAuthReader(
      std::shared_ptr<
          ::grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
          stream)
      : stream_(stream) {}

  Status Read(std::string* token) override {
    pb::HandshakeResponse request;
    if (stream_->Read(&request)) {
      *token = std::move(*request.mutable_payload());
      return Status::OK();
    }
    return FromGrpcStatus(stream_->Finish());
  }

 private:
  std::shared_ptr<::grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
      stream_;
};

/// \brief The base of the ClientDataStream implementation for gRPC.
template <typename Stream, typename ReadPayloadType>
class FinishableDataStream : public internal::ClientDataStream {
 public:
  FinishableDataStream(std::shared_ptr<ClientRpc> rpc, std::shared_ptr<Stream> stream)
      : rpc_(std::move(rpc)), stream_(std::move(stream)), finished_(false) {}

  void TryCancel() override { rpc_->context.TryCancel(); }

 protected:
  Status DoFinish() override {
    if (finished_) {
      return server_status_;
    }

    // Drain the read side, as otherwise gRPC Finish() will hang. We
    // only call Finish() when the client closes the writer or the
    // reader finishes, so it's OK to assume the client no longer
    // wants to read and drain the read side. (If the client wants to
    // indicate that it is done writing, but not done reading, it
    // should use DoneWriting.
    ReadPayloadType message;
    while (ReadPayload(stream_.get(), &message)) {
      // Drain the read side to avoid gRPC hanging in Finish()
    }

    server_status_ = FromGrpcStatus(stream_->Finish(), &rpc_->context);
    if (!server_status_.ok()) {
      server_status_ = Status::FromDetailAndArgs(
          server_status_.code(), server_status_.detail(), server_status_.message(),
          ". gRPC client debug context: ", rpc_->context.debug_error_string());
    }
    if (!transport_status_.ok()) {
      if (server_status_.ok()) {
        server_status_ = transport_status_;
      } else {
        server_status_ = Status::FromDetailAndArgs(
            server_status_.code(), server_status_.detail(), server_status_.message(),
            ". gRPC client debug context: ", rpc_->context.debug_error_string(),
            ". Additional context: ", transport_status_.ToString());
      }
    }
    finished_ = true;

    return server_status_;
  }

  std::shared_ptr<ClientRpc> rpc_;
  std::shared_ptr<Stream> stream_;
  bool finished_;
  Status server_status_;
  // A transport-side error that needs to get combined with the server status
  Status transport_status_;
};

/// \brief A ClientDataStream implementation for gRPC that manages a
///   mutex to protect from concurrent reads/writes, and drains the
///   read side on finish.
template <typename Stream, typename ReadPayload>
class WritableDataStream : public FinishableDataStream<Stream, ReadPayload> {
 public:
  using Base = FinishableDataStream<Stream, ReadPayload>;
  WritableDataStream(std::shared_ptr<ClientRpc> rpc, std::shared_ptr<Stream> stream)
      : Base(std::move(rpc), std::move(stream)), read_mutex_(), done_writing_(false) {}

  Status WritesDone() override {
    // This is only used by the writer side of a stream, so it need
    // not be protected with a lock.
    if (done_writing_) {
      return Status::OK();
    }
    done_writing_ = true;
    if (!stream_->WritesDone()) {
      // Error happened, try to close the stream to get more detailed info
      return this->Finish(MakeFlightError(FlightStatusCode::Internal,
                                          "Could not flush pending record batches"));
    }
    return Status::OK();
  }

 protected:
  Status DoFinish() override {
    // This may be used concurrently by reader/writer side of a
    // stream, so it needs to be protected.
    std::lock_guard<std::mutex> guard(read_mutex_);

    // Try to flush pending writes. Don't use our WritesDone() to
    // avoid recursion.
    bool finished_writes = done_writing_ || stream_->WritesDone();
    done_writing_ = true;

    Status st = Base::DoFinish();
    if (!finished_writes) {
      return Status::FromDetailAndArgs(
          st.code(), st.detail(), st.message(),
          ". Additionally, could not finish writing record batches before closing");
    }
    return st;
  }

  using Base::stream_;
  std::mutex read_mutex_;
  bool done_writing_;
};

class GrpcClientGetStream
    : public FinishableDataStream<::grpc::ClientReader<pb::FlightData>,
                                  internal::FlightData> {
 public:
  using FinishableDataStream::FinishableDataStream;

  bool ReadData(internal::FlightData* data) override {
    return ReadPayload(stream_.get(), data);
  }
  Status WritesDone() override { return Status::NotImplemented("NYI"); }
};

class GrpcClientPutStream
    : public WritableDataStream<::grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>,
                                pb::PutResult> {
 public:
  using Stream = ::grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>;
  using WritableDataStream::WritableDataStream;

  bool ReadPutMetadata(std::shared_ptr<Buffer>* out) override {
    std::lock_guard<std::mutex> guard(read_mutex_);
    if (finished_) return false;
    pb::PutResult message;
    if (stream_->Read(&message)) {
      *out = Buffer::FromString(std::move(*message.mutable_app_metadata()));
    } else {
      // Stream finished
      *out = nullptr;
    }
    return true;
  }
  arrow::Result<bool> WriteData(const FlightPayload& payload) override {
    return WritePayload(payload, this->stream_.get());
  }
};

class GrpcClientExchangeStream
    : public WritableDataStream<
          ::grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>,
          internal::FlightData> {
 public:
  using Stream = ::grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>;
  GrpcClientExchangeStream(std::shared_ptr<ClientRpc> rpc, std::shared_ptr<Stream> stream)
      : WritableDataStream(std::move(rpc), std::move(stream)) {}

  bool ReadData(internal::FlightData* data) override {
    std::lock_guard<std::mutex> guard(read_mutex_);
    if (finished_) return false;
    return ReadPayload(stream_.get(), data);
  }
  arrow::Result<bool> WriteData(const FlightPayload& payload) override {
    return WritePayload(payload, this->stream_.get());
  }
};

static constexpr char kBearerPrefix[] = "Bearer ";
static constexpr char kBasicPrefix[] = "Basic ";

/// \brief Add base64 encoded credentials to the outbound headers.
///
/// \param context Context object to add the headers to.
/// \param username Username to format and encode.
/// \param password Password to format and encode.
void AddBasicAuthHeaders(::grpc::ClientContext* context, const std::string& username,
                         const std::string& password) {
  const std::string credentials = username + ":" + password;
  context->AddMetadata(internal::kAuthHeader,
                       kBasicPrefix + arrow::util::base64_encode(credentials));
}

/// \brief Get bearer token from inbound headers.
///
/// \param context Incoming ClientContext that contains headers.
/// \return Arrow result with bearer token (empty if no bearer token found).
arrow::Result<std::pair<std::string, std::string>> GetBearerTokenHeader(
    ::grpc::ClientContext& context) {
  // Lambda function to compare characters without case sensitivity.
  auto char_compare = [](const char& char1, const char& char2) {
    return (::toupper(char1) == ::toupper(char2));
  };

  // Get the auth token if it exists, this can be in the initial or the trailing metadata.
  auto trailing_headers = context.GetServerTrailingMetadata();
  auto initial_headers = context.GetServerInitialMetadata();
  auto bearer_iter = trailing_headers.find(internal::kAuthHeader);
  if (bearer_iter == trailing_headers.end()) {
    bearer_iter = initial_headers.find(internal::kAuthHeader);
    if (bearer_iter == initial_headers.end()) {
      return std::make_pair("", "");
    }
  }

  // Check if the value of the auth token starts with the bearer prefix and latch it.
  std::string bearer_val(bearer_iter->second.data(), bearer_iter->second.size());
  if (bearer_val.size() > strlen(kBearerPrefix)) {
    if (std::equal(bearer_val.begin(), bearer_val.begin() + strlen(kBearerPrefix),
                   kBearerPrefix, char_compare)) {
      return std::make_pair(internal::kAuthHeader, bearer_val);
    }
  }

  // The server is not required to provide a bearer token.
  return std::make_pair("", "");
}

// Dummy self-signed certificate to be used because TlsCredentials
// requires root CA certs, even if you are skipping server
// verification.
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
constexpr char kDummyRootCert[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIICwzCCAaugAwIBAgIJAM12DOkcaqrhMA0GCSqGSIb3DQEBBQUAMBQxEjAQBgNV\n"
    "BAMTCWxvY2FsaG9zdDAeFw0yMDEwMDcwODIyNDFaFw0zMDEwMDUwODIyNDFaMBQx\n"
    "EjAQBgNVBAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
    "ggEBALjJ8KPEpF0P4GjMPrJhjIBHUL0AX9E4oWdgJRCSFkPKKEWzQabTQBikMOhI\n"
    "W4VvBMaHEBuECE5OEyrzDRiAO354I4F4JbBfxMOY8NIW0uWD6THWm2KkCzoZRIPW\n"
    "yZL6dN+mK6cEH+YvbNuy5ZQGNjGG43tyiXdOCAc4AI9POeTtjdMpbbpR2VY4Ad/E\n"
    "oTEiS3gNnN7WIAdgMhCJxjzvPwKszV3f7pwuTHzFMsuHLKr6JeaVUYfbi4DxxC8Z\n"
    "k6PF6dLlLf3ngTSLBJyaXP1BhKMvz0TaMK3F0y2OGwHM9J8np2zWjTlNVEzffQZx\n"
    "SWMOQManlJGs60xYx9KCPJMZZsMCAwEAAaMYMBYwFAYDVR0RBA0wC4IJbG9jYWxo\n"
    "b3N0MA0GCSqGSIb3DQEBBQUAA4IBAQC0LrmbcNKgO+D50d/wOc+vhi9K04EZh8bg\n"
    "WYAK1kLOT4eShbzqWGV/1EggY4muQ6ypSELCLuSsg88kVtFQIeRilA6bHFqQSj6t\n"
    "sqgh2cWsMwyllCtmX6Maf3CLb2ZdoJlqUwdiBdrbIbuyeAZj3QweCtLKGSQzGDyI\n"
    "KH7G8nC5d0IoRPiCMB6RnMMKsrhviuCdWbAFHop7Ff36JaOJ8iRa2sSf2OXE8j/5\n"
    "obCXCUvYHf4Zw27JcM2AnnQI9VJLnYxis83TysC5s2Z7t0OYNS9kFmtXQbUNlmpS\n"
    "doQ/Eu47vWX7S0TXeGziGtbAOKxbHE0BGGPDOAB/jGW/JVbeTiXY\n"
    "-----END CERTIFICATE-----\n";
#endif

class GrpcResultStream : public ResultStream {
 public:
  explicit GrpcResultStream(const FlightCallOptions& options)
      : rpc_(options),
        stop_token_(options.stop_token),
        status_(
            Status::UnknownError("Internal implementation error, stream not started")) {}

  ~GrpcResultStream() override {
    if (stream_) {
      rpc_.context.TryCancel();
      auto status = FromGrpcStatus(stream_->Finish(), &rpc_.context);
      if (!status.ok() && !status.IsCancelled()) {
        ARROW_LOG(DEBUG)
            << "DoAction result was not fully consumed, server returned error: "
            << status.ToString();
      }
    }
  }

  static arrow::Result<std::unique_ptr<GrpcResultStream>> Make(
      const FlightCallOptions& options, pb::FlightService::Stub* stub,
      ClientAuthHandler* auth_handler, const Action& action) {
    auto result = std::make_unique<GrpcResultStream>(options);
    ARROW_RETURN_NOT_OK(result->Init(stub, auth_handler, action));
    return result;
  }

  Status Init(pb::FlightService::Stub* stub, ClientAuthHandler* auth_handler,
              const Action& action) {
    pb::Action pb_action;
    RETURN_NOT_OK(internal::ToProto(action, &pb_action));
    RETURN_NOT_OK(rpc_.SetToken(auth_handler));
    stream_ = stub->DoAction(&rpc_.context, pb_action);
    // GH-15150: wait for initial metadata to allow some side effects to occur
    stream_->WaitForInitialMetadata();
    return Status::OK();
  }

  arrow::Result<std::unique_ptr<Result>> Next() override {
    if (stream_) {
      pb::Result pb_result;
      if (!stop_token_.IsStopRequested() && stream_->Read(&pb_result)) {
        auto result = std::make_unique<Result>();
        RETURN_NOT_OK(internal::FromProto(pb_result, result.get()));
        return result;
      } else if (stop_token_.IsStopRequested()) {
        rpc_.context.TryCancel();
      }
      RETURN_NOT_OK(stop_token_.Poll());

      status_ = FromGrpcStatus(stream_->Finish(), &rpc_.context);
      stream_.reset();
    }
    RETURN_NOT_OK(status_);
    return nullptr;
  }

 private:
  ClientRpc rpc_;
  StopToken stop_token_;
  Status status_;
  std::unique_ptr<::grpc::ClientReader<pb::Result>> stream_;
};

class GrpcClientImpl : public internal::ClientTransport {
 public:
  static arrow::Result<std::unique_ptr<internal::ClientTransport>> Make() {
    return std::make_unique<GrpcClientImpl>();
  }

  Status Init(const FlightClientOptions& options, const Location& location,
              const arrow::internal::Uri& uri) override {
    const std::string& scheme = location.scheme();

    std::stringstream grpc_uri;
    std::shared_ptr<::grpc::ChannelCredentials> creds;
    if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp || scheme == kSchemeGrpcTls) {
      grpc_uri << arrow::internal::UriEncodeHost(uri.host()) << ':' << uri.port_text();

      if (scheme == kSchemeGrpcTls) {
        if (options.disable_server_verification) {
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
          namespace ge = ::GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS;

#if defined(GRPC_USE_CERTIFICATE_VERIFIER)
          // gRPC >= 1.43
          class NoOpCertificateVerifier : public ge::ExternalCertificateVerifier {
           public:
            bool Verify(ge::TlsCustomVerificationCheckRequest*,
                        std::function<void(::grpc::Status)>,
                        ::grpc::Status* sync_status) override {
              *sync_status = ::grpc::Status::OK;
              return true;  // Check done synchronously
            }
            void Cancel(ge::TlsCustomVerificationCheckRequest*) override {}
          };
          auto cert_verifier =
              ge::ExternalCertificateVerifier::Create<NoOpCertificateVerifier>();

#else   // defined(GRPC_USE_CERTIFICATE_VERIFIER)
        // gRPC < 1.43
        // A callback to supply to TlsCredentialsOptions that accepts any server
        // arguments.
          struct NoOpTlsAuthorizationCheck
              : public ge::TlsServerAuthorizationCheckInterface {
            int Schedule(ge::TlsServerAuthorizationCheckArg* arg) override {
              arg->set_success(1);
              arg->set_status(GRPC_STATUS_OK);
              return 0;
            }
          };
          auto server_authorization_check = std::make_shared<NoOpTlsAuthorizationCheck>();
          noop_auth_check_ = std::make_shared<ge::TlsServerAuthorizationCheckConfig>(
              server_authorization_check);
#endif  // defined(GRPC_USE_CERTIFICATE_VERIFIER)

#if defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS)
          auto certificate_provider =
              std::make_shared<::grpc::experimental::StaticDataCertificateProvider>(
                  kDummyRootCert);
#if defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS_ROOT_CERTS)
          ::grpc::experimental::TlsChannelCredentialsOptions tls_options(
              certificate_provider);
#else   // defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS_ROOT_CERTS)
        // While gRPC >= 1.36 does not require a root cert (it has a default)
        // in practice the path it hardcodes is broken. See grpc/grpc#21655.
          ::grpc::experimental::TlsChannelCredentialsOptions tls_options;
          tls_options.set_certificate_provider(certificate_provider);
#endif  // defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS_ROOT_CERTS)
          tls_options.watch_root_certs();
          tls_options.set_root_cert_name("dummy");
#if defined(GRPC_USE_CERTIFICATE_VERIFIER)
          tls_options.set_certificate_verifier(std::move(cert_verifier));
          tls_options.set_check_call_host(false);
          tls_options.set_verify_server_certs(false);
#else   // defined(GRPC_USE_CERTIFICATE_VERIFIER)
          tls_options.set_server_verification_option(
              grpc_tls_server_verification_option::GRPC_TLS_SKIP_ALL_SERVER_VERIFICATION);
          tls_options.set_server_authorization_check_config(noop_auth_check_);
#endif  // defined(GRPC_USE_CERTIFICATE_VERIFIER)
#elif defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
          // continues defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS)
          auto materials_config = std::make_shared<ge::TlsKeyMaterialsConfig>();
          materials_config->set_pem_root_certs(kDummyRootCert);
          ge::TlsCredentialsOptions tls_options(
              GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE,
              GRPC_TLS_SKIP_ALL_SERVER_VERIFICATION, materials_config,
              std::shared_ptr<ge::TlsCredentialReloadConfig>(), noop_auth_check_);
#endif  // defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS)
          creds = ge::TlsCredentials(tls_options);
#else   // defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
          return Status::NotImplemented(
              "Using encryption with server verification disabled is unsupported. "
              "Please use a release of Arrow Flight built with gRPC 1.27 or higher.");
#endif  // defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
        } else {
          ::grpc::SslCredentialsOptions ssl_options;
          if (!options.tls_root_certs.empty()) {
            ssl_options.pem_root_certs = options.tls_root_certs;
          }
          if (!options.cert_chain.empty()) {
            ssl_options.pem_cert_chain = options.cert_chain;
          }
          if (!options.private_key.empty()) {
            ssl_options.pem_private_key = options.private_key;
          }
          creds = ::grpc::SslCredentials(ssl_options);
        }
      } else {
        creds = ::grpc::InsecureChannelCredentials();
      }
    } else if (scheme == kSchemeGrpcUnix) {
      grpc_uri << "unix://" << uri.path();
      creds = ::grpc::InsecureChannelCredentials();
    } else {
      return Status::NotImplemented("Flight scheme ", scheme,
                                    " is not supported by the gRPC transport");
    }

    ::grpc::ChannelArguments args;
    // We can't set the same config value twice, so for values where
    // we want to set defaults, keep them in a map and update them;
    // then update them all at once
    std::unordered_map<std::string, int> default_args;
    // Try to reconnect quickly at first, in case the server is still starting up
    default_args[GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS] = 100;
    // Receive messages of any size
    default_args[GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH] = -1;
    // Setting this arg enables each client to open it's own TCP connection to server,
    // not sharing one single connection, which becomes bottleneck under high load.
    default_args[GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL] = 1;

    if (options.override_hostname != "") {
      args.SetSslTargetNameOverride(options.override_hostname);
    }

    // Allow setting generic gRPC options.
    for (const auto& arg : options.generic_options) {
      if (std::holds_alternative<int>(arg.second)) {
        default_args[arg.first] = std::get<int>(arg.second);
      } else if (std::holds_alternative<std::string>(arg.second)) {
        args.SetString(arg.first, std::get<std::string>(arg.second));
      }
      // Otherwise unimplemented
    }
    for (const auto& pair : default_args) {
      args.SetInt(pair.first, pair.second);
    }

    std::vector<std::unique_ptr<::grpc::experimental::ClientInterceptorFactoryInterface>>
        interceptors;
    interceptors.emplace_back(
        new GrpcClientInterceptorAdapterFactory(std::move(options.middleware)));

    stub_ = pb::FlightService::NewStub(
        ::grpc::experimental::CreateCustomChannelWithInterceptors(
            grpc_uri.str(), creds, args, std::move(interceptors)));
    return Status::OK();
  }

  Status Close() override {
    // TODO(ARROW-15473): if we track ongoing RPCs, we can cancel them first
    // gRPC does not offer a real Close(). We could reset() the gRPC
    // client but that can cause gRPC to hang in shutdown
    // (ARROW-15793).
    return Status::OK();
  }

  Status Authenticate(const FlightCallOptions& options,
                      std::unique_ptr<ClientAuthHandler> auth_handler) override {
    auth_handler_ = std::move(auth_handler);
    ClientRpc rpc(options);
    std::shared_ptr<
        ::grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
        stream = stub_->Handshake(&rpc.context);
    GrpcClientAuthSender outgoing{stream};
    GrpcClientAuthReader incoming{stream};
    RETURN_NOT_OK(auth_handler_->Authenticate(&outgoing, &incoming));
    // Explicitly close our side of the connection
    bool finished_writes = stream->WritesDone();
    RETURN_NOT_OK(FromGrpcStatus(stream->Finish(), &rpc.context));
    if (!finished_writes) {
      return MakeFlightError(FlightStatusCode::Internal,
                             "Could not finish writing before closing");
    }
    return Status::OK();
  }

  arrow::Result<std::pair<std::string, std::string>> AuthenticateBasicToken(
      const FlightCallOptions& options, const std::string& username,
      const std::string& password) override {
    // Add basic auth headers to outgoing headers.
    ClientRpc rpc(options);
    AddBasicAuthHeaders(&rpc.context, username, password);
    std::shared_ptr<
        ::grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
        stream = stub_->Handshake(&rpc.context);
    // Explicitly close our side of the connection.
    bool finished_writes = stream->WritesDone();
    RETURN_NOT_OK(FromGrpcStatus(stream->Finish(), &rpc.context));
    if (!finished_writes) {
      return MakeFlightError(FlightStatusCode::Internal,
                             "Could not finish writing before closing");
    }
    // Grab bearer token from incoming headers.
    return GetBearerTokenHeader(rpc.context);
  }

  Status ListFlights(const FlightCallOptions& options, const Criteria& criteria,
                     std::unique_ptr<FlightListing>* listing) override {
    pb::Criteria pb_criteria;
    RETURN_NOT_OK(internal::ToProto(criteria, &pb_criteria));

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    std::unique_ptr<::grpc::ClientReader<pb::FlightInfo>> stream(
        stub_->ListFlights(&rpc.context, pb_criteria));

    std::vector<FlightInfo> flights;

    pb::FlightInfo pb_info;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_info)) {
      FlightInfo::Data info_data;
      RETURN_NOT_OK(internal::FromProto(pb_info, &info_data));
      flights.emplace_back(std::move(info_data));
    }
    if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();
    RETURN_NOT_OK(options.stop_token.Poll());
    listing->reset(new SimpleFlightListing(std::move(flights)));
    return FromGrpcStatus(stream->Finish(), &rpc.context);
  }

  Status DoAction(const FlightCallOptions& options, const Action& action,
                  std::unique_ptr<ResultStream>* results) override {
    ARROW_ASSIGN_OR_RAISE(*results, GrpcResultStream::Make(options, stub_.get(),
                                                           auth_handler_.get(), action));
    return Status::OK();
  }

  Status ListActions(const FlightCallOptions& options,
                     std::vector<ActionType>* types) override {
    pb::Empty empty;

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    std::unique_ptr<::grpc::ClientReader<pb::ActionType>> stream(
        stub_->ListActions(&rpc.context, empty));

    pb::ActionType pb_type;
    ActionType type;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_type)) {
      RETURN_NOT_OK(internal::FromProto(pb_type, &type));
      types->emplace_back(std::move(type));
    }
    if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();
    RETURN_NOT_OK(options.stop_token.Poll());
    return FromGrpcStatus(stream->Finish(), &rpc.context);
  }

  Status GetFlightInfo(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info) override {
    pb::FlightDescriptor pb_descriptor;
    pb::FlightInfo pb_response;

    RETURN_NOT_OK(internal::ToProto(descriptor, &pb_descriptor));

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    Status s = FromGrpcStatus(
        stub_->GetFlightInfo(&rpc.context, pb_descriptor, &pb_response), &rpc.context);
    RETURN_NOT_OK(s);

    FlightInfo::Data info_data;
    RETURN_NOT_OK(internal::FromProto(pb_response, &info_data));
    info->reset(new FlightInfo(std::move(info_data)));
    return Status::OK();
  }

  arrow::Result<std::unique_ptr<SchemaResult>> GetSchema(
      const FlightCallOptions& options, const FlightDescriptor& descriptor) override {
    pb::FlightDescriptor pb_descriptor;
    pb::SchemaResult pb_response;

    RETURN_NOT_OK(internal::ToProto(descriptor, &pb_descriptor));

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    Status s = FromGrpcStatus(stub_->GetSchema(&rpc.context, pb_descriptor, &pb_response),
                              &rpc.context);
    RETURN_NOT_OK(s);

    std::string str;
    RETURN_NOT_OK(internal::FromProto(pb_response, &str));
    return std::make_unique<SchemaResult>(std::move(str));
  }

  Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<internal::ClientDataStream>* out) override {
    pb::Ticket pb_ticket;
    RETURN_NOT_OK(internal::ToProto(ticket, &pb_ticket));

    auto rpc = std::make_shared<ClientRpc>(options);
    RETURN_NOT_OK(rpc->SetToken(auth_handler_.get()));
    std::shared_ptr<::grpc::ClientReader<pb::FlightData>> stream =
        stub_->DoGet(&rpc->context, pb_ticket);
    *out = std::make_unique<GrpcClientGetStream>(std::move(rpc), std::move(stream));
    return Status::OK();
  }

  Status DoPut(const FlightCallOptions& options,
               std::unique_ptr<internal::ClientDataStream>* out) override {
    using GrpcStream = ::grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>;

    auto rpc = std::make_shared<ClientRpc>(options);
    RETURN_NOT_OK(rpc->SetToken(auth_handler_.get()));
    std::shared_ptr<GrpcStream> stream = stub_->DoPut(&rpc->context);
    *out = std::make_unique<GrpcClientPutStream>(std::move(rpc), std::move(stream));
    return Status::OK();
  }

  Status DoExchange(const FlightCallOptions& options,
                    std::unique_ptr<internal::ClientDataStream>* out) override {
    using GrpcStream = ::grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>;

    auto rpc = std::make_shared<ClientRpc>(options);
    RETURN_NOT_OK(rpc->SetToken(auth_handler_.get()));
    std::shared_ptr<GrpcStream> stream = stub_->DoExchange(&rpc->context);
    *out = std::make_unique<GrpcClientExchangeStream>(std::move(rpc), std::move(stream));
    return Status::OK();
  }

 private:
  std::unique_ptr<pb::FlightService::Stub> stub_;
  std::shared_ptr<ClientAuthHandler> auth_handler_;
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS) && \
    !defined(GRPC_USE_CERTIFICATE_VERIFIER)
  // Scope the TlsServerAuthorizationCheckConfig to be at the class instance level, since
  // it gets created during Connect() and needs to persist to DoAction() calls. gRPC does
  // not correctly increase the reference count of this object:
  // https://github.com/grpc/grpc/issues/22287
  std::shared_ptr<
      ::GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS::TlsServerAuthorizationCheckConfig>
      noop_auth_check_;
#endif
};
std::once_flag kGrpcClientTransportInitialized;
}  // namespace

void InitializeFlightGrpcClient() {
  std::call_once(kGrpcClientTransportInitialized, []() {
    auto* registry = flight::internal::GetDefaultTransportRegistry();
    for (const auto& transport : {"grpc", "grpc+tls", "grpc+tcp", "grpc+unix"}) {
      ARROW_CHECK_OK(registry->RegisterClient(transport, GrpcClientImpl::Make));
    }
  });
}

}  // namespace grpc
}  // namespace transport
}  // namespace flight
}  // namespace arrow
