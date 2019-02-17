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

#include "arrow/flight/protocol-internal.h"

// NOTE(wesm): Including .cc files in another .cc file would ordinarily be a
// no-no. We have customized the serialization path for FlightData, which is
// currently only possible through some pre-processor commands that need to be
// included before either of these files is compiled. Because we don't want to
// edit the generated C++ files, we include them here and do our gRPC
// customizations in protocol-internal.h
#include "arrow/flight/Flight.grpc.pb.cc"  // NOLINT
#include "arrow/flight/Flight.pb.cc"       // NOLINT
