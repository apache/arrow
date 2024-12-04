%RECORDBATCHSTREAMWRITER Class for serializing record batches to the Arrow
% IPC Streaming format.

% Licensed to the Apache Software Foundation (ASF) under one or more
% contributor license agreements.  See the NOTICE file distributed with
% this work for additional information regarding copyright ownership.
% The ASF licenses this file to you under the Apache License, Version
% 2.0 (the "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
% implied.  See the License for the specific language governing
% permissions and limitations under the License.

classdef RecordBatchStreamWriter < arrow.io.ipc.RecordBatchWriter
    
    methods
        function obj = RecordBatchStreamWriter(filename, schema)
            arguments
                filename(1, 1) string {mustBeNonzeroLengthText} 
                schema(1, 1) arrow.tabular.Schema
            end
            args = struct(Filename=filename, SchemaProxyID=schema.Proxy.ID);
            proxyName = "arrow.io.ipc.proxy.RecordBatchStreamWriter";
            proxy = arrow.internal.proxy.create(proxyName, args);
            obj@arrow.io.ipc.RecordBatchWriter(proxy);
        end
    end
end

