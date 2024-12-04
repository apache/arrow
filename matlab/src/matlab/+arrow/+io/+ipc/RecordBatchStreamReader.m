%RECORDBATCHSTREAMREADER Class for reading Arrow record batches from the
% Arrow IPC Stream format.

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

classdef RecordBatchStreamReader < matlab.mixin.Scalar

    properties(SetAccess=private, GetAccess=public, Hidden)
        Proxy
    end

    properties (Dependent, SetAccess=private, GetAccess=public)
        Schema
    end

    methods
        function obj = RecordBatchStreamReader(filename)
            arguments
                filename(1, 1) string {mustBeNonzeroLengthText}
            end
            args = struct(Filename=filename);
            proxyName = "arrow.io.ipc.proxy.RecordBatchStreamReader";
            obj.Proxy = arrow.internal.proxy.create(proxyName, args);
        end

        function schema = get.Schema(obj)
            proxyID = obj.Proxy.getSchema();
            proxyName = "arrow.tabular.proxy.Schema";
            proxy = libmexclass.proxy.Proxy(ID=proxyID, Name=proxyName);
            schema = arrow.tabular.Schema(proxy);
        end

        function tf = hasNextRecordBatch(obj)
            tf = obj.Proxy.hasNextRecordBatch();
        end

        function rb = readRecordBatch(obj)
            proxyID = obj.Proxy.readRecordBatch();
            proxyName = "arrow.tabular.proxy.RecordBatch";
            proxy = libmexclass.proxy.Proxy(ID=proxyID, Name=proxyName);
            rb = arrow.tabular.RecordBatch(proxy);
        end

    end

end
