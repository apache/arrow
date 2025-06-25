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

    methods (Static)
        function obj = fromBytes(bytes)
            arguments
                bytes(:, 1) uint8
            end
            args = struct(Bytes=bytes, Type="Bytes");
            proxyName = "arrow.io.ipc.proxy.RecordBatchStreamReader";
            proxy = arrow.internal.proxy.create(proxyName, args);
            obj = arrow.io.ipc.RecordBatchStreamReader(proxy);
        end

        function obj = fromFile(filename)
            arguments
                filename(1, 1) string {mustBeNonzeroLengthText}
            end
            args = struct(Filename=filename, Type="File");
            proxyName = "arrow.io.ipc.proxy.RecordBatchStreamReader";
            proxy = arrow.internal.proxy.create(proxyName, args);
            obj = arrow.io.ipc.RecordBatchStreamReader(proxy);
        end
    end

    methods
        function obj = RecordBatchStreamReader(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy
            end
            obj.Proxy = proxy;
        end

        function schema = get.Schema(obj)
            proxyID = obj.Proxy.getSchema();
            proxyName = "arrow.tabular.proxy.Schema";
            proxy = libmexclass.proxy.Proxy(ID=proxyID, Name=proxyName);
            schema = arrow.tabular.Schema(proxy);
        end

        function tf = hasnext(obj)
            tf = obj.Proxy.hasNextRecordBatch();
        end

        function tf = done(obj)
            tf = ~obj.Proxy.hasNextRecordBatch();
        end

        function arrowRecordBatch = read(obj)
	    % NOTE: This function is a "convenience alias" for the readRecordBatch
	    % method, which has a longer name. This is the exact same implementation
	    % as readRecordBatch. Since this method might be called in a tight loop,
	    % it should be slightly more efficient to call the C++ code directly,
	    % rather than invoking obj.readRecordBatch indirectly. We are intentionally
	    % trading off code duplication for performance here.
            proxyID = obj.Proxy.readRecordBatch();
            proxyName = "arrow.tabular.proxy.RecordBatch";
            proxy = libmexclass.proxy.Proxy(ID=proxyID, Name=proxyName);
            arrowRecordBatch = arrow.tabular.RecordBatch(proxy);
        end

        function arrowRecordBatch = readRecordBatch(obj)
            proxyID = obj.Proxy.readRecordBatch();
            proxyName = "arrow.tabular.proxy.RecordBatch";
            proxy = libmexclass.proxy.Proxy(ID=proxyID, Name=proxyName);
            arrowRecordBatch = arrow.tabular.RecordBatch(proxy);
        end

        function arrowTable = readTable(obj)
            proxyID = obj.Proxy.readTable();
            proxyName = "arrow.tabular.proxy.Table";
            proxy = libmexclass.proxy.Proxy(ID=proxyID, Name=proxyName);
            arrowTable = arrow.tabular.Table(proxy);
        end

    end

end
