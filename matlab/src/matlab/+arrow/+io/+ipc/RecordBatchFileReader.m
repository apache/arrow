%RECORDBATCHFILEREADER Class for reading Arrow RecordBatches from the 
% Arrow binary (IPC) format.

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

classdef RecordBatchFileReader < matlab.mixin.Scalar

    properties(SetAccess=private, GetAccess=public, Hidden)
        Proxy
    end

    properties (Dependent, SetAccess=private, GetAccess=public)
        NumRecordBatches
        Schema
    end

    methods
        function obj = RecordBatchFileReader(filename)
            arguments
                filename(1, 1) string {mustBeNonzeroLengthText} 
            end
            args = struct(Filename=filename);
            proxyName = "arrow.io.ipc.proxy.RecordBatchFileReader";
            obj.Proxy = arrow.internal.proxy.create(proxyName, args);
        end

        function numRecordBatches = get.NumRecordBatches(obj)
            numRecordBatches = obj.Proxy.getNumRecordBatches();
        end

        function schema = get.Schema(obj)
            proxyID = obj.Proxy.getSchema();
            proxyName = "arrow.tabular.proxy.Schema";
            proxy = libmexclass.proxy.Proxy(ID=proxyID, Name=proxyName);
            schema = arrow.tabular.Schema(proxy);
        end

        function rb = read(obj, index)
            arguments
                obj(1, 1) arrow.io.ipc.RecordBatchFileReader
                index(1, 1)
            end
            index = arrow.internal.validate.index.numeric(index, "int32");
            args = struct(Index=index);
            proxyID = obj.Proxy.readRecordBatchAtIndex(args);
            proxyName = "arrow.tabular.proxy.RecordBatch";
            proxy = libmexclass.proxy.Proxy(ID=proxyID, Name=proxyName);
            rb = arrow.tabular.RecordBatch(proxy);
        end
    end
end