%RECORDBATCHFILEWRITER Class for serializing record batches to a file using
% the IPC format.

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

classdef RecordBatchFileWriter < matlab.mixin.Scalar

    properties(SetAccess=private, GetAccess=public, Hidden)
        Proxy
    end

    methods
        function obj = RecordBatchFileWriter(filename, schema)
            arguments
                filename(1, 1) string {mustBeNonzeroLengthText} 
                schema(1, 1) arrow.tabular.Schema
            end
            args = struct(Filename=filename, SchemaProxyID=schema.Proxy.ID);
            proxyName = "arrow.io.ipc.proxy.RecordBatchFileWriter";
            obj.Proxy = arrow.internal.proxy.create(proxyName, args);
        end

        function writeRecordBatch(obj, recordBatch)
            arguments
                obj(1, 1) arrow.io.ipc.RecordBatchFileWriter
                recordBatch(1, 1) arrow.tabular.RecordBatch
            end

            args = struct(RecordBatchProxyID=recordBatch.Proxy.ID);
            obj.Proxy.writeRecordBatch(args);
        end

        function writeTable(obj, arrowTable)
            arguments
                obj(1, 1) arrow.io.ipc.RecordBatchFileWriter
                arrowTable(1, 1) arrow.tabular.Table
            end

            args = struct(TableProxyID=arrowTable.Proxy.ID);
            obj.Proxy.writeTable(args);
        end

        function write(obj, tabularObj)
            arguments
                obj(1, 1) arrow.io.ipc.RecordBatchFileWriter
                tabularObj(1, 1)
            end
            if isa(tabularObj, "arrow.tabular.RecordBatch")
                obj.writeRecordBatch(tabularObj);
            elseif isa(tabularObj, "arrow.tabular.Table")
                obj.writeTable(tabularObj);
            else
                id = "arrow:matlab:ipc:write:InvalidType";
                msg = "tabularObj input argument must be an instance of " + ...
                    "either arrow.tabular.RecordBatch or arrow.tabular.Table.";
                error(id, msg);
            end
        end

        function close(obj)
            obj.Proxy.close();
        end
    end
end
