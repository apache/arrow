%RECORDBATCHWRITER Class for serializing record batches to the Arrow
% IPC format.

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

classdef (Abstract) RecordBatchWriter < matlab.mixin.Scalar

    properties(SetAccess=private, GetAccess=public, Hidden)
        Proxy
    end

    methods
        function obj = RecordBatchWriter(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy
            end
            obj.Proxy = proxy;
        end

        function writeRecordBatch(obj, recordBatch)
            arguments
                obj(1, 1) arrow.io.ipc.RecordBatchWriter
                recordBatch(1, 1) arrow.tabular.RecordBatch
            end

            args = struct(RecordBatchProxyID=recordBatch.Proxy.ID);
            obj.Proxy.writeRecordBatch(args);
        end

        function writeTable(obj, arrowTable)
            arguments
                obj(1, 1) arrow.io.ipc.RecordBatchWriter
                arrowTable(1, 1) arrow.tabular.Table
            end

            args = struct(TableProxyID=arrowTable.Proxy.ID);
            obj.Proxy.writeTable(args);
        end

        function write(obj, tabularObj)
            arguments
                obj(1, 1) arrow.io.ipc.RecordBatchWriter
                tabularObj(1, 1)
            end
            if isa(tabularObj, "arrow.tabular.RecordBatch")
                obj.writeRecordBatch(tabularObj);
            elseif isa(tabularObj, "arrow.tabular.Table")
                obj.writeTable(tabularObj);
            else
                id = "arrow:io:ipc:write:InvalidType";
                msg = "Input must be an instance of " + ...
                    "either arrow.tabular.RecordBatch or arrow.tabular.Table.";
                error(id, msg);
            end
        end

        function close(obj)
            obj.Proxy.close();
        end
    end
end