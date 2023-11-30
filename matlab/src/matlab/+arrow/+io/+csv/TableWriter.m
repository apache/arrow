%TABLEWRITER Writes tabular data in an arrow.tabular.Table to a CSV file.

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
classdef TableWriter < matlab.mixin.Scalar

    properties(Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end

    properties(Dependent, SetAccess=private, GetAccess=public)
        Filename
    end

    methods
        function obj = TableWriter(filename)
            arguments
                filename (1, 1) string {mustBeNonmissing, mustBeNonzeroLengthText}
            end

            args = struct(Filename=filename);
            proxyName = "arrow.io.csv.proxy.TableWriter";
            obj.Proxy = arrow.internal.proxy.create(proxyName, args);
        end

        function write(obj, table)
            arguments
                obj (1, 1) arrow.io.csv.TableWriter
                table (1, 1) arrow.tabular.Table
            end
            args = struct(TableProxyID=table.Proxy.ID);
            obj.Proxy.write(args);
        end

        function filename = get.Filename(obj)
            filename = obj.Proxy.getFilename();
        end
    end
end
