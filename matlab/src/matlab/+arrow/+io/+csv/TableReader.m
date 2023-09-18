%TABLEREADER Reads tabular data from a CSV file into an arrow.tabular.Table.

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

classdef TableReader

    properties (GetAccess=public, SetAccess=private, Hidden)
        Proxy
    end

    properties (Dependent, SetAccess=private, GetAccess=public)
        Filename
    end

    methods

        function obj = TableReader(filename)
            arguments
                filename (1, 1) string {mustBeNonmissing, mustBeNonzeroLengthText}
            end

            args = struct(Filename=filename);
            obj.Proxy = arrow.internal.proxy.create("arrow.io.csv.proxy.TableReader", args);
        end

        function table = read(obj)
            tableProxyID = obj.Proxy.read();
            proxy = libmexclass.proxy.Proxy(Name="arrow.tabular.proxy.Table", ID=tableProxyID);
            table = arrow.tabular.Table(proxy);
        end

        function filename = get.Filename(obj)
            filename = obj.Proxy.getFilename();
        end

    end

end