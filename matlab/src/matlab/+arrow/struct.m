%STRUCT Constructs an arrow.type.StructType object

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

function type = struct(fields)
    arguments(Repeating)
        fields(1, :) arrow.type.Field {mustBeNonempty}
    end

    % Must have at least one Field in a Struct
    if isempty(fields)
        error("arrow:struct:TooFewInputs", ...
            "Must supply at least one arrow.type.Field");
    end

    fields = horzcat(fields{:});

    % Extract the corresponding Proxy IDs from each of the
    % supplied arrow.type.Field objects.
    numFields = numel(fields);
    fieldProxyIDs = zeros(1, numFields, "uint64");
    for ii = 1:numFields
        fieldProxyIDs(ii) = fields(ii).Proxy.ID;
    end

    % Construct an Arrow Field Proxy in C++ from the supplied Field Proxy IDs.
    args = struct(FieldProxyIDs=fieldProxyIDs);
    proxy = arrow.internal.proxy.create("arrow.type.proxy.StructType", args);
    type = arrow.type.StructType(proxy);
end