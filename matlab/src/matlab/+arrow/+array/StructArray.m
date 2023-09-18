% arrow.array.StructArray

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

classdef StructArray < arrow.array.Array

    properties (Dependent, GetAccess=public, SetAccess=private)
        NumFields
    end

    methods
        function obj = StructArray(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.StructArray")}
            end

            obj@arrow.array.Array(proxy);
        end

        function numFields = get.NumFields(obj)
            numFields = obj.Proxy.getNumFields();
        end

        function f = field(obj, idx)
            import arrow.internal.validate.*

            idx = index.numericOrString(idx, "int32", AllowNonScalar=false);

            if isnumeric(idx)
                args = struct(Index=idx);
                [proxyID, typeID] = obj.Proxy.getColumnByIndex(args);
            else
                args = struct(Name=idx);
                [proxyID, typeID] = obj.Proxy.getColumnByName(args);
            end
            
            traits = arrow.type.traits.traits(arrow.type.ID(typeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.ArrayProxyClassName, ID=proxyID);
            f = traits.ArrayConstructor(proxy);
        end

        function T = toMATLAB(obj)
            T = table(obj);
        end

        function T = table(obj)
            import arrow.tabular.internal.*

            numFields = obj.NumFields;
            matlabArrays = cell(1, numFields);
            
            for ii = 1:numFields
                arrowArray = obj.field(ii);
                matlabArrays{ii} = toMATLAB(arrowArray);
            end

            validVariableNames = makeValidVariableNames(obj.FieldNames);
            validDimensionNames = makeValidDimensionNames(validVariableNames);

            T = table(matlabArrays{:}, ...
                VariableNames=validVariableNames, ...
                DimensionNames=validDimensionNames);
        end
    end

    methods (Static)
        function array = fromArrays(arrowArrays, opts)
            arguments(Repeating)
                arrowArrays(1, 1) arrow.array.Array
            end
            arguments
                opts.FieldNames(1, :) string {mustBeNonmissing} = compose("Field%d", 1:numel(arrowArrays))
            end

            import arrow.tabular.internal.validateArrayLengths
            import arrow.tabular.internal.validateColumnNames
            import arrow.array.internal.getArrayProxyIDs

            if numel(arrowArrays) == 0
                error("arrow:struct:ZeroFields", ...
                    "Must supply at least one field");
            end

            validateArrayLengths(arrowArrays);
            validateColumnNames(opts.FieldNames,  numel(arrowArrays));

            arrayProxyIDs = getArrayProxyIDs(arrowArrays);
            args = struct(ArrayProxyIDs=arrayProxyIDs, FieldNames=opts.FieldNames);
            proxyName = "arrow.array.proxy.StructArray";
            proxy = arrow.internal.proxy.create(proxyName, args);
            array = arrow.array.StructArray(proxy);
        end
    end
end