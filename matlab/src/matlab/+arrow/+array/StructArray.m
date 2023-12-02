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
        FieldNames
    end

    properties (Hidden, Dependent, GetAccess=public, SetAccess=private)
        NullSubstitutionValue
    end

    methods
        function obj = StructArray(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.StructArray")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.Array(proxy);
        end

        function numFields = get.NumFields(obj)
            numFields = obj.Proxy.getNumFields();
        end

        function fieldNames = get.FieldNames(obj)
            fieldNames = obj.Proxy.getFieldNames();
        end

        function F = field(obj, idx)
            import arrow.internal.validate.*

            idx = index.numericOrString(idx, "int32", AllowNonScalar=false);

            if isnumeric(idx)
                args = struct(Index=idx);
                fieldStruct = obj.Proxy.getFieldByIndex(args);
            else
                args = struct(Name=idx);
                fieldStruct = obj.Proxy.getFieldByName(args);
            end
            
            traits = arrow.type.traits.traits(arrow.type.ID(fieldStruct.TypeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.ArrayProxyClassName, ID=fieldStruct.ProxyID);
            F = traits.ArrayConstructor(proxy);
        end

        function T = toMATLAB(obj)
            T = table(obj);
        end

        function T = table(obj)
            import arrow.tabular.internal.*

            numFields = obj.NumFields;
            matlabArrays = cell(1, numFields);

            invalid = ~obj.Valid;
            numInvalid = nnz(invalid);
            
            for ii = 1:numFields
                arrowArray = obj.field(ii);
                matlabArray = toMATLAB(arrowArray);
                if numInvalid ~= 0
                    % MATLAB tables do not support null values themselves. 
                    % So, to encode the StructArray's null values, we 
                    % iterate over each  variable in the resulting MATLAB
                    % table, and for each variable, we set the value of all
                    % null elements to the "NullSubstitutionValue" that
                    % corresponds to the variable's type (e.g. NaN for
                    % double, NaT for datetime, etc.).
                    matlabArray(invalid, :) = repmat(arrowArray.NullSubstitutionValue, [numInvalid 1]);
                end
                matlabArrays{ii} = matlabArray;
            end

            fieldNames = [obj.Type.Fields.Name];
            validVariableNames = makeValidVariableNames(fieldNames);
            validDimensionNames = makeValidDimensionNames(validVariableNames);

            T = table(matlabArrays{:}, ...
                VariableNames=validVariableNames, ...
                DimensionNames=validDimensionNames);
        end

        function nullSubVal = get.NullSubstitutionValue(obj)
            % Return a cell array containing each field's type-specific
            % "null" value. For example, NaN is the type-specific null
            % value for Float32Arrays and Float64Arrays
            numFields = obj.NumFields;
            nullSubVal = cell(1, numFields);
            for ii = 1:obj.NumFields
                nullSubVal{ii} = obj.field(ii).NullSubstitutionValue;
            end
        end
    end

    methods (Static)
        function array = fromArrays(arrowArrays, opts)
            arguments(Repeating)
                arrowArrays(1, 1) arrow.array.Array
            end
            arguments
                opts.FieldNames(1, :) string {mustBeNonmissing} = compose("Field%d", 1:numel(arrowArrays))
                opts.Valid
            end

            import arrow.tabular.internal.validateArrayLengths
            import arrow.tabular.internal.validateColumnNames
            import arrow.array.internal.getArrayProxyIDs
            import arrow.internal.validate.parseValid

            if numel(arrowArrays) == 0
                error("arrow:struct:ZeroFields", ...
                    "Must supply at least one field array.");
            end

            validateArrayLengths(arrowArrays);
            validateColumnNames(opts.FieldNames, numel(arrowArrays));
            validElements = parseValid(opts, arrowArrays{1}.NumElements);

            arrayProxyIDs = getArrayProxyIDs(arrowArrays);
            args = struct(ArrayProxyIDs=arrayProxyIDs, ...
                FieldNames=opts.FieldNames, Valid=validElements);
            proxyName = "arrow.array.proxy.StructArray";
            proxy = arrow.internal.proxy.create(proxyName, args);
            array = arrow.array.StructArray(proxy);
        end

        function array = fromMATLAB(T, opts)
            arguments
                T table
                opts.FieldNames(1, :) string {mustBeNonmissing} = T.Properties.VariableNames
                opts.Valid
            end

            import arrow.tabular.internal.decompose
            import arrow.tabular.internal.validateColumnNames
            import arrow.array.internal.getArrayProxyIDs
            import arrow.internal.validate.parseValid

            if width(T) == 0
                % StructArrays require at least one field
                error("arrow:struct:ZeroVariables", ...
                    "Input table T must have at least one variable.");
            end

            % If FieldNames was provided, make sure the number of field
            % names is equal to the width of the table.
            validateColumnNames(opts.FieldNames, width(T));

            arrowArrays = decompose(T);
            arrayProxyIDs = getArrayProxyIDs(arrowArrays);
            validElements = parseValid(opts, height(T));

            args = struct(ArrayProxyIDs=arrayProxyIDs, ...
                FieldNames=opts.FieldNames, Valid=validElements);
            proxyName = "arrow.array.proxy.StructArray";
            proxy = arrow.internal.proxy.create(proxyName, args);
            array = arrow.array.StructArray(proxy);
        end
    end
end