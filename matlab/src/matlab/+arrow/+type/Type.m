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

classdef (Abstract) Type < matlab.mixin.CustomDisplay & ...
                           matlab.mixin.Heterogeneous
%TYPE Abstract type class. 

    properties (Dependent, GetAccess=public, SetAccess=private)
        ID
        Fields
        NumFields
    end

    properties (GetAccess=public, SetAccess=private, Hidden)
        Proxy
    end

    methods
        function obj = Type(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy
            end
            obj.Proxy = proxy;
        end

        function numFields = get.NumFields(obj)
            numFields = obj.Proxy.getNumFields();
        end

        function typeID = get.ID(obj)
            typeID = arrow.type.ID(obj.Proxy.getTypeID());
        end

        function F = field(obj, idx)
            import arrow.internal.validate.*

            idx = index.numeric(idx, "int32", AllowNonScalar=false);
            args = struct(Index=idx);
            proxyID = obj.Proxy.getFieldByIndex(args);
            proxy = libmexclass.proxy.Proxy(Name="arrow.type.proxy.Field", ID=proxyID);
            F = arrow.type.Field(proxy);
        end

        function fields = get.Fields(obj)
            numFields = obj.NumFields;
            if numFields == 0
                fields = arrow.type.Field.empty(0, 0);
            else
                fields = cell(1, numFields);
                for ii = 1:numFields
                    fields{ii} = obj.field(ii);
                end
                fields = horzcat(fields{:});
            end
        end
    end

    methods(Access = protected)
        groups = getDisplayPropertyGroups(obj)
    end

    methods (Sealed, Access = protected)
        function header = getHeader(obj)
            import arrow.internal.display.getClassNameForDisplay
            import arrow.internal.display.makeLinkString
            import arrow.internal.display.makeDimensionString

            if isempty(obj)
                fullClassName = "arrow.type.Type";
                typeLink = getClassNameForDisplay(fullClassName);
                dimensionString = makeDimensionString(size(obj));
                header = "  " + dimensionString + " " + typeLink + " array with properties:";
            elseif isscalar(obj)
                fullClassName = string(class(obj));
                typeLink = getClassNameForDisplay(fullClassName);
                header = "  " + typeLink + " with properties:";
            else
                dimensionString = makeDimensionString(size(obj));
                classNames = arrayfun(@(x) string(class(x)), obj);
                uniqueClasses = unique(classNames);
                if numel(uniqueClasses) == 1
                    typeLink = getClassNameForDisplay(uniqueClasses(1));
                    header = "  " + dimensionString + " " + typeLink + " array with properties:";
                else
                    heterogeneousLink = makeLinkString(HelpTarget="matlab.mixin.Heterogeneous", Text="heterogeneous", BoldFont=false);
                    baseClassName = string(class(obj));
                    baseClassLink = getClassNameForDisplay(baseClassName, BoldFont=true);
                    typeLinkParts = strings(size(uniqueClasses));
                    for ii = 1:numel(uniqueClasses)
                        c = uniqueClasses(ii);
                        parts = split(c, ".");
                        shortName = parts(end);
                        typeLinkParts(ii) = makeLinkString(HelpTarget=c, Text=shortName, BoldFont=false);
                    end
                    typeLinksStr = join(typeLinkParts, ", ");
                    header = "  " + dimensionString + " " + heterogeneousLink + " " + baseClassLink + ...
                        " (" + typeLinksStr + ") array with properties:";
                end
            end
        end
 
        function groups = getPropertyGroups(obj)
            if isscalar(obj)
               groups = getDisplayPropertyGroups(obj);
            else
                % Check if every type in the array has the same class type.
                % If so, call getDisplayPropertyGroups() so that all
                % properties associated with that class are displayed.
                classnames = arrayfun(@(type) string(class(type)), obj);
                if numel(unique(classnames)) == 1
                    groups = getDisplayPropertyGroups(obj(1));
                else
                    % If the array is heterogeneous, just display ID, which
                    % is the only property shared by all concrete
                    % subclasses of arrow.type.Type.
                    proplist = "ID";
                    groups = matlab.mixin.util.PropertyGroup(proplist);
                end
            end
        end
 
        function footer = getFooter(obj)
            footer = getFooter@matlab.mixin.CustomDisplay(obj);
        end
 
        function displayNonScalarObject(obj)
            displayNonScalarObject@matlab.mixin.CustomDisplay(obj);
        end

        function displayScalarObject(obj)
            displayScalarObject@matlab.mixin.CustomDisplay(obj)
        end

        function displayEmptyObject(obj)
            displayEmptyObject@matlab.mixin.CustomDisplay(obj);
        end

        function displayScalarHandleToDeletedObject(obj)
            displayScalarHandleToDeletedObject@matlab.mixin.CustomDisplay(obj);
        end
    end

    methods(Abstract, Hidden)
        data = preallocateMATLABArray(obj, length)
    end

    methods (Sealed)
        function tf = isequal(obj, varargin)

            narginchk(2, inf);
            tf = false;
            
            proxyIDs = zeros([numel(obj) numel(varargin)], "uint64");

            for ii = 1:numel(varargin)
                type = varargin{ii};
                if ~isa(type, "arrow.type.Type") || ~isequal(size(obj), size(type))
                    % Return early if type is not an arrow.type.Type or if
                    % the dimensions of obj and type do not match.
                    return;
                end

                % type(:) flattens N-dimensional arrays into a column
                % vector before collecting the Proxy properties into a
                % row vector.
                proxies = [type(:).Proxy];
                proxyIDs(:, ii) = [proxies.ID];
            end

            for ii = 1:numel(obj)
                % Invoke isEqual proxy method on each Type 
                % in the object array
                tf = obj(ii).Proxy.isEqual(proxyIDs(ii, :));
                if ~tf
                    return;
                end
            end
        end
    end
end
