% arrow.array.Date32Array

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

classdef Date32Array < arrow.array.Array

    properties (Hidden, GetAccess=public, SetAccess=private)
        NullSubstitutionValue = NaT
    end

    methods

        function obj = Date32Array(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.Date32Array")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.Array(proxy);
        end

        function dates = toMATLAB(obj)
            import arrow.type.DateUnit

            matlabArray = obj.Proxy.toMATLAB();
            % UNIX Epoch (January 1st, 1970).
            unixEpoch = datetime(0, ConvertFrom="posixtime");
            % A Date32 value encodes a certain number of whole days
            % before or after the UNIX Epoch.
            dates = unixEpoch + days(matlabArray);
            dates(~obj.Valid) = obj.NullSubstitutionValue;
        end

        function dates = datetime(obj)
            dates = obj.toMATLAB();
        end

    end

    methods(Static)

        function array = fromMATLAB(data, opts)
            arguments
                data
                opts.InferNulls(1, 1) logical = true
                opts.Valid
            end

            import arrow.array.Date32Array

            arrow.internal.validate.type(data, "datetime");
            arrow.internal.validate.shape(data);

            validElements = arrow.internal.validate.parseValidElements(data, opts);

            % If the input MATLAB datetime array is zoned (i.e. has a TimeZone),
            % then the datetime representing the UNIX Epoch must also have a TimeZone.
            if ~isempty(data.TimeZone)
                unixEpoch = datetime(0, ConvertFrom="posixtime", TimeZone="UTC");
            else
                unixEpoch = datetime(0, ConvertFrom="posixtime");
            end

            % Explicitly round down (i.e. floor) to the nearest whole number
            % of days because durations and datetimes are not guaranteed
            % to encode "whole" number dates / times (e.g. 1.5 days is a valid duration)
            % and the int32 function rounds to the nearest whole number.
            % Rounding to the nearest whole number without flooring first would result in a
            % "round up error" of 1 whole day in cases where the fractional part of
            % the duration is large enough to result in rounding up (e.g. 1.5 days would
            % become 2 days).
            numDays = int32(floor(days(data - unixEpoch)));
            args = struct(MatlabArray=numDays, Valid=validElements);
            proxy = arrow.internal.proxy.create("arrow.array.proxy.Date32Array", args);
            array = Date32Array(proxy);
        end

    end

end
