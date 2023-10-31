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

classdef DatetimeValidator < arrow.array.internal.list.ClassTypeValidator

    properties (GetAccess=public, SetAccess=private)
        Zoned (1, 1) logical = false
    end

    methods
        function obj = DatetimeValidator(date)
            arguments
                date(:, :) datetime
            end
            obj@arrow.array.internal.list.ClassTypeValidator(date);
            obj.Zoned = ~isempty(date.TimeZone);
        end

        function validateElement(obj, element)
            validateElement@arrow.array.internal.list.ClassTypeValidator(obj, element);
            % zoned and obj.Zoned must be equal because zoned
            % and unzoned datetimes cannot be concatenated together.
            zoned = ~isempty(element.TimeZone);
            if obj.Zoned && ~zoned
                errorID = "arrow:array:list:ExpectedZonedDatetime";
                msg = "Expected all datetime elements in the cell array to " + ...
                    "have a time zone but encountered a datetime array without a time zone";
                error(errorID, msg);
            elseif ~obj.Zoned && zoned
                errorID = "arrow:array:list:ExpectedUnzonedDatetime";
                msg = "Expected all datetime elements in the cell array to " + ...
                    "not have a time zone but encountered a datetime array with a time zone";
                error(errorID, msg);
            end
        end
    end
end