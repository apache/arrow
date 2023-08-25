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

    properties(Access=private)
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
            % UNIX Epoch (January 1st, 1970)
            unixEpoch = datetime(0, ConvertFrom="posixtime");
            % Date32 value represents the number of days before
            % or after the Unix Epoch. This works for negative values
            % too.
            dates = unixEpoch + days(matlabArray);
            dates(~obj.Valid) = obj.NullSubstitutionValue;
        end

        function times = datetime(obj)
            times = obj.toMATLAB();
        end
    end

    methods(Static, Access=private)
        function ticks = convertDurationToTicks(data, timeUnit)
            if (timeUnit == arrow.type.TimeUnit.Second)
                ticks = cast(seconds(data), "int32");
            else
                ticks = cast(milliseconds(data), "int32");
            end
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
            % UNIX Epoch (January 1st, 1970)
            unixEpoch = datetime(0, ConvertFrom="posixtime");
            numDays = int32(floor(days(data - unixEpoch)));
            args = struct(MatlabArray=numDays, Valid=validElements);
            proxy = arrow.internal.proxy.create("arrow.array.proxy.Date32Array", args);
            array = Date32Array(proxy);
        end
    end

    methods (Access=protected)

        function displayScalarObject(obj)
            maxLength = 20;
            openBracket = "[";
            closeBracket = "]";
            indent = "  ";
            data = obj.toMATLAB();
            data.Format = "yyyy-MM-dd";
            % Abbreviate with ellipsis if more than maxLength elements.
            if obj.Length > maxLength
                firstTenElements = data(1:10);
                lastTenElements = data(end-9:end);
                ellipsis = "...";
                beforeEllipsis = indent + strjoin(string(firstTenElements), "," + newline + indent) + ",";
                afterEllipsis = indent + strjoin(string(lastTenElements), "," + newline + indent);
                str = ...
                    openBracket + newline + ...
                    beforeEllipsis + newline + ...
                    indent + ellipsis + newline + ...
                    afterEllipsis + newline + ...
                    closeBracket;
                disp(str);

            else
                str = openBracket + newline + ...
                    indent + strjoin(string(data), "," + newline + indent) + newline + ...
                    closeBracket;
                disp(str);
            end
        end

    end

end
