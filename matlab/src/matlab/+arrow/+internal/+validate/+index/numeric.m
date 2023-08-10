%NUMERIC Validates the numeric index value.

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

function index = numeric(index, intType)

    % This function assumes index is numeric
    assert(isnumeric(index));

    if any(index < 1)
        errid = "arrow:badSubscript:NonPositive";
        msg = "Numeric indices must be positive integers.";
        error(errid, msg);
    elseif any(floor(index) ~= index) || any(isinf(index))
        errid = "arrow:badSubscript:NonInteger";
        msg = "Numeric indices must be finite positive integers.";
        error(errid, msg);
    elseif any(~isreal(index))
        errid = "arrow:badSubscript:NonReal";
        msg = "Numeric indices must be real positive integers.";
        error(errid, msg);
    elseif any(index > intmax(intType))
        errid = "arrow:badSubscript:ExceedsIntMax";
        msg = "Index must be between 1 and intmax(""" + intType + " "").";
        error(errid, msg);
    end

    % Convert to full storage if sparse
    if issparse(index)
        index = full(index);
    end

    index = cast(index, intType);
end
