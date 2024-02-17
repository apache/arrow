%NUMERIC Validates numeric index values. Also validates that the index
%values do not exceed intmax(intType). 
% 
%intType must be  one of the following values: "int8", "int16", "int32", 
%"int64", "uint8", "uint16", "uint32", or "uint64".

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

function index = numeric(index, intType, opts)
    arguments
        index
        intType(1, 1) string
        opts.AllowNonScalar(1, 1) = true
    end

    if ~isnumeric(index)
        errid = "arrow:badsubscript:NonNumeric";
        msg = "Expected numeric index values.";
        error(errid, msg);
    end

    if ~opts.AllowNonScalar && ~isscalar(index)
        errid = "arrow:badsubscript:NonScalar";
        msg = "Expected a scalar index value.";
        error(errid, msg);
    end

     % Convert to full storage if sparse
    if issparse(index)
        index = full(index);
    end

    % Ensure the output is a column vector
    index = reshape(index, [], 1);

    if any(index < 1)
        errid = "arrow:badsubscript:NonPositive";
        msg = "Numeric indices must be positive integers.";
        error(errid, msg);
    elseif any(floor(index) ~= index) || any(isinf(index))
        errid = "arrow:badsubscript:NonInteger";
        msg = "Numeric indices must be finite positive integers.";
        error(errid, msg);
    elseif ~isreal(index)
        errid = "arrow:badsubscript:NonReal";
        msg = "Numeric indices must be positive real integers.";
        error(errid, msg);
    elseif any(index > intmax(intType))
        errid = "arrow:badsubscript:ExceedsIntMax";
        msg = "Index must be between 1 and intmax(""" + intType + """).";
        error(errid, msg);
    end

    index = cast(index, intType);
end
