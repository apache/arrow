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

function typeTraits = traits(type)
    % "Gateway" function that links an arrow Type ID enumeration (e.g.
    % arrow.type.ID.String) or a MATLAB class string (e.g. "datetime")
    % to associated type information.
    import arrow.type.traits.*
    import arrow.type.*
    
    if isa(type, "arrow.type.ID")
        switch type
            case ID.UInt8
                typeTraits = UInt8Traits();
            case ID.UInt16
                typeTraits = UInt16Traits();
            case ID.UInt32
                typeTraits = UInt32Traits();
            case ID.UInt64
                typeTraits = UInt64Traits();
            case ID.Int8
                typeTraits = Int8Traits();
            case ID.Int16
                typeTraits = Int16Traits();
            case ID.Int32
                typeTraits = Int32Traits();
            case ID.Int64
                typeTraits = Int64Traits();
            case ID.Float32
                typeTraits = Float32Traits();
            case ID.Float64
                typeTraits = Float64Traits();
            case ID.Boolean
                typeTraits = BooleanTraits();
            case ID.String
                typeTraits = StringTraits();
            case ID.Timestamp
                typeTraits = TimestampTraits();
            case ID.Time32
                typeTraits = Time32Traits();
            case ID.Time64
                typeTraits = Time64Traits();
            case ID.Date32
                typeTraits = Date32Traits();
            case ID.Date64
                typeTraits = Date64Traits();
            case ID.List
                typeTraits = ListTraits();
            case ID.Struct
                typeTraits = StructTraits();
            otherwise
                error("arrow:type:traits:UnsupportedArrowTypeID", "Unsupported Arrow type ID: " + type);
        end
    elseif isa(type, "string") % MATLAB class string
        switch type
            case "uint8"
                typeTraits = UInt8Traits();
            case "uint16"
                typeTraits = UInt16Traits();
            case "uint32"
                typeTraits = UInt32Traits();
            case "uint64"
                typeTraits = UInt64Traits();
            case "int8"
                typeTraits = Int8Traits();
            case "int16"
                typeTraits = Int16Traits();
            case "int32"
                typeTraits = Int32Traits();
            case "int64"
                typeTraits = Int64Traits();
            case "single"
                typeTraits = Float32Traits();
            case "double"
                typeTraits = Float64Traits();
            case "logical"
                typeTraits = BooleanTraits();
            case "string"
                typeTraits = StringTraits();
            case "datetime"
                typeTraits = TimestampTraits();
            case "duration"
                typeTraits = Time64Traits();
            case "table"
                typeTraits = StructTraits();
            otherwise
                error("arrow:type:traits:UnsupportedMatlabClass", "Unsupported MATLAB class: " + type);
        end
    else
        error("arrow:type:traits:UnsupportedInputType", "The input argument to the traits function " + ...
                                                        "must be a MATLAB class string or an arrow.type.ID enumeration.");
    end
end
