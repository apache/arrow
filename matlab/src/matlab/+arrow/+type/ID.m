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

classdef ID < uint64
%ID Data type enumeration
    enumeration
        Boolean (1)
        UInt8   (2)
        Int8    (3)
        UInt16  (4)
        Int16   (5)
        UInt32  (6)
        Int32   (7)
        UInt64  (8)
        Int64   (9)
        % Float16 (10) not yet supported
        Float32 (11)
        Float64 (12)
        String  (13)
        % Binary (14)
        % FixedSizeBinary (15)
        % Date32 (16)
        % Date64 (17)
        Timestamp (18)
    end

    methods
        function bitWidth = bitWidth(obj)
            import arrow.type.ID
            switch obj
                case ID.Boolean
                    bitWidth = 1;
                case {ID.UInt8, ID.Int8}
                    bitWidth = 8;
                case {ID.UInt16, ID.Int16}
                    bitWidth = 16;
                case {ID.UInt32, ID.Int32, ID.Float32}
                    bitWidth = 32;
                case {ID.UInt64, ID.Int64, ID.Float64, ID.Timestamp}
                    bitWidth = 64;
                otherwise
                    bitWidth = NaN;
            end
        end
    end
end
