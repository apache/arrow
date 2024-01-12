% Shared superclass for DateType-related tests for Date32Type and Date64Type
classdef tDateType < matlab.unittest.TestCase
    
    properties
        ConstructionFcn
        ArrowType
        TypeID
        BitWidth
        ClassName
    end
    
    methods (Test)
        function TestClass(testCase)
            % Verify ArrowType is an object of the expected class type.
            name = string(class(testCase.ArrowType));
            testCase.verifyEqual(name, testCase.ClassName);
        end

        function DefaultDateUnit(testCase)
            type = testCase.ConstructionFcn();
            actualUnit = type.DateUnit;
            expectedUnit = testCase.getDefaultDateUnit();
            testCase.verifyEqual(actualUnit, expectedUnit);
        end

        function DateUnitNoSetter(testCase)
            % Verify that an error is thrown when trying to set the value
            % of the DateUnit property.
            type = testCase.ConstructionFcn();
            testCase.verifyError(@() setfield(type, "DateUnit", "Millisecond"), "MATLAB:class:SetProhibited");
        end

        function InvalidProxy(testCase)
            % Verify that an error is thrown when a Proxy of an unexpected
            % type is passed to the DateType constructor.
            array = arrow.array([1, 2, 3]);
            proxy = array.Proxy;
            testCase.verifyError(@() testCase.ConstructionFcn(proxy), "arrow:proxy:ProxyNameMismatch");
        end

        function IsEqualTrue(testCase)
            % Verifies isequal method of DateType returns true if
            % conditions are met:
            %
            % 1. All input arguments have a class type DateType
            % 2. All inputs have the same size

            % Scalar DateType arrays
            dateType1 = testCase.ConstructionFcn();
            dateType2 = testCase.ConstructionFcn();
            testCase.verifyTrue(isequal(dateType1, dateType2));

            % Non-scalar DateType arrays
            typeArray1 = [dateType1 dateType1];
            typeArray2 = [dateType2 dateType2];
            testCase.verifyTrue(isequal(typeArray1, typeArray2));
        end

        function IsEqualFalse(testCase)
            % Verifies the isequal method of DateType returns false when expected.
            % Pass a different arrow.type.Type subclass to isequal
            dateType = testCase.ConstructionFcn();
            int32Type = arrow.int32();
            testCase.verifyFalse(isequal(dateType, int32Type));
            testCase.verifyFalse(isequal([dateType dateType], [int32Type int32Type]));

            % DateType arrays have different sizes
            typeArray1 = [dateType dateType];
            typeArray2 = [dateType dateType]';
            testCase.verifyFalse(isequal(typeArray1, typeArray2));
        end
    end

    methods (Access = private)
        function defaultUnit = retrieveDefaultDateUnit(~)
            defaultUnit = arrow.type.DateUnit.Millisecond;
        end
    end

end
