Result<std::shared_ptr<DataType>> type_singleton(Type::type id) {
  switch (id) {
    case Type::NA:
      return TypeTraits<NullType>::type_singleton();
    case Type::BOOL:
      return TypeTraits<BooleanType>::type_singleton();
    case Type::INT8:
      return TypeTraits<Int8Type>::type_singleton();
    case Type::INT16:
      return TypeTraits<Int16Type>::type_singleton();
    case Type::INT32:
      return TypeTraits<Int32Type>::type_singleton();
    case Type::INT64:
      return TypeTraits<Int64Type>::type_singleton();
    case Type::UINT8:
      return TypeTraits<UInt8Type>::type_singleton();
    case Type::UINT16:
      return TypeTraits<UInt16Type>::type_singleton();
    case Type::UINT32:
      return TypeTraits<UInt32Type>::type_singleton();
    case Type::UINT64:
      return TypeTraits<UInt64Type>::type_singleton();
    case Type::HALF_FLOAT:
      return TypeTraits<HalfFloatType>::type_singleton();
    case Type::FLOAT:
      return TypeTraits<FloatType>::type_singleton();
    case Type::DOUBLE:
      return TypeTraits<DoubleType>::type_singleton();
    case Type::STRING:
      return TypeTraits<StringType>::type_singleton();
    case Type::BINARY:
      return TypeTraits<BinaryType>::type_singleton();
    case Type::LARGE_STRING:
      return TypeTraits<LargeStringType>::type_singleton();
    case Type::LARGE_BINARY:
      return TypeTraits<LargeBinaryType>::type_singleton();
    case Type::DATE32:
      return TypeTraits<Date32Type>::type_singleton();
    case Type::DATE64:
      return TypeTraits<Date64Type>::type_singleton();
    case Type::DAY_TIME_INTERVAL:
      return TypeTraits<DayTimeIntervalType>::type_singleton();
    case Type::MONTH_INTERVAL:
      return TypeTraits<MonthIntervalType>::type_singleton();
    case Type::MONTH_DAY_NANO_INTERVAL:
      return TypeTraits<MonthDayNanoIntervalType>::type_singleton();
    case Type::BINARY_VIEW:
      return TypeTraits<BinaryViewType>::type_singleton();
    case Type::STRING_VIEW:
      return TypeTraits<StringViewType>::type_singleton();
    default:
      return Status::NotImplemented("Type ", id, " requires parameters or is not supported");
  }
} 