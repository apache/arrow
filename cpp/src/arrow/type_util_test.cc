TEST(TypeUtil, TypeSingleton) {
  // Test parameter-free types
  ASSERT_OK_AND_ASSIGN(auto null_type, type_singleton(Type::NA));
  ASSERT_EQ(*null_type, *null());

  ASSERT_OK_AND_ASSIGN(auto bool_type, type_singleton(Type::BOOL));
  ASSERT_EQ(*bool_type, *boolean());

  ASSERT_OK_AND_ASSIGN(auto int8_type, type_singleton(Type::INT8));
  ASSERT_EQ(*int8_type, *int8());

  ASSERT_OK_AND_ASSIGN(auto string_type, type_singleton(Type::STRING));
  ASSERT_EQ(*string_type, *utf8());

  // Test parameterized types
  ASSERT_RAISES(NotImplemented, type_singleton(Type::TIMESTAMP));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::TIME32));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::TIME64));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::DURATION));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::DECIMAL128));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::FIXED_SIZE_BINARY));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::LIST));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::LARGE_LIST));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::FIXED_SIZE_LIST));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::STRUCT));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::SPARSE_UNION));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::DENSE_UNION));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::DICTIONARY));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::EXTENSION));
  ASSERT_RAISES(NotImplemented, type_singleton(Type::MAP));
} 