/// \brief Create a DataType instance from its type ID
/// \param[in] id The type ID to create a DataType for
/// \return A Result containing the created DataType, or an error if the type
/// requires parameters or is not supported
ARROW_EXPORT Result<std::shared_ptr<DataType>> type_singleton(Type::type id); 