
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

class Value {

  template<typename Type, enable_if_integer<Type>>
  static inline Result<typename Type::c_type> FromPython(Type* type, PyObject* obj) {
    typename Type::c_type value;
    RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    return value;
  }

};

// class Converter;

class Converter {
 public:
  // explicit Converter() {}

  virtual ~Converter() = default;

  virtual Result<std::shared_ptr<Array>> ToArray(PyObject *obj, int64_t size) = 0;
};

template <typename Type>
class PrimitiveConverter : public Converter {
 public:
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using CType = typename Type::c_type;

  explicit PrimitiveConverter(std::unique_ptr<ArrayBuilder> builder)
    : builder_(std::move(builder)) {}

  Result<std::shared_ptr<Array>> ToArray(PyObject *obj, int64_t size) override {
    // Type* type = checked_cast<Type*>(builder_->type());
    BuilderType* builder = checked_cast<BuilderType*>(builder_.get());

    // Ensure we've allocated enough space
    RETURN_NOT_OK(builder_->Reserve(size));

    // Iterate over the items adding each one
    return internal::VisitSequence(obj, [&, builder](PyObject* item, bool* /* unused */) {
      if (item == Py_None) {  // predict false
        return builder->AppendNull();
      } else {
        //ARROW_ASSIGN_OR_RAISE(auto value, 1);//Value::FromPython(type, item));
        CType value;
        RETURN_NOT_OK(internal::CIntFromPython(item, &value));
        return builder->Append(value);
      }
    });
  }

 protected:
  std::unique_ptr<ArrayBuilder> builder_;
};

Result<std::shared_ptr<Converter>> MakeConverter(const std::shared_ptr<DataType>& type,
                                                 MemoryPool* pool) {
  std::unique_ptr<ArrayBuilder> builder;
  switch (type->id()) {
    case Type::INT64:
      RETURN_NOT_OK(MakeBuilder(pool, type, &builder));
      return std::make_shared<PrimitiveConverter<Int64Type>>(std::move(builder));
      break;
    default:
      return Status::NotImplemented("Sequence converter for type ", type->ToString(),
                                    " not implemented");
  }
}

Status ConvertPySequence2(PyObject* sequence_source, PyObject* mask,
                          const PyConversionOptions& options,
                          std::shared_ptr<ChunkedArray>* out) {
  PyAcquireGIL lock;

  PyObject* seq;
  OwnedRef tmp_seq_nanny;

  std::shared_ptr<DataType> real_type;

  int64_t size = options.size;
  RETURN_NOT_OK(ConvertToSequenceAndInferSize(sequence_source, &seq, &size));
  tmp_seq_nanny.reset(seq);

  // In some cases, type inference may be "loose", like strings. If the user
  // passed pa.string(), then we will error if we encounter any non-UTF8
  // value. If not, then we will allow the result to be a BinaryArray
  bool strict_conversions = false;

  if (options.type == nullptr) {
    RETURN_NOT_OK(InferArrowType(seq, mask, options.from_pandas, &real_type));
  } else {
    real_type = options.type;
    strict_conversions = true;
  }
  DCHECK_GE(size, 0);

  // Create ArrayBuilder for type, then pass into the SeqConverter
  // instance. The reason this is created here rather than in GetConverter is
  // because of nested types (child SeqConverter objects need the child
  // builders created by MakeBuilder)
  // std::unique_ptr<ArrayBuilder> type_builder;
  // RETURN_NOT_OK(MakeBuilder(options.pool, real_type, &type_builder));
  // RETURN_NOT_OK(converter->Init(type_builder.get()));
  ARROW_ASSIGN_OR_RAISE(auto converter, MakeConverter(real_type, options.pool));
  ARROW_ASSIGN_OR_RAISE(auto array, converter->ToArray(seq, size));

  *out = std::make_shared<ChunkedArray>(
    std::vector<std::shared_ptr<Array>>{array}, real_type);

  return Status::OK();
}
