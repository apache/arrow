#include "arrow/extension_type.h"

namespace arrow {

class ComplexArray : public ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class ComplexType : public ExtensionType {
 private:
  std::shared_ptr<FloatingPointType> subtype_;

  static std::shared_ptr<DataType> MakeType(std::shared_ptr<DataType> subtype);
  static std::shared_ptr<FloatingPointType> FloatCast(std::shared_ptr<DataType> subtype);

 public:
  explicit ComplexType(std::shared_ptr<DataType> subtype)
      : ExtensionType(MakeType(subtype)), subtype_(FloatCast(subtype)) {}

  std::shared_ptr<FloatingPointType> subtype() const { return subtype_; }
  std::string name() const override;
  std::string extension_name() const override;

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const override {
    return std::make_shared<ComplexArray>(data);
  }

  Result<std::shared_ptr<DataType>> Deserialize(
      std::shared_ptr<DataType> storage_type,
      const std::string& serialized) const override;

  std::string Serialize() const override;
};

std::shared_ptr<DataType> complex(std::shared_ptr<DataType> subtype);
std::shared_ptr<DataType> complex64();
std::shared_ptr<DataType> complex128();

};  // namespace arrow