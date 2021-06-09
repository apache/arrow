#include <sstream>

#include <arrow/extensions/complex_type.h>

namespace arrow {

std::shared_ptr<DataType> ComplexType::MakeType(std::shared_ptr<DataType> subtype)
{
    return fixed_size_list(FloatCast(subtype), 2);
}

std::shared_ptr<FloatingPointType> ComplexType::FloatCast(std::shared_ptr<DataType> subtype)
{
    auto float_type = std::dynamic_pointer_cast<FloatingPointType>(subtype);

    if(!float_type)
    {
        throw std::runtime_error("ComplexType subtype not floating point");
    }

    if(float_type->precision() != FloatingPointType::SINGLE &&
       float_type->precision() != FloatingPointType::DOUBLE)
    {
        throw std::runtime_error("Complex subtype must be single or double precision");        
    }

    return float_type;
}


std::string ComplexType::name() const {
    std::stringstream ss("complex");

    switch(subtype()->precision())
    {
        case FloatingPointType::SINGLE:
            ss << "64";
            break;
        case FloatingPointType::DOUBLE:
            ss << "128";
            break;
        case FloatingPointType::HALF:
        default:
            throw std::runtime_error("Complex Type must be single or double precision");
            break;
    }

    return ss.str();
}

std::string ComplexType::extension_name() const {
    return "complex";
}


bool ComplexType::ExtensionEquals(const ExtensionType& other) const {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
        return false;
    }
    return this->subtype() == static_cast<const ComplexType&>(other).subtype();
}

Result<std::shared_ptr<DataType>> ComplexType::Deserialize(
    std::shared_ptr<DataType> storage_type,
    const std::string& serialized) const {

    auto ltype = std::static_pointer_cast<ListType>(storage_type);
    return std::make_shared<ComplexType>(ltype->value_type());
}

std::string ComplexType::Serialize() const {
return "";
}

std::shared_ptr<DataType> complex(std::shared_ptr<DataType> subtype) {
  return std::make_shared<ComplexType>(subtype);
}

std::shared_ptr<DataType> complex64() {
    return std::make_shared<ComplexType>(float32());
}

std::shared_ptr<DataType> complex128() {
    return std::make_shared<ComplexType>(float64());
}

};