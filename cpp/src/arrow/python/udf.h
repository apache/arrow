#pragma once

#include "arrow/python/platform.h"

#include <cstdint>
#include <memory>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"

#include "arrow/python/common.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/visibility.h"

#include <iostream>

namespace cp = arrow::compute;

namespace arrow {

namespace py {

// class ARROW_PYTHON_EXPORT UDFBuilder {
//   public:
//     virtual Status MakeFunction();

//   protected:
//     UDFBuilder(std::string func_name, cp::Function::Kind kind, cp::Arity arity, cp::FunctionDoc func_doc,
//                  std::vector<cp::InputType> in_types, cp::OutputType out_type)
//       : func_name_(func_name),
//         kind_(kind),
//         arity_(arity),
//         func_doc_(func_doc),
//         in_types_(in_types),
//         out_type_(out_type) {}

//     std::string func_name_;
//     cp::Function::Kind kind_;
//     cp::Arity arity_;
//     cp::FunctionDoc func_doc_;
//     std::vector<cp::InputType> in_types_;
//     cp::OutputType out_type_;
// };

// class ARROW_PYTHON_EXPORT ScalarUDFBuilder : public UDFBuilder{
//   public:
//     ScalarUDFBuilder(std::string func_name, cp::Arity arity, cp::FunctionDoc func_doc,
//                    std::vector<cp::InputType> in_types, cp::OutputType out_type, PyObject* function)
//         : UDFBuilder(func_name, cp::Function::SCALAR, arity, func_doc, in_types, out_type), function_(function)  {}

//     Status MakeFunction() override;

//   private:
//     PyObject* function_;

// };

}  // namespace py

}  // namespace arrow