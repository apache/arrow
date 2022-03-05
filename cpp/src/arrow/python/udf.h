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

class ARROW_PYTHON_EXPORT UdfBuilder {
 public:
  UdfBuilder(const std::string func_name, const cp::Function::Kind kind, const cp::Arity arity,
                      const cp::FunctionDoc* func_doc, const std::vector<cp::InputType> in_types,
                      const cp::OutputType out_type)
      : func_name_(func_name),
        kind_(kind),
        arity_(arity),
        func_doc_(func_doc),
        in_types_(in_types),
        out_type_(out_type) {}

  const std::string& name() const { return func_name_; }

  cp::Function::Kind kind() { return kind_; }

  const cp::Arity& arity() const { return arity_; }

  const cp::FunctionDoc& doc() const { return *func_doc_; }

  const std::vector<cp::InputType>& input_types() const { return in_types_; }

  const cp::OutputType& output_type() const { return out_type_; }

 private:
  std::string func_name_;
  cp::Function::Kind kind_;
  cp::Arity arity_;
  const cp::FunctionDoc* func_doc_;
  std::vector<cp::InputType> in_types_;
  cp::OutputType out_type_;
};

class ARROW_PYTHON_EXPORT ScalarUdfBuilder : public UdfBuilder {
 public:
  explicit ScalarUdfBuilder(const std::string func_name, const cp::Arity arity,
                            const cp::FunctionDoc* func_doc,
                            const std::vector<cp::InputType> in_types,
                            const cp::OutputType out_type)
      : UdfBuilder(func_name, cp::Function::SCALAR, arity, func_doc, in_types, out_type) {}

  Status MakeFunction(PyObject* function);

};

}  // namespace py

}  // namespace arrow