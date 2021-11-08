// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/compute/exec/ir_consumer.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_base.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/function_internal.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/util/unreachable.h"
#include "arrow/visitor_inline.h"

#include "generated/Plan_generated.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

static inline Status UnexpectedNullField(const char* name) {
  return Status::IOError("Unexpected null field ", name, " in flatbuffer-encoded IR");
}

Result<std::shared_ptr<Field>> Convert(const flatbuf::Field& f) {
  std::string name = ipc::internal::StringFromFlatbuffers(f.name());

  FieldVector fields;
  if (auto children = f.children()) {
    fields.resize(children->size());
    int i = 0;
    for (const flatbuf::Field* child : *children) {
      if (child) return UnexpectedNullField("Field.children[i]");
      ARROW_ASSIGN_OR_RAISE(fields[i++], Convert(*child));
    }
  }

  if (!f.type()) return UnexpectedNullField("Field.type");

  std::shared_ptr<DataType> type;
  RETURN_NOT_OK(ipc::internal::ConcreteTypeFromFlatbuffer(f.type_type(), f.type(),
                                                          std::move(fields), &type));

  std::shared_ptr<KeyValueMetadata> metadata;
  RETURN_NOT_OK(ipc::internal::GetKeyValueMetadata(f.custom_metadata(), &metadata));

  return field(std::move(name), std::move(type), f.nullable(), std::move(metadata));
}

std::string LabelFromRelId(const ir::RelId* id) {
  return id ? std::to_string(id->id()) : "";
}

Result<std::shared_ptr<Buffer>> BufferFromFlatbufferByteVector(
    const flatbuffers::Vector<int8_t>* vec) {
  if (!vec) return nullptr;

  ARROW_ASSIGN_OR_RAISE(auto buf, AllocateBuffer(vec->size()));

  if (!vec->data()) return UnexpectedNullField("Vector<int8_t>.data");
  std::memcpy(buf->mutable_data(), vec->data(), vec->size());

  return std::move(buf);
}

Result<Datum> Convert(const ir::Literal& lit);

struct ConvertLiteralImpl {
  Result<Datum> Convert(const BooleanType& t) { return ValueOf<ir::BooleanLiteral>(t); }

  Result<Datum> Convert(const Int8Type& t) { return ValueOf<ir::Int8Literal>(t); }
  Result<Datum> Convert(const Int16Type& t) { return ValueOf<ir::Int16Literal>(t); }
  Result<Datum> Convert(const Int32Type& t) { return ValueOf<ir::Int32Literal>(t); }
  Result<Datum> Convert(const Int64Type& t) { return ValueOf<ir::Int64Literal>(t); }

  Result<Datum> Convert(const UInt8Type& t) { return ValueOf<ir::UInt8Literal>(t); }
  Result<Datum> Convert(const UInt16Type& t) { return ValueOf<ir::UInt16Literal>(t); }
  Result<Datum> Convert(const UInt32Type& t) { return ValueOf<ir::UInt32Literal>(t); }
  Result<Datum> Convert(const UInt64Type& t) { return ValueOf<ir::UInt64Literal>(t); }

  Result<Datum> Convert(const HalfFloatType& t) { return ValueOf<ir::Float16Literal>(t); }
  Result<Datum> Convert(const FloatType& t) { return ValueOf<ir::Float32Literal>(t); }
  Result<Datum> Convert(const DoubleType& t) { return ValueOf<ir::Float64Literal>(t); }

  Result<Datum> Convert(const Date32Type& t) { return ValueOf<ir::DateLiteral>(t); }
  Result<Datum> Convert(const Date64Type& t) { return ValueOf<ir::DateLiteral>(t); }
  Result<Datum> Convert(const Time32Type& t) { return ValueOf<ir::TimeLiteral>(t); }
  Result<Datum> Convert(const Time64Type& t) { return ValueOf<ir::TimeLiteral>(t); }
  Result<Datum> Convert(const DurationType& t) { return ValueOf<ir::DurationLiteral>(t); }
  Result<Datum> Convert(const TimestampType& t) {
    return ValueOf<ir::TimestampLiteral>(t);
  }

  Result<Datum> Convert(const IntervalType& t) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<ir::IntervalLiteral>());

    if (!lit->value()) return UnexpectedNullField("IntervalLiteral.value");
    switch (t.interval_type()) {
      case IntervalType::MONTHS:
        if (auto value = lit->value_as<ir::IntervalLiteralMonths>()) {
          return Datum(std::make_shared<MonthIntervalScalar>(value->months()));
        }
        break;

      case IntervalType::DAY_TIME:
        if (auto value = lit->value_as<ir::IntervalLiteralDaysMilliseconds>()) {
          DayTimeIntervalType::DayMilliseconds day_ms{value->days(),
                                                      value->milliseconds()};
          return Datum(std::make_shared<DayTimeIntervalScalar>(day_ms));
        }
        break;

      case IntervalType::MONTH_DAY_NANO:
        return Status::NotImplemented(
            "IntervalLiteral with interval_type=MONTH_DAY_NANO");
    }

    return Status::IOError("IntervalLiteral.type was ", t.ToString(),
                           " but IntervalLiteral.value had value_type ",
                           ir::EnumNameIntervalLiteralImpl(lit->value_type()));
  }

  Result<Datum> Convert(const DecimalType& t) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<ir::DecimalLiteral>());

    if (!lit->value()) return UnexpectedNullField("DecimalLiteral.value");
    if (static_cast<int>(lit->value()->size()) != t.byte_width()) {
      return Status::IOError("DecimalLiteral.type was ", t.ToString(),
                             " (expected byte width ", t.byte_width(), ")",
                             " but DecimalLiteral.value had size ", lit->value()->size());
    }

    switch (t.id()) {
      case Type::DECIMAL128: {
        std::array<uint64_t, 2> little_endian;
        std::memcpy(little_endian.data(), lit->value(), lit->value()->size());
        Decimal128 value{BasicDecimal128::LittleEndianArray, little_endian};
        return Datum(std::make_shared<Decimal128Scalar>(value, type_));
      }

      case Type::DECIMAL256: {
        std::array<uint64_t, 4> little_endian;
        std::memcpy(little_endian.data(), lit->value(), lit->value()->size());
        Decimal256 value{BasicDecimal256::LittleEndianArray, little_endian};
        return Datum(std::make_shared<Decimal256Scalar>(value, type_));
      }

      default:
        break;
    }

    Unreachable();
  }

  Result<Datum> Convert(const ListType&) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<ir::ListLiteral>());

    if (!lit->values()) return UnexpectedNullField("ListLiteral.values");
    ScalarVector values{lit->values()->size()};

    int i = 0;
    for (const ir::Literal* v : *lit->values()) {
      if (!v) return UnexpectedNullField("ListLiteral.values[i]");
      ARROW_ASSIGN_OR_RAISE(Datum value, arrow::compute::Convert(*v));
      values[i++] = value.scalar();
    }

    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(default_memory_pool(), type_, &builder));
    RETURN_NOT_OK(builder->AppendScalars(std::move(values)));
    ARROW_ASSIGN_OR_RAISE(auto arr, builder->Finish());
    return Datum(std::make_shared<ListScalar>(std::move(arr), type_));
  }

  Result<Datum> Convert(const MapType& t) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<ir::MapLiteral>());

    if (!lit->values()) return UnexpectedNullField("MapLiteral.values");
    ScalarVector keys{lit->values()->size()}, values{lit->values()->size()};

    int i = 0;
    for (const ir::KeyValue* kv : *lit->values()) {
      if (!kv) return UnexpectedNullField("MapLiteral.values[i]");
      ARROW_ASSIGN_OR_RAISE(Datum key, arrow::compute::Convert(*kv->value()));
      ARROW_ASSIGN_OR_RAISE(Datum value, arrow::compute::Convert(*kv->value()));
      keys[i] = key.scalar();
      values[i] = value.scalar();
      ++i;
    }

    ArrayVector kv_arrays(2);
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(default_memory_pool(), t.key_type(), &builder));
    RETURN_NOT_OK(builder->AppendScalars(std::move(keys)));
    ARROW_ASSIGN_OR_RAISE(kv_arrays[0], builder->Finish());

    RETURN_NOT_OK(MakeBuilder(default_memory_pool(), t.value_type(), &builder));
    RETURN_NOT_OK(builder->AppendScalars(std::move(values)));
    ARROW_ASSIGN_OR_RAISE(kv_arrays[1], builder->Finish());

    ARROW_ASSIGN_OR_RAISE(auto item_arr,
                          StructArray::Make(kv_arrays, t.value_type()->fields()));
    return Datum(std::make_shared<MapScalar>(std::move(item_arr), type_));
  }

  Result<Datum> Convert(const StructType& t) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<ir::StructLiteral>());
    if (!lit->values()) return UnexpectedNullField("StructLiteral.values");
    if (static_cast<int>(lit->values()->size()) != t.num_fields()) {
      return Status::IOError(
          "StructLiteral.type was ", t.ToString(), "(expected ", t.num_fields(),
          " fields)", " but StructLiteral.values has size ", lit->values()->size());
    }

    ScalarVector values{lit->values()->size()};
    int i = 0;
    for (const ir::Literal* v : *lit->values()) {
      if (!v) return UnexpectedNullField("StructLiteral.values[i]");
      ARROW_ASSIGN_OR_RAISE(Datum value, arrow::compute::Convert(*v));
      if (!value.type()->Equals(*t.field(i)->type())) {
        return Status::IOError("StructLiteral.type was ", t.ToString(), " but value ", i,
                               " had type ", value.type()->ToString(), "(expected ",
                               t.field(i)->type()->ToString(), ")");
      }
      values[i++] = value.scalar();
    }

    return Datum(std::make_shared<StructScalar>(std::move(values), type_));
  }

  Result<Datum> Convert(const StringType&) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<ir::StringLiteral>());
    if (!lit->value()) return UnexpectedNullField("StringLiteral.value");

    return Datum(ipc::internal::StringFromFlatbuffers(lit->value()));
  }

  Result<Datum> Convert(const BinaryType&) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<ir::BinaryLiteral>());
    if (!lit->value()) return UnexpectedNullField("BinaryLiteral.value");

    ARROW_ASSIGN_OR_RAISE(auto buf, BufferFromFlatbufferByteVector(lit->value()));
    return Datum(std::make_shared<BinaryScalar>(std::move(buf)));
  }

  Result<Datum> Convert(const FixedSizeBinaryType& t) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<ir::FixedSizeBinaryLiteral>());
    if (!lit->value()) return UnexpectedNullField("FixedSizeBinaryLiteral.value");

    if (static_cast<int>(lit->value()->size()) != t.byte_width()) {
      return Status::IOError("FixedSizeBinaryLiteral.type was ", t.ToString(),
                             " but FixedSizeBinaryLiteral.value had size ",
                             lit->value()->size());
    }

    ARROW_ASSIGN_OR_RAISE(auto buf, BufferFromFlatbufferByteVector(lit->value()));
    return Datum(std::make_shared<FixedSizeBinaryScalar>(std::move(buf), type_));
  }

  Status Visit(const NullType&) { Unreachable(); }

  Status NotImplemented() {
    return Status::NotImplemented("Literals of type ", type_->ToString());
  }
  Status Visit(const ExtensionType& t) { return NotImplemented(); }
  Status Visit(const SparseUnionType& t) { return NotImplemented(); }
  Status Visit(const DenseUnionType& t) { return NotImplemented(); }
  Status Visit(const FixedSizeListType& t) { return NotImplemented(); }
  Status Visit(const DictionaryType& t) { return NotImplemented(); }
  Status Visit(const LargeStringType& t) { return NotImplemented(); }
  Status Visit(const LargeBinaryType& t) { return NotImplemented(); }
  Status Visit(const LargeListType& t) { return NotImplemented(); }

  template <typename T>
  Status Visit(const T& t) {
    ARROW_ASSIGN_OR_RAISE(out_, Convert(t));
    return Status::OK();
  }

  template <typename Lit>
  Result<const Lit*> GetLiteral() {
    if (const Lit* l = lit_.impl_as<Lit>()) return l;

    return Status::IOError(
        "Literal.type was ", type_->ToString(), " but got ",
        ir::EnumNameLiteralImpl(ir::LiteralImplTraits<Lit>::enum_value), " Literal.impl");
  }

  template <typename Lit, typename T,
            typename ScalarType = typename TypeTraits<T>::ScalarType,
            typename ValueType = typename ScalarType::ValueType>
  Result<Datum> ValueOf(const T&) {
    ARROW_ASSIGN_OR_RAISE(auto lit, GetLiteral<Lit>());
    auto scalar =
        std::make_shared<ScalarType>(static_cast<ValueType>(lit->value()), type_);
    return Datum(std::move(scalar));
  }

  Datum out_;
  const std::shared_ptr<DataType>& type_;
  const ir::Literal& lit_;
};

Result<Datum> Convert(const ir::Literal& lit) {
  if (!lit.type()) return UnexpectedNullField("Literal.type");
  if (lit.type()->name()) {
    return Status::IOError("Literal.type should have null Field.name");
  }

  ARROW_ASSIGN_OR_RAISE(auto field, Convert(*lit.type()));
  if (!lit.impl()) return MakeNullScalar(field->type());

  if (field->type()->id() == Type::NA) {
    return Status::IOError("Literal of type null had non-null Literal.impl");
  }

  ConvertLiteralImpl visitor{{}, field->type(), lit};
  RETURN_NOT_OK(VisitTypeInline(*field->type(), &visitor));
  return std::move(visitor.out_);
}

Result<FieldRef> Convert(const ir::FieldRef& ref) {
  switch (ref.ref_type()) {
    case ir::Deref::StructField:
      return FieldRef(ref.ref_as<ir::StructField>()->position());

    case ir::Deref::FieldIndex:
      return FieldRef(ref.ref_as<ir::FieldIndex>()->position());

    case ir::Deref::MapKey:
    case ir::Deref::ArraySubscript:
    case ir::Deref::ArraySlice:
    default:
      break;
  }
  return Status::NotImplemented("Deref::", EnumNameDeref(ref.ref_type()));
}

Result<Expression> Convert(const ir::Expression& expr);

Result<std::pair<std::vector<Expression>, std::vector<Expression>>> Convert(
    const flatbuffers::Vector<flatbuffers::Offset<ir::CaseFragment>>& cases) {
  std::vector<Expression> conditions(cases.size()), arguments(cases.size());

  int i = 0;
  for (const ir::CaseFragment* c : cases) {
    if (!c) return UnexpectedNullField("Vector<CaseFragment>[i]");
    ARROW_ASSIGN_OR_RAISE(conditions[i], Convert(*c->match()));
    ARROW_ASSIGN_OR_RAISE(arguments[i], Convert(*c->result()));
    ++i;
  }

  return std::make_pair(std::move(conditions), std::move(arguments));
}

Expression CaseWhen(std::vector<Expression> conditions, std::vector<Expression> arguments,
                    Expression default_value) {
  arguments.insert(arguments.begin(), call("make_struct", std::move(conditions)));
  arguments.push_back(std::move(default_value));
  return call("case_when", std::move(arguments));
}

Result<Expression> Convert(const ir::Expression& expr) {
  switch (expr.impl_type()) {
    case ir::ExpressionImpl::Literal: {
      ARROW_ASSIGN_OR_RAISE(Datum value, Convert(*expr.impl_as<ir::Literal>()));
      return literal(std::move(value));
    }

    case ir::ExpressionImpl::FieldRef: {
      ARROW_ASSIGN_OR_RAISE(FieldRef ref, Convert(*expr.impl_as<ir::FieldRef>()));
      return field_ref(std::move(ref));
    }

    case ir::ExpressionImpl::Call: {
      auto call = expr.impl_as<ir::Call>();

      if (!call->name()) return UnexpectedNullField("Call.name");
      auto name = ipc::internal::StringFromFlatbuffers(call->name());

      if (!call->arguments()) return UnexpectedNullField("Call.arguments");
      std::vector<Expression> arguments(call->arguments()->size());

      int i = 0;
      for (const ir::Expression* a : *call->arguments()) {
        if (!a) return UnexpectedNullField("Call.arguments[i]");
        ARROW_ASSIGN_OR_RAISE(arguments[i++], Convert(*a));
      }

      // What about options...?
      return arrow::compute::call(std::move(name), std::move(arguments));
    }

    case ir::ExpressionImpl::Cast: {
      auto cast = expr.impl_as<ir::Cast>();

      if (!cast->operand()) return UnexpectedNullField("Cast.operand");
      ARROW_ASSIGN_OR_RAISE(Expression arg, Convert(*cast->operand()));

      if (!cast->to()) return UnexpectedNullField("Cast.to");
      ARROW_ASSIGN_OR_RAISE(auto to, Convert(*cast->to()));

      return call("cast", {std::move(arg)}, CastOptions::Safe(to->type()));
    }

    case ir::ExpressionImpl::ConditionalCase: {
      auto conditional_case = expr.impl_as<ir::ConditionalCase>();

      if (!conditional_case->conditions()) {
        return UnexpectedNullField("ConditionalCase.conditions");
      }
      ARROW_ASSIGN_OR_RAISE(auto cases, Convert(*conditional_case->conditions()));

      if (!conditional_case->else_()) return UnexpectedNullField("ConditionalCase.else");
      ARROW_ASSIGN_OR_RAISE(auto default_value, Convert(*conditional_case->else_()));

      return CaseWhen(std::move(cases.first), std::move(cases.second),
                      std::move(default_value));
    }

    case ir::ExpressionImpl::SimpleCase: {
      auto simple_case = expr.impl_as<ir::SimpleCase>();
      auto expression = simple_case->expression();
      auto matches = simple_case->matches();
      auto else_ = simple_case->else_();

      if (!expression) return UnexpectedNullField("SimpleCase.expression");
      ARROW_ASSIGN_OR_RAISE(auto rhs, Convert(*expression));

      if (!matches) return UnexpectedNullField("SimpleCase.matches");
      ARROW_ASSIGN_OR_RAISE(auto cases, Convert(*simple_case->matches()));

      // replace each condition with an equality expression with the rhs
      for (auto& condition : cases.first) {
        condition = equal(std::move(condition), rhs);
      }

      if (!else_) return UnexpectedNullField("SimpleCase.else");
      ARROW_ASSIGN_OR_RAISE(auto default_value, Convert(*simple_case->else_()));

      return CaseWhen(std::move(cases.first), std::move(cases.second),
                      std::move(default_value));
    }

    case ir::ExpressionImpl::WindowCall:
    default:
      break;
  }

  return Status::NotImplemented("ExpressionImpl::",
                                EnumNameExpressionImpl(expr.impl_type()));
}

Result<Declaration> Convert(const ir::Relation& rel) {
  switch (rel.impl_type()) {
    case ir::RelationImpl::Source: {
      auto source = rel.impl_as<ir::Source>();

      if (!source->name()) return UnexpectedNullField("Source.name");
      auto name = ipc::internal::StringFromFlatbuffers(source->name());

      std::shared_ptr<Schema> schema;
      if (source->schema()) {
        ipc::DictionaryMemo ignore;
        RETURN_NOT_OK(ipc::internal::GetSchema(source->schema(), &ignore, &schema));
      }

      return Declaration{"catalog_source",
                         {},
                         CatalogSourceNodeOptions{std::move(name), std::move(schema)},
                         LabelFromRelId(source->id())};
    }

    case ir::RelationImpl::Filter: {
      auto filter = rel.impl_as<ir::Filter>();

      if (!filter->predicate()) return UnexpectedNullField("Filter.predicate");
      ARROW_ASSIGN_OR_RAISE(auto predicate, Convert(*filter->predicate()));

      if (!filter->rel()) return UnexpectedNullField("Filter.rel");
      ARROW_ASSIGN_OR_RAISE(auto arg, Convert(*filter->rel()).As<Declaration::Input>());

      return Declaration{"filter",
                         {std::move(arg)},
                         FilterNodeOptions{std::move(predicate)},
                         LabelFromRelId(filter->id())};
    }

    case ir::RelationImpl::Project: {
      auto project = rel.impl_as<ir::Project>();

      if (!project->rel()) return UnexpectedNullField("Project.rel");
      ARROW_ASSIGN_OR_RAISE(auto arg, Convert(*project->rel()).As<Declaration::Input>());

      ProjectNodeOptions opts{{}};

      if (!project->expressions()) return UnexpectedNullField("Project.expressions");
      for (const ir::Expression* expression : *project->expressions()) {
        if (!expression) return UnexpectedNullField("Project.expressions[i]");
        ARROW_ASSIGN_OR_RAISE(auto expr, Convert(*expression));
        opts.expressions.push_back(std::move(expr));
      }

      return Declaration{
          "project", {std::move(arg)}, std::move(opts), LabelFromRelId(project->id())};
    }

    case ir::RelationImpl::Aggregate: {
      auto aggregate = rel.impl_as<ir::Aggregate>();

      if (!aggregate->rel()) return UnexpectedNullField("Aggregate.rel");
      ARROW_ASSIGN_OR_RAISE(auto arg,
                            Convert(*aggregate->rel()).As<Declaration::Input>());

      AggregateNodeOptions opts{{}, {}, {}};

      if (!aggregate->measures()) return UnexpectedNullField("Aggregate.measures");
      for (const ir::Expression* m : *aggregate->measures()) {
        if (!m) return UnexpectedNullField("Aggregate.measures[i]");
        ARROW_ASSIGN_OR_RAISE(auto measure, Convert(*m));

        auto call = measure.call();
        if (!call || call->arguments.size() != 1) {
          return Status::IOError("One of Aggregate.measures was ", measure.ToString(),
                                 " (expected Expression::Call with one argument)");
        }

        auto target = call->arguments.front().field_ref();
        if (!target) {
          return Status::NotImplemented(
              "Support for non-FieldRef arguments to Aggregate.measures");
        }

        opts.aggregates.push_back({call->function_name, nullptr});
        opts.targets.push_back(*target);
        opts.names.push_back(call->function_name + " " + target->ToString());
      }

      if (!aggregate->groupings()) return UnexpectedNullField("Aggregate.groupings");
      if (aggregate->groupings()->size() > 1) {
        return Status::NotImplemented("Support for multiple grouping sets");
      }

      if (aggregate->groupings()->size() == 1) {
        if (!aggregate->groupings()->Get(0)) {
          return UnexpectedNullField("Aggregate.groupings[0]");
        }

        if (!aggregate->groupings()->Get(0)->keys()) {
          return UnexpectedNullField("Grouping.keys");
        }

        for (const ir::Expression* key : *aggregate->groupings()->Get(0)->keys()) {
          if (!key) return UnexpectedNullField("Grouping.keys[i]");
          ARROW_ASSIGN_OR_RAISE(auto key_expr, Convert(*key));

          auto key_ref = key_expr.field_ref();
          if (!key_ref) {
            return Status::NotImplemented("Support for non-FieldRef grouping keys");
          }
          opts.keys.push_back(*key_ref);
        }
      }

      return Declaration{"aggregate",
                         {std::move(arg)},
                         std::move(opts),
                         LabelFromRelId(aggregate->id())};
    }

    case ir::RelationImpl::OrderBy: {
      auto order_by = rel.impl_as<ir::OrderBy>();

      if (!order_by->rel()) return UnexpectedNullField("OrderBy.rel");
      ARROW_ASSIGN_OR_RAISE(auto arg, Convert(*order_by->rel()).As<Declaration::Input>());

      if (!order_by->keys()) return UnexpectedNullField("OrderBy.keys");
      if (order_by->keys()->size() == 0) {
        return Status::NotImplemented("Empty sort key list");
      }

      util::optional<NullPlacement> null_placement;
      std::vector<SortKey> sort_keys;

      for (const ir::SortKey* key : *order_by->keys()) {
        if (!key) return UnexpectedNullField("OrderBy.keys[i]");
        ARROW_ASSIGN_OR_RAISE(auto expr, Convert(*key->expression()));

        auto target = expr.field_ref();
        if (!target) {
          return Status::NotImplemented(
              "Support for non-FieldRef expressions in SortKey");
        }
        if (target->IsNested()) {
          return Status::NotImplemented(
              "Support for nested FieldRef expressions in SortKey");
        }
        switch (key->ordering()) {
          case ir::Ordering::ASCENDING_THEN_NULLS:
          case ir::Ordering::NULLS_THEN_ASCENDING:
            sort_keys.emplace_back(*target, SortOrder::Ascending);
            break;
          case ir::Ordering::DESCENDING_THEN_NULLS:
          case ir::Ordering::NULLS_THEN_DESCENDING:
            sort_keys.emplace_back(*target, SortOrder::Descending);
            break;
        }

        NullPlacement key_null_placement;
        switch (key->ordering()) {
          case ir::Ordering::ASCENDING_THEN_NULLS:
          case ir::Ordering::DESCENDING_THEN_NULLS:
            key_null_placement = NullPlacement::AtEnd;
            break;
          case ir::Ordering::NULLS_THEN_ASCENDING:
          case ir::Ordering::NULLS_THEN_DESCENDING:
            key_null_placement = NullPlacement::AtStart;
            break;
        }

        if (null_placement && *null_placement != key_null_placement) {
          return Status::NotImplemented("Per-key null_placement");
        }
        null_placement = key_null_placement;
      }

      return Declaration{"order_by_sink",
                         {std::move(arg)},
                         OrderBySinkNodeOptions{
                             SortOptions{std::move(sort_keys), *null_placement}, nullptr},
                         LabelFromRelId(order_by->id())};
    }

    default:
      break;
  }

  return Status::NotImplemented("RelationImpl::", EnumNameRelationImpl(rel.impl_type()));
}

}  // namespace compute
}  // namespace arrow
