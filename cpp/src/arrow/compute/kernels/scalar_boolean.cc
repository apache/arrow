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

#include <array>

#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

using internal::Bitmap;

namespace compute {

namespace {

template <typename ComputeWord>
void ComputeKleene(ComputeWord&& compute_word, KernelContext* ctx, const ArrayData& left,
                   const ArrayData& right, ArrayData* out) {
  DCHECK(left.null_count != 0 || right.null_count != 0)
      << "ComputeKleene is unnecessarily expensive for the non-null case";

  Bitmap left_valid_bm{left.buffers[0], left.offset, left.length};
  Bitmap left_data_bm{left.buffers[1], left.offset, left.length};

  Bitmap right_valid_bm{right.buffers[0], right.offset, right.length};
  Bitmap right_data_bm{right.buffers[1], right.offset, right.length};

  std::array<Bitmap, 2> out_bms{Bitmap(out->buffers[0], out->offset, out->length),
                                Bitmap(out->buffers[1], out->offset, out->length)};

  auto apply = [&](uint64_t left_valid, uint64_t left_data, uint64_t right_valid,
                   uint64_t right_data, uint64_t* out_validity, uint64_t* out_data) {
    auto left_true = left_valid & left_data;
    auto left_false = left_valid & ~left_data;

    auto right_true = right_valid & right_data;
    auto right_false = right_valid & ~right_data;

    compute_word(left_true, left_false, right_true, right_false, out_validity, out_data);
  };

  if (right.null_count == 0) {
    std::array<Bitmap, 3> in_bms{left_valid_bm, left_data_bm, right_data_bm};
    Bitmap::VisitWordsAndWrite(
        in_bms, &out_bms,
        [&](const std::array<uint64_t, 3>& in, std::array<uint64_t, 2>* out) {
          apply(in[0], in[1], ~uint64_t(0), in[2], &(out->at(0)), &(out->at(1)));
        });
    return;
  }

  if (left.null_count == 0) {
    std::array<Bitmap, 3> in_bms{left_data_bm, right_valid_bm, right_data_bm};
    Bitmap::VisitWordsAndWrite(
        in_bms, &out_bms,
        [&](const std::array<uint64_t, 3>& in, std::array<uint64_t, 2>* out) {
          apply(~uint64_t(0), in[0], in[1], in[2], &(out->at(0)), &(out->at(1)));
        });
    return;
  }

  DCHECK(left.null_count != 0 && right.null_count != 0);
  std::array<Bitmap, 4> in_bms{left_valid_bm, left_data_bm, right_valid_bm,
                               right_data_bm};
  Bitmap::VisitWordsAndWrite(
      in_bms, &out_bms,
      [&](const std::array<uint64_t, 4>& in, std::array<uint64_t, 2>* out) {
        apply(in[0], in[1], in[2], in[3], &(out->at(0)), &(out->at(1)));
      });
}

inline BooleanScalar InvertScalar(const Scalar& in) {
  return in.is_valid ? BooleanScalar(!checked_cast<const BooleanScalar&>(in).value)
                     : BooleanScalar();
}

inline Bitmap GetBitmap(const ArrayData& arr, int index) {
  return Bitmap{arr.buffers[index], arr.offset, arr.length};
}

struct InvertOp {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    *checked_cast<BooleanScalar*>(out) = InvertScalar(in);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    GetBitmap(*out, 1).CopyFromInverted(GetBitmap(in, 1));
    return Status::OK();
  }
};

template <typename Op>
struct Commutative {
  static Status Call(KernelContext* ctx, const Scalar& left, const ArrayData& right,
                     ArrayData* out) {
    return Op::Call(ctx, right, left, out);
  }
};

struct AndOp : Commutative<AndOp> {
  using Commutative<AndOp>::Call;

  static Status Call(KernelContext* ctx, const Scalar& left, const Scalar& right,
                     Scalar* out) {
    if (left.is_valid && right.is_valid) {
      checked_cast<BooleanScalar*>(out)->value =
          checked_cast<const BooleanScalar&>(left).value &&
          checked_cast<const BooleanScalar&>(right).value;
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const Scalar& right,
                     ArrayData* out) {
    if (right.is_valid) {
      checked_cast<const BooleanScalar&>(right).value
          ? GetBitmap(*out, 1).CopyFrom(GetBitmap(left, 1))
          : GetBitmap(*out, 1).SetBitsTo(false);
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const ArrayData& right,
                     ArrayData* out) {
    ::arrow::internal::BitmapAnd(left.buffers[1]->data(), left.offset,
                                 right.buffers[1]->data(), right.offset, right.length,
                                 out->offset, out->buffers[1]->mutable_data());
    return Status::OK();
  }
};

struct KleeneAndOp : Commutative<KleeneAndOp> {
  using Commutative<KleeneAndOp>::Call;

  static Status Call(KernelContext* ctx, const Scalar& left, const Scalar& right,
                     Scalar* out) {
    bool left_true = left.is_valid && checked_cast<const BooleanScalar&>(left).value;
    bool left_false = left.is_valid && !checked_cast<const BooleanScalar&>(left).value;

    bool right_true = right.is_valid && checked_cast<const BooleanScalar&>(right).value;
    bool right_false = right.is_valid && !checked_cast<const BooleanScalar&>(right).value;

    checked_cast<BooleanScalar*>(out)->value = left_true && right_true;
    out->is_valid = left_false || right_false || (left_true && right_true);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const Scalar& right,
                     ArrayData* out) {
    bool right_true = right.is_valid && checked_cast<const BooleanScalar&>(right).value;
    bool right_false = right.is_valid && !checked_cast<const BooleanScalar&>(right).value;

    if (right_false) {
      out->null_count = 0;
      out->buffers[0] = nullptr;
      GetBitmap(*out, 1).SetBitsTo(false);  // all false case
      return Status::OK();
    }

    if (right_true) {
      if (left.GetNullCount() == 0) {
        out->null_count = 0;
        out->buffers[0] = nullptr;
      } else {
        GetBitmap(*out, 0).CopyFrom(GetBitmap(left, 0));
      }
      GetBitmap(*out, 1).CopyFrom(GetBitmap(left, 1));
      return Status::OK();
    }

    // scalar was null: out[i] is valid iff left[i] was false
    if (left.GetNullCount() == 0) {
      ::arrow::internal::InvertBitmap(left.buffers[1]->data(), left.offset, left.length,
                                      out->buffers[0]->mutable_data(), out->offset);
    } else {
      ::arrow::internal::BitmapAndNot(left.buffers[0]->data(), left.offset,
                                      left.buffers[1]->data(), left.offset, left.length,
                                      out->offset, out->buffers[0]->mutable_data());
    }
    ::arrow::internal::CopyBitmap(left.buffers[1]->data(), left.offset, left.length,
                                  out->buffers[1]->mutable_data(), out->offset);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const ArrayData& right,
                     ArrayData* out) {
    if (left.GetNullCount() == 0 && right.GetNullCount() == 0) {
      out->null_count = 0;
      // Kleene kernels have validity bitmap pre-allocated. Therefore, set it to 1
      BitUtil::SetBitmap(out->buffers[0]->mutable_data(), out->offset, out->length);
      return AndOp::Call(ctx, left, right, out);
    }
    auto compute_word = [](uint64_t left_true, uint64_t left_false, uint64_t right_true,
                           uint64_t right_false, uint64_t* out_valid,
                           uint64_t* out_data) {
      *out_data = left_true & right_true;
      *out_valid = left_false | right_false | (left_true & right_true);
    };
    ComputeKleene(compute_word, ctx, left, right, out);
    return Status::OK();
  }
};

struct OrOp : Commutative<OrOp> {
  using Commutative<OrOp>::Call;

  static Status Call(KernelContext* ctx, const Scalar& left, const Scalar& right,
                     Scalar* out) {
    if (left.is_valid && right.is_valid) {
      checked_cast<BooleanScalar*>(out)->value =
          checked_cast<const BooleanScalar&>(left).value ||
          checked_cast<const BooleanScalar&>(right).value;
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const Scalar& right,
                     ArrayData* out) {
    if (right.is_valid) {
      checked_cast<const BooleanScalar&>(right).value
          ? GetBitmap(*out, 1).SetBitsTo(true)
          : GetBitmap(*out, 1).CopyFrom(GetBitmap(left, 1));
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const ArrayData& right,
                     ArrayData* out) {
    ::arrow::internal::BitmapOr(left.buffers[1]->data(), left.offset,
                                right.buffers[1]->data(), right.offset, right.length,
                                out->offset, out->buffers[1]->mutable_data());
    return Status::OK();
  }
};

struct KleeneOrOp : Commutative<KleeneOrOp> {
  using Commutative<KleeneOrOp>::Call;

  static Status Call(KernelContext* ctx, const Scalar& left, const Scalar& right,
                     Scalar* out) {
    bool left_true = left.is_valid && checked_cast<const BooleanScalar&>(left).value;
    bool left_false = left.is_valid && !checked_cast<const BooleanScalar&>(left).value;

    bool right_true = right.is_valid && checked_cast<const BooleanScalar&>(right).value;
    bool right_false = right.is_valid && !checked_cast<const BooleanScalar&>(right).value;

    checked_cast<BooleanScalar*>(out)->value = left_true || right_true;
    out->is_valid = left_true || right_true || (left_false && right_false);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const Scalar& right,
                     ArrayData* out) {
    bool right_true = right.is_valid && checked_cast<const BooleanScalar&>(right).value;
    bool right_false = right.is_valid && !checked_cast<const BooleanScalar&>(right).value;

    if (right_true) {
      out->null_count = 0;
      out->buffers[0] = nullptr;
      GetBitmap(*out, 1).SetBitsTo(true);  // all true case
      return Status::OK();
    }

    if (right_false) {
      if (left.GetNullCount() == 0) {
        out->null_count = 0;
        out->buffers[0] = nullptr;
      } else {
        GetBitmap(*out, 0).CopyFrom(GetBitmap(left, 0));
      }
      GetBitmap(*out, 1).CopyFrom(GetBitmap(left, 1));
      return Status::OK();
    }

    // scalar was null: out[i] is valid iff left[i] was true
    if (left.GetNullCount() == 0) {
      ::arrow::internal::CopyBitmap(left.buffers[1]->data(), left.offset, left.length,
                                    out->buffers[0]->mutable_data(), out->offset);
    } else {
      ::arrow::internal::BitmapAnd(left.buffers[0]->data(), left.offset,
                                   left.buffers[1]->data(), left.offset, left.length,
                                   out->offset, out->buffers[0]->mutable_data());
    }
    ::arrow::internal::CopyBitmap(left.buffers[1]->data(), left.offset, left.length,
                                  out->buffers[1]->mutable_data(), out->offset);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const ArrayData& right,
                     ArrayData* out) {
    if (left.GetNullCount() == 0 && right.GetNullCount() == 0) {
      out->null_count = 0;
      // Kleene kernels have validity bitmap pre-allocated. Therefore, set it to 1
      BitUtil::SetBitmap(out->buffers[0]->mutable_data(), out->offset, out->length);
      return OrOp::Call(ctx, left, right, out);
    }

    static auto compute_word = [](uint64_t left_true, uint64_t left_false,
                                  uint64_t right_true, uint64_t right_false,
                                  uint64_t* out_valid, uint64_t* out_data) {
      *out_data = left_true | right_true;
      *out_valid = left_true | right_true | (left_false & right_false);
    };

    ComputeKleene(compute_word, ctx, left, right, out);
    return Status::OK();
  }
};

struct XorOp : Commutative<XorOp> {
  using Commutative<XorOp>::Call;

  static Status Call(KernelContext* ctx, const Scalar& left, const Scalar& right,
                     Scalar* out) {
    if (left.is_valid && right.is_valid) {
      checked_cast<BooleanScalar*>(out)->value =
          checked_cast<const BooleanScalar&>(left).value ^
          checked_cast<const BooleanScalar&>(right).value;
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const Scalar& right,
                     ArrayData* out) {
    if (right.is_valid) {
      checked_cast<const BooleanScalar&>(right).value
          ? GetBitmap(*out, 1).CopyFromInverted(GetBitmap(left, 1))
          : GetBitmap(*out, 1).CopyFrom(GetBitmap(left, 1));
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const ArrayData& right,
                     ArrayData* out) {
    ::arrow::internal::BitmapXor(left.buffers[1]->data(), left.offset,
                                 right.buffers[1]->data(), right.offset, right.length,
                                 out->offset, out->buffers[1]->mutable_data());
    return Status::OK();
  }
};

struct AndNotOp {
  static Status Call(KernelContext* ctx, const Scalar& left, const Scalar& right,
                     Scalar* out) {
    return AndOp::Call(ctx, left, InvertScalar(right), out);
  }

  static Status Call(KernelContext* ctx, const Scalar& left, const ArrayData& right,
                     ArrayData* out) {
    if (left.is_valid) {
      checked_cast<const BooleanScalar&>(left).value
          ? GetBitmap(*out, 1).CopyFromInverted(GetBitmap(right, 1))
          : GetBitmap(*out, 1).SetBitsTo(false);
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const Scalar& right,
                     ArrayData* out) {
    return AndOp::Call(ctx, left, InvertScalar(right), out);
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const ArrayData& right,
                     ArrayData* out) {
    ::arrow::internal::BitmapAndNot(left.buffers[1]->data(), left.offset,
                                    right.buffers[1]->data(), right.offset, right.length,
                                    out->offset, out->buffers[1]->mutable_data());
    return Status::OK();
  }
};

struct KleeneAndNotOp {
  static Status Call(KernelContext* ctx, const Scalar& left, const Scalar& right,
                     Scalar* out) {
    return KleeneAndOp::Call(ctx, left, InvertScalar(right), out);
  }

  static Status Call(KernelContext* ctx, const Scalar& left, const ArrayData& right,
                     ArrayData* out) {
    bool left_true = left.is_valid && checked_cast<const BooleanScalar&>(left).value;
    bool left_false = left.is_valid && !checked_cast<const BooleanScalar&>(left).value;

    if (left_false) {
      out->null_count = 0;
      out->buffers[0] = nullptr;
      GetBitmap(*out, 1).SetBitsTo(false);  // all false case
      return Status::OK();
    }

    if (left_true) {
      if (right.GetNullCount() == 0) {
        out->null_count = 0;
        out->buffers[0] = nullptr;
      } else {
        GetBitmap(*out, 0).CopyFrom(GetBitmap(right, 0));
      }
      GetBitmap(*out, 1).CopyFromInverted(GetBitmap(right, 1));
      return Status::OK();
    }

    // scalar was null: out[i] is valid iff right[i] was true
    if (right.GetNullCount() == 0) {
      ::arrow::internal::CopyBitmap(right.buffers[1]->data(), right.offset, right.length,
                                    out->buffers[0]->mutable_data(), out->offset);
    } else {
      ::arrow::internal::BitmapAnd(right.buffers[0]->data(), right.offset,
                                   right.buffers[1]->data(), right.offset, right.length,
                                   out->offset, out->buffers[0]->mutable_data());
    }
    ::arrow::internal::InvertBitmap(right.buffers[1]->data(), right.offset, right.length,
                                    out->buffers[1]->mutable_data(), out->offset);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const Scalar& right,
                     ArrayData* out) {
    return KleeneAndOp::Call(ctx, left, InvertScalar(right), out);
  }

  static Status Call(KernelContext* ctx, const ArrayData& left, const ArrayData& right,
                     ArrayData* out) {
    if (left.GetNullCount() == 0 && right.GetNullCount() == 0) {
      out->null_count = 0;
      // Kleene kernels have validity bitmap pre-allocated. Therefore, set it to 1
      BitUtil::SetBitmap(out->buffers[0]->mutable_data(), out->offset, out->length);
      return AndNotOp::Call(ctx, left, right, out);
    }

    static auto compute_word = [](uint64_t left_true, uint64_t left_false,
                                  uint64_t right_true, uint64_t right_false,
                                  uint64_t* out_valid, uint64_t* out_data) {
      *out_data = left_true & right_false;
      *out_valid = left_false | right_true | (left_true & right_false);
    };

    ComputeKleene(compute_word, ctx, left, right, out);
    return Status::OK();
  }
};

void MakeFunction(const std::string& name, int arity, ArrayKernelExec exec,
                  const FunctionDoc* doc, FunctionRegistry* registry,
                  NullHandling::type null_handling = NullHandling::INTERSECTION) {
  auto func = std::make_shared<ScalarFunction>(name, Arity(arity), doc);

  // Scalar arguments not yet supported
  std::vector<InputType> in_types(arity, InputType(boolean()));
  ScalarKernel kernel(std::move(in_types), boolean(), exec);
  kernel.null_handling = null_handling;

  DCHECK_OK(func->AddKernel(kernel));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

const FunctionDoc invert_doc{"Invert boolean values", "", {"values"}};

const FunctionDoc and_doc{
    "Logical 'and' boolean values",
    ("When a null is encountered in either input, a null is output.\n"
     "For a different null behavior, see function \"and_kleene\"."),
    {"x", "y"}};

const FunctionDoc and_not_doc{
    "Logical 'and not' boolean values",
    ("When a null is encountered in either input, a null is output.\n"
     "For a different null behavior, see function \"and_not_kleene\"."),
    {"x", "y"}};

const FunctionDoc or_doc{
    "Logical 'or' boolean values",
    ("When a null is encountered in either input, a null is output.\n"
     "For a different null behavior, see function \"or_kleene\"."),
    {"x", "y"}};

const FunctionDoc xor_doc{
    "Logical 'xor' boolean values",
    ("When a null is encountered in either input, a null is output."),
    {"x", "y"}};

const FunctionDoc and_kleene_doc{
    "Logical 'and' boolean values (Kleene logic)",
    ("This function behaves as follows with nulls:\n\n"
     "- true and null = null\n"
     "- null and true = null\n"
     "- false and null = false\n"
     "- null and false = false\n"
     "- null and null = null\n"
     "\n"
     "In other words, in this context a null value really means \"unknown\",\n"
     "and an unknown value 'and' false is always false.\n"
     "For a different null behavior, see function \"and\"."),
    {"x", "y"}};

const FunctionDoc and_not_kleene_doc{
    "Logical 'and not' boolean values (Kleene logic)",
    ("This function behaves as follows with nulls:\n\n"
     "- true and null = null\n"
     "- null and false = null\n"
     "- false and null = false\n"
     "- null and true = false\n"
     "- null and null = null\n"
     "\n"
     "In other words, in this context a null value really means \"unknown\",\n"
     "and an unknown value 'and not' true is always false, as is false\n"
     "'and not' an unknown value.\n"
     "For a different null behavior, see function \"and_not\"."),
    {"x", "y"}};

const FunctionDoc or_kleene_doc{
    "Logical 'or' boolean values (Kleene logic)",
    ("This function behaves as follows with nulls:\n\n"
     "- true or null = true\n"
     "- null and true = true\n"
     "- false and null = null\n"
     "- null and false = null\n"
     "- null and null = null\n"
     "\n"
     "In other words, in this context a null value really means \"unknown\",\n"
     "and an unknown value 'or' true is always true.\n"
     "For a different null behavior, see function \"and\"."),
    {"x", "y"}};

}  // namespace

namespace internal {

void RegisterScalarBoolean(FunctionRegistry* registry) {
  // These functions can write into sliced output bitmaps
  MakeFunction("invert", 1, applicator::SimpleUnary<InvertOp>, &invert_doc, registry);
  MakeFunction("and", 2, applicator::SimpleBinary<AndOp>, &and_doc, registry);
  MakeFunction("and_not", 2, applicator::SimpleBinary<AndNotOp>, &and_not_doc, registry);
  MakeFunction("or", 2, applicator::SimpleBinary<OrOp>, &or_doc, registry);
  MakeFunction("xor", 2, applicator::SimpleBinary<XorOp>, &xor_doc, registry);

  MakeFunction("and_kleene", 2, applicator::SimpleBinary<KleeneAndOp>, &and_kleene_doc,
               registry, NullHandling::COMPUTED_PREALLOCATE);
  MakeFunction("and_not_kleene", 2, applicator::SimpleBinary<KleeneAndNotOp>,
               &and_not_kleene_doc, registry, NullHandling::COMPUTED_PREALLOCATE);
  MakeFunction("or_kleene", 2, applicator::SimpleBinary<KleeneOrOp>, &or_kleene_doc,
               registry, NullHandling::COMPUTED_PREALLOCATE);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
