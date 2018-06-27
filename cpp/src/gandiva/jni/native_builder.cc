/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <google/protobuf/io/coded_stream.h>

#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include <memory>
#include <map>

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "jni/org_apache_arrow_gandiva_evaluator_NativeBuilder.h"
#include "jni/module_holder.h"
#include "Types.pb.h"
#include "gandiva/tree_expr_builder.h"

#define INIT_MODULE_ID   (4)

using gandiva::DataTypePtr;
using gandiva::FieldPtr;
using gandiva::FieldVector;
using gandiva::SchemaPtr;
using gandiva::NodePtr;
using gandiva::NodeVector;
using gandiva::ExpressionPtr;
using gandiva::ExpressionVector;
using gandiva::TreeExprBuilder;
using gandiva::Projector;

using gandiva::ArrayDataVector;
using gandiva::ProjectorHolder;

// forward declarations
NodePtr ProtoTypeToNode(const types::TreeNode& node);

// map from module ids returned to Java and module pointers
std::unordered_map<jlong, std::shared_ptr<ProjectorHolder>> projector_modules_map_;
std::mutex g_mtx_;

// atomic counter for projector module ids
jlong projector_module_id_(INIT_MODULE_ID);

// exception class
static jclass gandiva_exception_ = nullptr;

jlong MapInsert(std::shared_ptr<ProjectorHolder> holder) {
  g_mtx_.lock();

  jlong result = projector_module_id_++;
  projector_modules_map_.insert(
    std::pair<jlong, std::shared_ptr<ProjectorHolder>>(result, holder));

  g_mtx_.unlock();

  return result;
}

void MapErase(jlong module_id) {
  g_mtx_.lock();
  projector_modules_map_.erase(module_id);
  g_mtx_.unlock();
}

std::shared_ptr<ProjectorHolder> MapLookup(jlong module_id) {
  std::shared_ptr<ProjectorHolder> result = nullptr;

  try {
    result = projector_modules_map_.at(module_id);
  } catch (const std::out_of_range& e) {
  }

  return result;
}

DataTypePtr ProtoTypeToDataType(const types::ExtGandivaType& ext_type) {
  switch (ext_type.type()) {
    case types::NONE:
      return arrow::null();
    case types::BOOL:
      return arrow::boolean();
    case types::UINT8:
      return arrow::uint8();
    case types::INT8:
      return arrow::int8();
    case types::UINT16:
      return arrow::uint16();
    case types::INT16:
      return arrow::int16();
    case types::UINT32:
      return arrow::uint32();
    case types::INT32:
      return arrow::int32();
    case types::UINT64:
      return arrow::uint64();
    case types::INT64:
      return arrow::int64();
    case types::HALF_FLOAT:
      return arrow::float16();
    case types::FLOAT:
      return arrow::float32();
    case types::DOUBLE:
      return arrow::float64();
    case types::UTF8:
      return arrow::utf8();
    case types::BINARY:
      return arrow::binary();
    case types::DATE32:
      return arrow::date32();
    case types::DATE64:
      return arrow::date64();
    case types::DECIMAL:
      // TODO: error handling
      return arrow::decimal(ext_type.precision(), ext_type.scale());

    case types::FIXED_SIZE_BINARY:
    case types::TIMESTAMP:
    case types::TIME32:
    case types::TIME64:
    case types::INTERVAL:
    case types::LIST:
    case types::STRUCT:
    case types::UNION:
    case types::DICTIONARY:
    case types::MAP:
      std::cerr << "Unhandled data type: " << ext_type.type() << "\n";
      return nullptr;

    default:
      std::cerr << "Unknown data type: " << ext_type.type() << "\n";
      return nullptr;
  }
}

FieldPtr ProtoTypeToField(const types::Field& f) {
  const std::string &name = f.name();
  DataTypePtr type = ProtoTypeToDataType(f.type());
  bool nullable = true;
  if (f.has_nullable()) {
    nullable = f.nullable();
  }

  return field(name, type, nullable);
}

NodePtr ProtoTypeToFieldNode(const types::FieldNode& node) {
  FieldPtr field_ptr = ProtoTypeToField(node.field());
  if (field_ptr == nullptr) {
    std::cerr << "Unable to create field node from protobuf\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeField(field_ptr);
}

NodePtr ProtoTypeToFnNode(const types::FunctionNode& node) {
  const std::string &name = node.functionname();
  NodeVector children;

  for (int i = 0; i < node.inargs_size(); i++) {
    const types::TreeNode& arg = node.inargs(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for function: " << name << "\n";
      return nullptr;
    }

    children.push_back(n);
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == nullptr) {
    std::cerr << "Unknown return type for function: " << name << "\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeFunction(name, children, return_type);
}

NodePtr ProtoTypeToIfNode(const types::IfNode& node) {
  NodePtr cond = ProtoTypeToNode(node.cond());
  if (cond == nullptr) {
    std::cerr << "Unable to create cond node for if node\n";
    return nullptr;
  }

  NodePtr then_node = ProtoTypeToNode(node.thennode());
  if (then_node == nullptr) {
    std::cerr << "Unable to create then node for if node\n";
    return nullptr;
  }

  NodePtr else_node = ProtoTypeToNode(node.elsenode());
  if (else_node == nullptr) {
    std::cerr << "Unable to create else node for if node\n";
    return nullptr;
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == nullptr) {
    std::cerr << "Unknown return type for if node\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeIf(cond, then_node, else_node, return_type);
}

NodePtr ProtoTypeToAndNode(const types::AndNode& node) {
  NodeVector children;

  for (int i = 0; i < node.args_size(); i++) {
    const types::TreeNode& arg = node.args(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for boolean and\n";
      return nullptr;
    }
    children.push_back(n);
  }
  return TreeExprBuilder::MakeAnd(children);
}

NodePtr ProtoTypeToOrNode(const types::OrNode& node) {
  NodeVector children;

  for (int i = 0; i < node.args_size(); i++) {
    const types::TreeNode& arg = node.args(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for boolean or\n";
      return nullptr;
    }
    children.push_back(n);
  }
  return TreeExprBuilder::MakeOr(children);
}

NodePtr ProtoTypeToNode(const types::TreeNode& node) {
  if (node.has_fieldnode()) {
    return ProtoTypeToFieldNode(node.fieldnode());
  }

  if (node.has_fnnode()) {
    return ProtoTypeToFnNode(node.fnnode());
  }

  if (node.has_ifnode()) {
    return ProtoTypeToIfNode(node.ifnode());
  }

  if (node.has_andnode()) {
    return ProtoTypeToAndNode(node.andnode());
  }

  if (node.has_ornode()) {
    return ProtoTypeToOrNode(node.ornode());
  }

  if (node.has_intnode()) {
    return TreeExprBuilder::MakeLiteral(node.intnode().value());
  }

  if (node.has_floatnode()) {
    return TreeExprBuilder::MakeLiteral(node.floatnode().value());
  }

  if (node.has_longnode()) {
    return TreeExprBuilder::MakeLiteral(node.longnode().value());
  }

  if (node.has_booleannode()) {
    return TreeExprBuilder::MakeLiteral(node.booleannode().value());
  }

  if (node.has_doublenode()) {
    return TreeExprBuilder::MakeLiteral(node.doublenode().value());
  }

  std::cerr << "Unknown node type in protobuf\n";
  return nullptr;
}

ExpressionPtr ProtoTypeToExpression(const types::ExpressionRoot& root) {
  NodePtr root_node = ProtoTypeToNode(root.root());
  if (root_node == nullptr) {
    std::cerr << "Unable to create expression node from expression protobuf\n";
    return nullptr;
  }

  FieldPtr field = ProtoTypeToField(root.resulttype());
  if (field == nullptr) {
    std::cerr << "Unable to extra return field from expression protobuf\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeExpression(root_node, field);
}

SchemaPtr ProtoTypeToSchema(const types::Schema& schema) {
  std::vector<FieldPtr> fields;

  for (int i = 0; i < schema.columns_size(); i++) {
    FieldPtr field = ProtoTypeToField(schema.columns(i));
    if (field == nullptr) {
      std::cerr << "Unable to extract arrow field from schema\n";
      return nullptr;
    }

    fields.push_back(field);
  }

  return arrow::schema(fields);
}

bool ParseProtobuf(uint8_t *buf, int bufLen, google::protobuf::Message *msg) {
  google::protobuf::io::CodedInputStream cis(buf, bufLen);
  cis.SetRecursionLimit(1000);
  return msg->ParseFromCodedStream(&cis);
}

void ThrowException(JNIEnv *env, const std::string msg) {
  if (gandiva_exception_ == nullptr) {
    std::string className = "org.apache.arrow.gandiva.exceptions.GandivaException";
    gandiva_exception_ = env->FindClass(className.c_str());
  }

  if (gandiva_exception_ == nullptr) {
    // Cannot find GandivaException class
    // Cannot throw exception
    return;
  }

  env->ThrowNew(gandiva_exception_, msg.c_str());
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_gandiva_evaluator_NativeBuilder_buildNativeCode
  (JNIEnv *env, jclass cls, jbyteArray schema_arr, jbyteArray exprs_arr) {
  jlong module_id = 0LL;
  std::shared_ptr<Projector> projector;
  std::shared_ptr<ProjectorHolder> holder;

  types::Schema schema;
  jsize schema_len = env->GetArrayLength(schema_arr);
  jbyte *schema_bytes = env->GetByteArrayElements(schema_arr, 0);

  types::ExpressionList exprs;
  jsize exprs_len = env->GetArrayLength(exprs_arr);
  jbyte *exprs_bytes = env->GetByteArrayElements(exprs_arr, 0);

  ExpressionVector expr_vector;
  SchemaPtr schema_ptr;
  FieldVector ret_types;
  gandiva::Status status;

  if (!ParseProtobuf(reinterpret_cast<uint8_t *>(schema_bytes), schema_len, &schema)) {
    std::cerr << "Unable to parse schema protobuf\n";
    env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);
    goto err_out;
  }

  if (!ParseProtobuf(reinterpret_cast<uint8_t *>(exprs_bytes), exprs_len, &exprs)) {
    env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);
    env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
    std::cerr << "Unable to parse expressions protobuf\n";
    goto err_out;
  }

  // convert types::Schema to arrow::Schema
  schema_ptr = ProtoTypeToSchema(schema);
  if (schema_ptr == nullptr) {
    std::cerr << "Unable to construct arrow schema object from schema protobuf\n";
    env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);
    env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
    goto err_out;
  }

  // create Expression out of the list of exprs
  for (int i = 0; i < exprs.exprs_size(); i++) {
    ExpressionPtr root = ProtoTypeToExpression(exprs.exprs(i));

    if (root == nullptr) {
      std::cerr << "Unable to construct expression object from expression protobuf\n";
      env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);
      env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
      goto err_out;
    }

    expr_vector.push_back(root);
    ret_types.push_back(root->result());
  }

  // good to invoke the evaluator now
  status = Projector::Make(schema_ptr, expr_vector, nullptr, &projector);
  if (!status.ok()) {
    std::cerr << "Failed to make LLVM module due to " << status.message() << "\n";
    env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);
    env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
    goto err_out;
  }

  // store the result in a map
  holder = std::shared_ptr<ProjectorHolder>(new ProjectorHolder(schema_ptr,
                                                                ret_types,
                                                                std::move(projector)));
  module_id = MapInsert(holder);
  env->ReleaseByteArrayElements(schema_arr, schema_bytes, JNI_ABORT);
  env->ReleaseByteArrayElements(exprs_arr, exprs_bytes, JNI_ABORT);
  return module_id;

err_out:
  ThrowException(env, "Unable to create native module for the given expressions");
  return module_id;
}

JNIEXPORT void JNICALL Java_org_apache_arrow_gandiva_evaluator_NativeBuilder_evaluate
  (JNIEnv *env, jclass cls,
   jlong module_id, jint num_rows,
   jlongArray buf_addrs, jlongArray buf_sizes,
   jlongArray out_buf_addrs, jlongArray out_buf_sizes) {
  std::shared_ptr<ProjectorHolder> holder = MapLookup(module_id);
  if (holder == nullptr) {
    ThrowException(env, "Unknown module id");
    return;
  }

  jlong *in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
  jlong *in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

  jlong *out_bufs = env->GetLongArrayElements(out_buf_addrs, 0);
  jlong *out_sizes = env->GetLongArrayElements(out_buf_sizes, 0);

  auto schema = holder->schema();
  std::vector<std::shared_ptr<arrow::ArrayData>> columns;
  auto num_fields = schema->num_fields();
  int buf_idx = 0;
  int sz_idx = 0;

  for (int i = 0; i < num_fields; i++) {
    auto field = schema->field(i);
    jlong validity_addr = in_buf_addrs[buf_idx++];
    jlong value_addr = in_buf_addrs[buf_idx++];

    jlong validity_size = in_buf_sizes[sz_idx++];
    jlong value_size = in_buf_sizes[sz_idx++];

    auto validity = std::shared_ptr<arrow::Buffer>(
      new arrow::Buffer(reinterpret_cast<uint8_t *>(validity_addr), validity_size));
    auto data = std::shared_ptr<arrow::Buffer>(
      new arrow::Buffer(reinterpret_cast<uint8_t *>(value_addr), value_size));

    auto array_data = arrow::ArrayData::Make(field->type(), num_rows, {validity, data});
    columns.push_back(array_data);
  }

  auto ret_types = holder->rettypes();
  ArrayDataVector output;
  buf_idx = 0;
  sz_idx = 0;
  for (FieldPtr field : ret_types) {
    uint8_t *validity_buf = reinterpret_cast<uint8_t *>(out_bufs[buf_idx++]);
    uint8_t *value_buf = reinterpret_cast<uint8_t *>(out_bufs[buf_idx++]);

    jlong bitmap_sz = out_sizes[sz_idx++];
    jlong data_sz = out_sizes[sz_idx++];

    std::shared_ptr<arrow::MutableBuffer> bitmap_buf =
      std::make_shared<arrow::MutableBuffer>(validity_buf, bitmap_sz);
    std::shared_ptr<arrow::MutableBuffer> data_buf =
      std::make_shared<arrow::MutableBuffer>(value_buf, data_sz);

    auto array_data = arrow::ArrayData::Make(field->type(),
                                             num_rows,
                                             {bitmap_buf, data_buf});
    output.push_back(array_data);
  }

  auto in_batch = arrow::RecordBatch::Make(schema, num_rows, columns);
  gandiva::Status status = holder->projector()->Evaluate(*in_batch, output);

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  env->ReleaseLongArrayElements(out_buf_addrs, out_bufs, JNI_ABORT);
  env->ReleaseLongArrayElements(out_buf_sizes, out_sizes, JNI_ABORT);

  if (status.ok()) {
    return;
  }

  ThrowException(env, status.message());
}

JNIEXPORT void JNICALL Java_org_apache_arrow_gandiva_evaluator_NativeBuilder_close
  (JNIEnv *env, jclass cls, jlong module_id) {
  MapErase(module_id);
}
