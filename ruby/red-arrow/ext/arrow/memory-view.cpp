/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "memory-view.hpp"

#include <arrow-glib/arrow-glib.hpp>
#include <rbgobject.h>

#include <ruby/version.h>

#if RUBY_API_VERSION_MAJOR >= 3
#  define HAVE_MEMORY_VIEW
#  define private memory_view_private
#  include <ruby/memory_view.h>
#  undef private
#endif

#include <sstream>

namespace red_arrow {
  namespace memory_view {
#ifdef HAVE_MEMORY_VIEW
    // This is workaround for the following rb_memory_view_t problems
    // in C++:
    //
    //   * Can't use "private" as member name
    //   * Can't assign a value to "rb_memory_view_t::private"
    //
    // This has compatible layout with rb_memory_view_t.
    struct memory_view {
      VALUE obj;
      void *data;
      ssize_t byte_size;
      bool readonly;
      const char *format;
      ssize_t item_size;
      struct {
        const rb_memory_view_item_component_t *components;
        size_t length;
      } item_desc;
      ssize_t ndim;
      const ssize_t *shape;
      const ssize_t *strides;
      const ssize_t *sub_offsets;
      void *private_data;
    };

    struct PrivateData {
      std::string format;
    };

    class PrimitiveArrayGetter : public arrow::ArrayVisitor {
    public:
      explicit PrimitiveArrayGetter(memory_view *view)
        : view_(view) {
      }

      arrow::Status Visit(const arrow::BooleanArray& array) override {
        fill(static_cast<const arrow::Array&>(array));
        // Memory view doesn't support bit stream. We use one byte
        // for 8 elements. Users can't calculate the number of
        // elements from memory view but it's limitation of memory view.
#ifdef ARROW_LITTLE_ENDIAN
        view_->format = "b8";
#else
        view_->format = "B8";
#endif
        view_->item_size = 1;
        view_->byte_size = (array.length() + 7) / 8;
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Int8Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "c";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Int16Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "s";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Int32Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "l";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Int64Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "q";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::UInt8Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "C";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::UInt16Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "S";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::UInt32Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "L";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::UInt64Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "Q";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::FloatArray& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "f";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::DoubleArray& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "d";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::FixedSizeBinaryArray& array) override {
        fill(static_cast<const arrow::Array&>(array));
        auto priv = static_cast<PrivateData *>(view_->private_data);
        const auto type =
          std::static_pointer_cast<const arrow::FixedSizeBinaryType>(
            array.type());
        std::ostringstream output;
        output << "C" << type->byte_width();
        priv->format = output.str();
        view_->format = priv->format.c_str();
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Date32Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "l";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Date64Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "q";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Time32Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "l";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Time64Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "q";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::TimestampArray& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "q";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Decimal128Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "q2";
        return arrow::Status::OK();
      }

      arrow::Status Visit(const arrow::Decimal256Array& array) override {
        fill(static_cast<const arrow::Array&>(array));
        view_->format = "q4";
        return arrow::Status::OK();
      }

      private:
      void fill(const arrow::Array& array) {
        const auto array_data = array.data();
        const auto data = array_data->GetValuesSafe<uint8_t>(1);
        view_->data = const_cast<void *>(reinterpret_cast<const void *>(data));
        const auto type =
          std::static_pointer_cast<const arrow::FixedWidthType>(array.type());
        view_->item_size = type->bit_width() / 8;
        view_->byte_size = view_->item_size * array.length();
      }

      memory_view *view_;
    };

    bool primitive_array_get(VALUE obj, rb_memory_view_t *view, int flags) {
      if (flags != RUBY_MEMORY_VIEW_SIMPLE) {
        return false;
      }
      auto view_ = reinterpret_cast<memory_view *>(view);
      view_->obj = obj;
      view_->private_data = new PrivateData();
      auto array = GARROW_ARRAY(RVAL2GOBJ(obj));
      auto arrow_array = garrow_array_get_raw(array);
      PrimitiveArrayGetter getter(view_);
      auto status = arrow_array->Accept(&getter);
      if (!status.ok()) {
        return false;
      }
      view_->readonly = true;
      view_->ndim = 1;
      view_->shape = NULL;
      view_->strides = NULL;
      view_->sub_offsets = NULL;
      return true;
    }

    bool primitive_array_release(VALUE obj, rb_memory_view_t *view) {
      auto view_ = reinterpret_cast<memory_view *>(view);
      delete static_cast<PrivateData *>(view_->private_data);
      return true;
    }

    bool primitive_array_available_p(VALUE obj) {
      return true;
    }

    rb_memory_view_entry_t primitive_array_entry = {
      primitive_array_get,
      primitive_array_release,
      primitive_array_available_p,
    };

    bool buffer_get(VALUE obj, rb_memory_view_t *view, int flags) {
      if (flags != RUBY_MEMORY_VIEW_SIMPLE) {
        return false;
      }
      auto view_ = reinterpret_cast<memory_view *>(view);
      view_->obj = obj;
      auto buffer = GARROW_BUFFER(RVAL2GOBJ(obj));
      auto arrow_buffer = garrow_buffer_get_raw(buffer);
      view_->data =
        const_cast<void *>(reinterpret_cast<const void *>(arrow_buffer->data()));
      // Memory view doesn't support bit stream. We use one byte
      // for 8 elements. Users can't calculate the number of
      // elements from memory view but it's limitation of memory view.
#ifdef ARROW_LITTLE_ENDIAN
      view_->format = "b8";
#else
      view_->format = "B8";
#endif
      view_->item_size = 1;
      view_->byte_size = arrow_buffer->size();
      view_->readonly = true;
      view_->ndim = 1;
      view_->shape = NULL;
      view_->strides = NULL;
      view_->sub_offsets = NULL;
      return true;
    }

    bool buffer_release(VALUE obj, rb_memory_view_t *view) {
      return true;
    }

    bool buffer_available_p(VALUE obj) {
      return true;
    }

    rb_memory_view_entry_t buffer_entry = {
      buffer_get,
      buffer_release,
      buffer_available_p,
    };
#endif

    void init(VALUE mArrow) {
#ifdef HAVE_MEMORY_VIEW
      auto cPrimitiveArray =
        rb_const_get_at(mArrow, rb_intern("PrimitiveArray"));
      rb_memory_view_register(cPrimitiveArray,
                              &(red_arrow::memory_view::primitive_array_entry));

      auto cBuffer = rb_const_get_at(mArrow, rb_intern("Buffer"));
      rb_memory_view_register(cBuffer, &(red_arrow::memory_view::buffer_entry));
#endif
    }
  }
}
