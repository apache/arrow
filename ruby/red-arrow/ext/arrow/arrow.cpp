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

#include "red-arrow.hpp"
#include "memory-view.hpp"

#include <cstring>
#include <glib.h>
#include <ruby.hpp>

namespace red_arrow {
  VALUE cDate;

  VALUE cArrowTime;

  VALUE ArrowTimeUnitSECOND;
  VALUE ArrowTimeUnitMILLI;
  VALUE ArrowTimeUnitMICRO;
  VALUE ArrowTimeUnitNANO;

  ID id_BigDecimal;
  ID id_jd;
  ID id_new;
  ID id_to_datetime;

  namespace symbols {
    VALUE day;
    VALUE millisecond;
    VALUE month;
    VALUE nanosecond;
  }

  void
  record_batch_reader_mark(gpointer object)
  {
    auto reader = GARROW_RECORD_BATCH_READER(object);
    auto sources = garrow_record_batch_reader_get_sources(reader);
    for (auto source = sources; sources; sources = g_list_next(sources)) {
      rbgobj_gc_mark_instance(source->data);
    }
  }

  void
  execute_plan_mark(gpointer object)
  {
    auto plan = GARROW_EXECUTE_PLAN(object);
    auto nodes = garrow_execute_plan_get_nodes(plan);
    for (auto node = nodes; nodes; nodes = g_list_next(nodes)) {
      rbgobj_gc_mark_instance(node->data);
    }
  }

  static GHashTable*
  ruby_hash_to_ghash_table(VALUE rb_hash)
  {
    if (NIL_P(rb_hash)) {
      return nullptr;
    }

    if (rb_type(rb_hash) != T_HASH) {
      rb_raise(rb_eTypeError, "expected Hash or nil, got %s", rb_obj_classname(rb_hash));
    }

    auto hash_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
    VALUE keys = rb_funcall(rb_hash, rb_intern("keys"), 0);
    long len = RARRAY_LEN(keys);

    for (long i = 0; i < len; ++i) {
      VALUE key = rb_ary_entry(keys, i);
      VALUE value = rb_hash_aref(rb_hash, key);
      gchar* gkey = g_strdup(StringValueCStr(key));
      gchar* gvalue = g_strdup(StringValueCStr(value));
      g_hash_table_insert(hash_table, gkey, gvalue);
    }

    return hash_table;
  }

  static void
  ghash_table_foreach_callback(gpointer key, gpointer value, gpointer user_data)
  {
    VALUE rb_hash = reinterpret_cast<VALUE>(user_data);
    gchar* gkey = static_cast<gchar*>(key);
    gchar* gvalue = static_cast<gchar*>(value);
    VALUE rb_key = rb_utf8_str_new(gkey, strlen(gkey));
    VALUE rb_value = rb_utf8_str_new(gvalue, strlen(gvalue));
    rb_hash_aset(rb_hash, rb_key, rb_value);
  }

  static VALUE
  ghash_table_to_ruby_hash(GHashTable* hash_table)
  {
    if (!hash_table) {
      return Qnil;
    }

    VALUE rb_hash = rb_hash_new();
    g_hash_table_foreach(hash_table,
                        ghash_table_foreach_callback,
                        reinterpret_cast<gpointer>(rb_hash));
    return rb_hash;
  }

  VALUE
  make_struct_options_set_field_nullability(VALUE rb_options, VALUE rb_array)
  {
    auto options = GARROW_MAKE_STRUCT_OPTIONS(RVAL2GOBJ(rb_options));

    GArray* array = nullptr;
    if (!NIL_P(rb_array)) {
      Check_Type(rb_array, T_ARRAY);
      long len = RARRAY_LEN(rb_array);
      array = g_array_sized_new(FALSE, FALSE, sizeof(gboolean), len);

      for (long i = 0; i < len; ++i) {
        VALUE val = rb_ary_entry(rb_array, i);
        gboolean bool_val = RTEST(val) ? TRUE : FALSE;
        g_array_append_val(array, bool_val);
      }
    }

    GValue gvalue = G_VALUE_INIT;
    g_value_init(&gvalue, G_TYPE_ARRAY);
    g_value_take_boxed(&gvalue, array);
    g_object_set_property(G_OBJECT(options), "field-nullability", &gvalue);
    g_value_unset(&gvalue);

    return rb_options;
  }

  VALUE
  make_struct_options_get_field_nullability(VALUE rb_options)
  {
    auto options = GARROW_MAKE_STRUCT_OPTIONS(RVAL2GOBJ(rb_options));

    GValue gvalue = G_VALUE_INIT;
    g_value_init(&gvalue, G_TYPE_ARRAY);
    g_object_get_property(G_OBJECT(options), "field-nullability", &gvalue);

    GArray* array = static_cast<GArray*>(g_value_get_boxed(&gvalue));
    VALUE rb_array = rb_ary_new();

    if (array) {
      for (guint i = 0; i < array->len; ++i) {
        gboolean val = g_array_index(array, gboolean, i);
        rb_ary_push(rb_array, val ? Qtrue : Qfalse);
      }
    }

    g_value_unset(&gvalue);
    return rb_array;
  }

  VALUE
  make_struct_options_get_field_metadata(VALUE rb_options)
  {
    auto options = GARROW_MAKE_STRUCT_OPTIONS(RVAL2GOBJ(rb_options));

    GValue gvalue = G_VALUE_INIT;
    g_value_init(&gvalue, G_TYPE_PTR_ARRAY);
    g_object_get_property(G_OBJECT(options), "field-metadata", &gvalue);

    GPtrArray* array = static_cast<GPtrArray*>(g_value_get_boxed(&gvalue));
    VALUE rb_array = rb_ary_new();

    if (array) {
      for (guint i = 0; i < array->len; ++i) {
        GHashTable* hash_table = static_cast<GHashTable*>(g_ptr_array_index(array, i));
        VALUE rb_hash = ghash_table_to_ruby_hash(hash_table);
        rb_ary_push(rb_array, rb_hash);
      }
    }

    g_value_unset(&gvalue);
    return rb_array;
  }

  VALUE
  make_struct_options_set_field_metadata(VALUE rb_options, VALUE rb_array)
  {
    auto options = GARROW_MAKE_STRUCT_OPTIONS(RVAL2GOBJ(rb_options));

    GPtrArray* array = nullptr;
    if (!NIL_P(rb_array)) {
      Check_Type(rb_array, T_ARRAY);
      long len = RARRAY_LEN(rb_array);
      array = g_ptr_array_sized_new(len);

      for (long i = 0; i < len; ++i) {
        VALUE val = rb_ary_entry(rb_array, i);
        GHashTable* hash_table = ruby_hash_to_ghash_table(val);
        g_ptr_array_add(array, hash_table);
      }
    }

    GValue gvalue = G_VALUE_INIT;
    g_value_init(&gvalue, G_TYPE_PTR_ARRAY);
    g_value_take_boxed(&gvalue, array);
    g_object_set_property(G_OBJECT(options), "field-metadata", &gvalue);
    g_value_unset(&gvalue);

    return rb_options;
  }
}

extern "C" void Init_arrow() {
  auto mArrow = rb_const_get_at(rb_cObject, rb_intern("Arrow"));

  auto cArrowArray = rb_const_get_at(mArrow, rb_intern("Array"));
  rb_define_method(cArrowArray, "values",
                   reinterpret_cast<rb::RawMethod>(red_arrow::array_values),
                   0);

  auto cArrowChunkedArray = rb_const_get_at(mArrow, rb_intern("ChunkedArray"));
  rb_define_method(cArrowChunkedArray, "values",
                   reinterpret_cast<rb::RawMethod>(red_arrow::chunked_array_values),
                   0);

  auto cArrowRecordBatch = rb_const_get_at(mArrow, rb_intern("RecordBatch"));
  rb_define_method(cArrowRecordBatch, "raw_records",
                   reinterpret_cast<rb::RawMethod>(red_arrow::record_batch_raw_records),
                   0);
  rb_define_method(cArrowRecordBatch, "each_raw_record",
                   reinterpret_cast<rb::RawMethod>(red_arrow::record_batch_each_raw_record),
                   0);

  auto cArrowTable = rb_const_get_at(mArrow, rb_intern("Table"));
  rb_define_method(cArrowTable, "raw_records",
                   reinterpret_cast<rb::RawMethod>(red_arrow::table_raw_records),
                   0);
  rb_define_method(cArrowTable, "each_raw_record",
                   reinterpret_cast<rb::RawMethod>(red_arrow::table_each_raw_record),
                   0);

  auto cArrowMakeStructOptions = rb_const_get_at(mArrow, rb_intern("MakeStructOptions"));
  rb_define_method(cArrowMakeStructOptions, "field_nullability",
                   reinterpret_cast<rb::RawMethod>(red_arrow::make_struct_options_get_field_nullability),
                   0);
  rb_define_method(cArrowMakeStructOptions, "field_nullability=",
                   reinterpret_cast<rb::RawMethod>(red_arrow::make_struct_options_set_field_nullability),
                   1);
  rb_define_method(cArrowMakeStructOptions, "field_metadata",
                   reinterpret_cast<rb::RawMethod>(red_arrow::make_struct_options_get_field_metadata),
                   0);
  rb_define_method(cArrowMakeStructOptions, "field_metadata=",
                   reinterpret_cast<rb::RawMethod>(red_arrow::make_struct_options_set_field_metadata),
                   1);

  red_arrow::cDate = rb_const_get(rb_cObject, rb_intern("Date"));

  red_arrow::cArrowTime = rb_const_get_at(mArrow, rb_intern("Time"));

  auto cArrowTimeUnit = rb_const_get_at(mArrow, rb_intern("TimeUnit"));
  red_arrow::ArrowTimeUnitSECOND =
    rb_const_get_at(cArrowTimeUnit, rb_intern("SECOND"));
  red_arrow::ArrowTimeUnitMILLI =
    rb_const_get_at(cArrowTimeUnit, rb_intern("MILLI"));
  red_arrow::ArrowTimeUnitMICRO =
    rb_const_get_at(cArrowTimeUnit, rb_intern("MICRO"));
  red_arrow::ArrowTimeUnitNANO =
    rb_const_get_at(cArrowTimeUnit, rb_intern("NANO"));

  red_arrow::id_BigDecimal = rb_intern("BigDecimal");
  red_arrow::id_jd = rb_intern("jd");
  red_arrow::id_new = rb_intern("new");
  red_arrow::id_to_datetime = rb_intern("to_datetime");

  red_arrow::memory_view::init(mArrow);

  red_arrow::symbols::day = ID2SYM(rb_intern("day"));
  red_arrow::symbols::millisecond = ID2SYM(rb_intern("millisecond"));
  red_arrow::symbols::month = ID2SYM(rb_intern("month"));
  red_arrow::symbols::nanosecond = ID2SYM(rb_intern("nanosecond"));

  rbgobj_register_mark_func(GARROW_TYPE_RECORD_BATCH_READER,
                            red_arrow::record_batch_reader_mark);
  rbgobj_register_mark_func(GARROW_TYPE_EXECUTE_PLAN,
                            red_arrow::execute_plan_mark);
}
