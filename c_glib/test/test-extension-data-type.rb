# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

class TestExtensionDataType < Test::Unit::TestCase
  class UUIDArray < Arrow::ExtensionArray
    type_register
  end

  class UUIDDataType < Arrow::ExtensionDataType
    type_register

    def initialize
      super(storage_data_type: Arrow::FixedSizeBinaryDataType.new(16))
    end

    # TODO
    # def get_extension_name_impl
    #   "uuid"
    # end

    # TODO
    # def get_array_gtype_impl
    #   UUIDArray.gtype
    # end
  end

  include Helper::Buildable

  def test_type
    data_type = UUIDDataType.new
    assert_equal(Arrow::Type::EXTENSION, data_type.id)
  end

  def test_name
    data_type = UUIDDataType.new
    assert_equal("extension", data_type.name)
  end

  def test_to_s
    omit("gobject-introspection gem doesn't support implementing methods for GLib object yet")
    data_type = UUIDDataType.new
    assert_equal("extension<uuid>", data_type.to_s)
  end

  def test_storage_data_type
    data_type = UUIDDataType.new
    assert_equal(Arrow::FixedSizeBinaryDataType.new(16),
                 data_type.storage_data_type)
  end

  def test_extension_name
    omit("gobject-introspection gem doesn't support implementing methods for GLib object yet")
    data_type = UUIDDataType.new
    assert_equal("uuid", data_type.extension_name)
  end

  def test_wrap_array
    omit("gobject-introspection gem doesn't support implementing methods for GLib object yet")
    data_type = UUIDDataType.new
    storage = build_fixed_size_binary_array(data_type.storage_data_type,
                                            ["a" * 16, nil, "c" * 16])
    extension_array = data_type.wrap_array(storage)
    assert_equal([
                   UUIDArray,
                   storage,
                 ],
                 [
                   extension_array.class,
                   extension_array.storage,
                 ])
  end

  def test_wrap_chunked_array
    omit("gobject-introspection gem doesn't support implementing methods for GLib object yet")
    data_type = UUIDDataType.new
    storage1 = build_fixed_size_binary_array(data_type.storage_data_type,
                                             ["a" * 16, nil])
    storage2 = build_fixed_size_binary_array(data_type.storage_data_type,
                                             ["c" * 16])
    chunkd_array = Arrow::ChunkedArray.new([storage1, storage2])
    extension_chunked_array = data_type.wrap_chunked_array(chunked_array)
    assert_equal([
                   data_type,
                   [UUIDArray] * chunked_array.size,
                 ],
                 [
                   extension_chunked_array.get_value_data_type,
                   extension_chunked_array.chunks.collect(&:class),
                 ])
  end
end
