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

#pragma once

#include <functional>
#include <memory>
#include <string>

#include "arrow/c/abi.h"
#include "arrow/device.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \defgroup c-data-interface Functions for working with the C data interface.
///
/// @{

/// \brief Export C++ DataType using the C data interface format.
///
/// The root type is considered to have empty name and metadata.
/// If you want the root type to have a name and/or metadata, pass
/// a Field instead.
///
/// \param[in] type DataType object to export
/// \param[out] out C struct where to export the datatype
ARROW_EXPORT
Status ExportType(const DataType& type, struct ArrowSchema* out);

/// \brief Export C++ Field using the C data interface format.
///
/// \param[in] field Field object to export
/// \param[out] out C struct where to export the field
ARROW_EXPORT
Status ExportField(const Field& field, struct ArrowSchema* out);

/// \brief Export C++ Schema using the C data interface format.
///
/// \param[in] schema Schema object to export
/// \param[out] out C struct where to export the field
ARROW_EXPORT
Status ExportSchema(const Schema& schema, struct ArrowSchema* out);

/// \brief Export C++ Array using the C data interface format.
///
/// The resulting ArrowArray struct keeps the array data and buffers alive
/// until its release callback is called by the consumer.
///
/// \param[in] array Array object to export
/// \param[out] out C struct where to export the array
/// \param[out] out_schema optional C struct where to export the array type
ARROW_EXPORT
Status ExportArray(const Array& array, struct ArrowArray* out,
                   struct ArrowSchema* out_schema = NULLPTR);

/// \brief Export C++ RecordBatch using the C data interface format.
///
/// The record batch is exported as if it were a struct array.
/// The resulting ArrowArray struct keeps the record batch data and buffers alive
/// until its release callback is called by the consumer.
///
/// \param[in] batch Record batch to export
/// \param[out] out C struct where to export the record batch
/// \param[out] out_schema optional C struct where to export the record batch schema
ARROW_EXPORT
Status ExportRecordBatch(const RecordBatch& batch, struct ArrowArray* out,
                         struct ArrowSchema* out_schema = NULLPTR);

/// \brief Import C++ DataType from the C data interface.
///
/// The given ArrowSchema struct is released (as per the C data interface
/// specification), even if this function fails.
///
/// \param[in,out] schema C data interface struct representing the data type
/// \return Imported type object
ARROW_EXPORT
Result<std::shared_ptr<DataType>> ImportType(struct ArrowSchema* schema);

/// \brief Import C++ Field from the C data interface.
///
/// The given ArrowSchema struct is released (as per the C data interface
/// specification), even if this function fails.
///
/// \param[in,out] schema C data interface struct representing the field
/// \return Imported field object
ARROW_EXPORT
Result<std::shared_ptr<Field>> ImportField(struct ArrowSchema* schema);

/// \brief Import C++ Schema from the C data interface.
///
/// The given ArrowSchema struct is released (as per the C data interface
/// specification), even if this function fails.
///
/// \param[in,out] schema C data interface struct representing the field
/// \return Imported field object
ARROW_EXPORT
Result<std::shared_ptr<Schema>> ImportSchema(struct ArrowSchema* schema);

/// \brief Import C++ array from the C data interface.
///
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting array.
///
/// \param[in,out] array C data interface struct holding the array data
/// \param[in] type type of the imported array
/// \return Imported array object
ARROW_EXPORT
Result<std::shared_ptr<Array>> ImportArray(struct ArrowArray* array,
                                           std::shared_ptr<DataType> type);

/// \brief Import C++ array and its type from the C data interface.
///
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting array.
/// The ArrowSchema struct is released, even if this function fails.
///
/// \param[in,out] array C data interface struct holding the array data
/// \param[in,out] type C data interface struct holding the array type
/// \return Imported array object
ARROW_EXPORT
Result<std::shared_ptr<Array>> ImportArray(struct ArrowArray* array,
                                           struct ArrowSchema* type);

/// \brief Import C++ record batch from the C data interface.
///
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting record batch.
///
/// \param[in,out] array C data interface struct holding the record batch data
/// \param[in] schema schema of the imported record batch
/// \return Imported record batch object
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ImportRecordBatch(struct ArrowArray* array,
                                                       std::shared_ptr<Schema> schema);

/// \brief Import C++ record batch and its schema from the C data interface.
///
/// The type represented by the ArrowSchema struct must be a struct type array.
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting record batch.
/// The ArrowSchema struct is released, even if this function fails.
///
/// \param[in,out] array C data interface struct holding the record batch data
/// \param[in,out] schema C data interface struct holding the record batch schema
/// \return Imported record batch object
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ImportRecordBatch(struct ArrowArray* array,
                                                       struct ArrowSchema* schema);

/// @}

/// \defgroup c-data-device-interface Functions for working with the C data device
/// interface.
///
/// @{

/// \brief EXPERIMENTAL: Export C++ Array as an ArrowDeviceArray.
///
/// The resulting ArrowDeviceArray struct keeps the array data and buffers alive
/// until its release callback is called by the consumer. All buffers in
/// the provided array MUST have the same device_type, otherwise an error
/// will be returned.
///
/// If sync is non-null, get_event will be called on it in order to
/// potentially provide an event for consumers to synchronize on.
///
/// \param[in] array Array object to export
/// \param[in] sync shared_ptr to object derived from Device::SyncEvent or null
/// \param[out] out C struct to export the array to
/// \param[out] out_schema optional C struct to export the array type to
ARROW_EXPORT
Status ExportDeviceArray(const Array& array, std::shared_ptr<Device::SyncEvent> sync,
                         struct ArrowDeviceArray* out,
                         struct ArrowSchema* out_schema = NULLPTR);

/// \brief EXPERIMENTAL: Export C++ RecordBatch as an ArrowDeviceArray.
///
/// The record batch is exported as if it were a struct array.
/// The resulting ArrowDeviceArray struct keeps the record batch data and buffers alive
/// until its release callback is called by the consumer.
///
/// All buffers of all columns in the record batch must have the same device_type
/// otherwise an error will be returned. If columns are on different devices,
/// they should be exported using different ArrowDeviceArray instances.
///
/// If sync is non-null, get_event will be called on it in order to
/// potentially provide an event for consumers to synchronize on.
///
/// \param[in] batch Record batch to export
/// \param[in] sync shared_ptr to object derived from Device::SyncEvent or null
/// \param[out] out C struct where to export the record batch
/// \param[out] out_schema optional C struct where to export the record batch schema
ARROW_EXPORT
Status ExportDeviceRecordBatch(const RecordBatch& batch,
                               std::shared_ptr<Device::SyncEvent> sync,
                               struct ArrowDeviceArray* out,
                               struct ArrowSchema* out_schema = NULLPTR);

using DeviceMemoryMapper =
    std::function<Result<std::shared_ptr<MemoryManager>>(ArrowDeviceType, int64_t)>;

ARROW_EXPORT
Result<std::shared_ptr<MemoryManager>> DefaultDeviceMemoryMapper(
    ArrowDeviceType device_type, int64_t device_id);

/// \brief EXPERIMENTAL: Import C++ device array from the C data interface.
///
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting array. The
/// buffers of the Array are located on the device indicated by the device_type.
///
/// \param[in,out] array C data interface struct holding the array data
/// \param[in] type type of the imported array
/// \param[in] mapper A function to map device + id to memory manager. If not
/// specified, defaults to map "cpu" to the built-in default memory manager.
/// \return Imported array object
ARROW_EXPORT
Result<std::shared_ptr<Array>> ImportDeviceArray(
    struct ArrowDeviceArray* array, std::shared_ptr<DataType> type,
    const DeviceMemoryMapper& mapper = DefaultDeviceMemoryMapper);

/// \brief EXPERIMENTAL: Import C++ device array and its type from the C data interface.
///
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting array.
/// The ArrowSchema struct is released, even if this function fails. The
/// buffers of the Array are located on the device indicated by the device_type.
///
/// \param[in,out] array C data interface struct holding the array data
/// \param[in,out] type C data interface struct holding the array type
/// \param[in] mapper A function to map device + id to memory manager. If not
/// specified, defaults to map "cpu" to the built-in default memory manager.
/// \return Imported array object
ARROW_EXPORT
Result<std::shared_ptr<Array>> ImportDeviceArray(
    struct ArrowDeviceArray* array, struct ArrowSchema* type,
    const DeviceMemoryMapper& mapper = DefaultDeviceMemoryMapper);

/// \brief EXPERIMENTAL: Import C++ record batch with buffers on a device from the C data
/// interface.
///
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting record batch.
/// The buffers of all columns of the record batch are located on the device
/// indicated by the device type.
///
/// \param[in,out] array C data interface struct holding the record batch data
/// \param[in] schema schema of the imported record batch
/// \param[in] mapper A function to map device + id to memory manager. If not
/// specified, defaults to map "cpu" to the built-in default memory manager.
/// \return Imported record batch object
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ImportDeviceRecordBatch(
    struct ArrowDeviceArray* array, std::shared_ptr<Schema> schema,
    const DeviceMemoryMapper& mapper = DefaultDeviceMemoryMapper);

/// \brief EXPERIMENTAL: Import C++ record batch with buffers on a device and its schema
/// from the C data interface.
///
/// The type represented by the ArrowSchema struct must be a struct type array.
/// The ArrowArray struct has its contents moved (as per the C data interface
/// specification) to a private object held alive by the resulting record batch.
/// The ArrowSchema struct is released, even if this function fails. The buffers
/// of all columns of the record batch are located on the device indicated by the
/// device type.
///
/// \param[in,out] array C data interface struct holding the record batch data
/// \param[in,out] schema C data interface struct holding the record batch schema
/// \param[in] mapper A function to map device + id to memory manager. If not
/// specified, defaults to map "cpu" to the built-in default memory manager.
/// \return Imported record batch object
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ImportDeviceRecordBatch(
    struct ArrowDeviceArray* array, struct ArrowSchema* schema,
    const DeviceMemoryMapper& mapper = DefaultDeviceMemoryMapper);

/// @}

/// \defgroup c-stream-interface Functions for working with the C data interface.
///
/// @{

/// \brief Export C++ RecordBatchReader using the C stream interface.
///
/// The resulting ArrowArrayStream struct keeps the record batch reader alive
/// until its release callback is called by the consumer.
///
/// \param[in] reader RecordBatchReader object to export
/// \param[out] out C struct where to export the stream
ARROW_EXPORT
Status ExportRecordBatchReader(std::shared_ptr<RecordBatchReader> reader,
                               struct ArrowArrayStream* out);

/// \brief Export C++ ChunkedArray using the C data interface format.
///
/// The resulting ArrowArrayStream struct keeps the chunked array data and buffers alive
/// until its release callback is called by the consumer.
///
/// \param[in] chunked_array ChunkedArray object to export
/// \param[out] out C struct where to export the stream
ARROW_EXPORT
Status ExportChunkedArray(std::shared_ptr<ChunkedArray> chunked_array,
                          struct ArrowArrayStream* out);

/// \brief Import C++ RecordBatchReader from the C stream interface.
///
/// The ArrowArrayStream struct has its contents moved to a private object
/// held alive by the resulting record batch reader.
///
/// \param[in,out] stream C stream interface struct
/// \return Imported RecordBatchReader object
ARROW_EXPORT
Result<std::shared_ptr<RecordBatchReader>> ImportRecordBatchReader(
    struct ArrowArrayStream* stream);

/// \brief Import C++ ChunkedArray from the C stream interface
///
/// The ArrowArrayStream struct has its contents moved to a private object,
/// is consumed in its entirity, and released before returning all chunks
/// as a ChunkedArray.
///
/// \param[in,out] stream C stream interface struct
/// \return Imported ChunkedArray object
ARROW_EXPORT
Result<std::shared_ptr<ChunkedArray>> ImportChunkedArray(struct ArrowArrayStream* stream);

/// @}

}  // namespace arrow
