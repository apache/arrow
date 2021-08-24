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

#include "arrow/dataset/c/api.h"
#include "arrow/dataset/c/util_internal.h"
#include "arrow/dataset/api.h"
#include "arrow/c/bridge.h"
#include <errno.h>
#include <iostream>

using arrow::dataset::internal::create_native_ref;
using arrow::dataset::internal::retrieve_native_instance;
using arrow::dataset::internal::release_native_ref;

const int kInspectAllFragments = arrow::dataset::InspectOptions::kInspectAllFragments;

namespace {

void set_errno(arrow::Status s) {
    switch (s.code()) {
    case arrow::StatusCode::OutOfMemory:
        errno = ENOMEM;
        return;
    case arrow::StatusCode::KeyError:
        errno = ENOKEY;
        return;
    case arrow::StatusCode::TypeError:
        errno = EBADE;
        return;
    case arrow::StatusCode::Invalid:
        errno = EINVAL;
        return;
    case arrow::StatusCode::IOError:
        errno = EIO;
        return;
    case arrow::StatusCode::CapacityError:
        errno = EXFULL;
        return;
    case arrow::StatusCode::IndexError:
        errno = ENODATA;
        return;
    case arrow::StatusCode::Cancelled:
        errno = ECANCELED;
        return;
    case arrow::StatusCode::UnknownError:
        errno = EAGAIN;
        return;
    case arrow::StatusCode::NotImplemented:
        errno = ENOSYS;
        return;
    case arrow::StatusCode::SerializationError:
        errno = EPROTO;
        return;
    case arrow::StatusCode::AlreadyExists:
        errno = EEXIST;
        return;
    default:
        errno = 0;
        return;
    }
}

template <typename T>
void set_errno(arrow::Result<T> t) {
    if (t.ok()) {
        errno = 0;
        return;
    }

    set_errno(t.status());
}

arrow::Result<std::shared_ptr<arrow::dataset::FileFormat>> get_file_format(const int format) {
    switch (format) {
    case DS_PARQUET_FORMAT:
        return std::make_shared<arrow::dataset::ParquetFileFormat>();
    case DS_CSV_FORMAT:
        return std::make_shared<arrow::dataset::CsvFileFormat>();
    case DS_IPC_FORMAT:
        return std::make_shared<arrow::dataset::IpcFileFormat>();
    default:
        std::string error_message = "illegal file format id: "+std::to_string(format);
        return arrow::Status::Invalid(error_message);
    }
}

}

DatasetFactory factory_from_path(const char* uri, const int file_format_id) {
    auto file_format = get_file_format(file_format_id);
    if (!file_format.ok()) {
        errno = EINVAL;
        return 0;
    }

    auto dfres = arrow::dataset::FileSystemDatasetFactory::Make(std::string(uri), file_format.ValueOrDie(), arrow::dataset::FileSystemFactoryOptions());
    set_errno(dfres);
    if (!dfres.ok()) {        
        return 0;
    }
    return create_native_ref(dfres.ValueOrDie());
}

void release_dataset_factory(DatasetFactory factory) {
    release_native_ref<arrow::dataset::DatasetFactory>(factory);
}

struct ArrowSchema inspect_schema(DatasetFactory factory, const int num_fragments) {
    auto df = retrieve_native_instance<arrow::dataset::DatasetFactory>(factory);
    arrow::dataset::InspectOptions opts;
    opts.fragments = num_fragments;
    auto schema = df->Inspect(opts);
    set_errno(schema);

    struct ArrowSchema out;
    if (!schema.ok()) {
        out.release = nullptr;    
        return out;
    }
    
    set_errno(arrow::ExportSchema(*(schema.ValueOrDie()), &out));
    return out;
}

Dataset create_dataset(DatasetFactory factory) {
    auto df = retrieve_native_instance<arrow::dataset::DatasetFactory>(factory);
    auto dsr = df->Finish();
    set_errno(dsr);
    if (!dsr.ok()) {                
        return 0;
    }
    
    return create_native_ref(dsr.ValueOrDie());
}

void close_dataset(Dataset dataset_id) {
    release_native_ref<arrow::dataset::Dataset>(dataset_id);
}

struct ArrowSchema get_dataset_schema(Dataset dataset_id) {
    auto ds = retrieve_native_instance<arrow::dataset::Dataset>(dataset_id);
    auto sc = ds->schema();
    struct ArrowSchema out;
    
    set_errno(arrow::ExportSchema(*sc, &out));
    return out;
}

const char* dataset_type_name(Dataset dataset_id) {
    auto ds = retrieve_native_instance<arrow::dataset::Dataset>(dataset_id);
    return ds->type_name().c_str();
}
