// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build cgo

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include "arrow/c/abi.h"
#include "utils.h"

static const int64_t kDefaultFlags = ARROW_FLAG_NULLABLE;

static void release_int32_type(struct ArrowSchema* schema) {
    // mark released
    schema->release = NULL;
}

void export_int32_type(struct ArrowSchema* schema) {
    const char* encoded_metadata;
    if (check_for_endianness() == 1) {
        encoded_metadata = kEncodedMeta1LE;
    } else {
        encoded_metadata = kEncodedMeta1BE;
    }
    *schema = (struct ArrowSchema) {
        // Type description
        .format = "i",
        .name = "",
        .metadata = encoded_metadata,
        .flags = 0,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_int32_type,
    };
}

static bool test1_released = false;

int test1_is_released() { return test1_released; }

static void release_int32_array(struct ArrowArray* array) {
    assert(array->n_buffers == 2);
    // free the buffers and buffers array
    free((void *) array->buffers[1]);
    free(array->buffers);
    // mark released
    array->release = NULL;
    test1_released = true;    
}

void export_int32_array(const int32_t* data, int64_t nitems, struct ArrowArray* array) {
    // initialize primitive fields
    *array = (struct ArrowArray) {
        .length = nitems,
        .offset = 0,
        .null_count = 0,
        .n_buffers = 2,
        .n_children = 0,
        .children = NULL,
        .dictionary = NULL,
        // bookkeeping
        .release = &release_int32_array
    };

    // allocate list of buffers
    array->buffers = (const void**)malloc(sizeof(void*) * array->n_buffers);
    assert(array->buffers != NULL);
    array->buffers[0] = NULL; // no nulls, null bitmap can be omitted
    array->buffers[1] = data;
}
