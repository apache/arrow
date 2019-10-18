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

#pragma once

#include <plasma/client.h>

#include <plasma-glib/client.h>

#include <plasma-glib/object.hpp>

plasma::ObjectID
gplasma_object_id_get_raw(GPlasmaObjectID *id);

GPlasmaCreatedObject *
gplasma_created_object_new_raw(GPlasmaClient *client,
                               GPlasmaObjectID *id,
                               std::shared_ptr<arrow::Buffer> *plasma_data,
                               GArrowBuffer *data,
                               std::shared_ptr<arrow::Buffer> *plasma_metadata,
                               GArrowBuffer *metadata,
                               gint gpu_device);

GPlasmaReferredObject *
gplasma_referred_object_new_raw(GPlasmaClient *client,
                                GPlasmaObjectID *id,
                                std::shared_ptr<arrow::Buffer> *plasma_data,
                                GArrowBuffer *data,
                                std::shared_ptr<arrow::Buffer> *plasma_metadata,
                                GArrowBuffer *metadata,
                                gint gpu_device);
