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

#include <arrow-glib/arrow-glib.h>

G_BEGIN_DECLS


#define GAFLIGHT_TYPE_LOCATION (gaflight_location_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightLocation,
                         gaflight_location,
                         GAFLIGHT,
                         LOCATION,
                         GObject)
struct _GAFlightLocationClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightLocation *
gaflight_location_new(const gchar *uri,
                      GError **error);

GARROW_AVAILABLE_IN_5_0
gchar *
gaflight_location_to_string(GAFlightLocation *location);

GARROW_AVAILABLE_IN_5_0
gchar *
gaflight_location_get_scheme(GAFlightLocation *location);

GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_location_equal(GAFlightLocation *location,
                        GAFlightLocation *other_location);


G_END_DECLS
