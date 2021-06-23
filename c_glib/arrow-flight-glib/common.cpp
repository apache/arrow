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

#include <arrow-glib/error.hpp>

#include <arrow-flight-glib/common.hpp>

G_BEGIN_DECLS

/**
 * SECTION: common
 * @section_id: common
 * @title: Classes both for client and server
 * @include: arrow-flight-glib/arrow-flight-glib.h
 *
 * #GAFlightLocation is a class for location.
 *
 * Since: 5.0.0
 */

typedef struct GAFlightLocationPrivate_ {
  arrow::flight::Location location;
} GAFlightLocationPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightLocation,
                           gaflight_location,
                           G_TYPE_OBJECT)

#define GAFLIGHT_LOCATION_GET_PRIVATE(obj)            \
  static_cast<GAFlightLocationPrivate *>(             \
    gaflight_location_get_instance_private(           \
      GAFLIGHT_LOCATION(obj)))

static void
gaflight_location_finalize(GObject *object)
{
  auto priv = GAFLIGHT_LOCATION_GET_PRIVATE(object);

  priv->location.~Location();

  G_OBJECT_CLASS(gaflight_location_parent_class)->finalize(object);
}

static void
gaflight_location_init(GAFlightLocation *object)
{
  auto priv = GAFLIGHT_LOCATION_GET_PRIVATE(object);
  new(&priv->location) arrow::flight::Location;
}

static void
gaflight_location_class_init(GAFlightLocationClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_location_finalize;
}

/**
 * gaflight_location_new:
 * @uri: An URI to specify location.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): The newly created location, %NULL on error.
 *
 * Since: 5.0.0
 */
GAFlightLocation *
gaflight_location_new(const gchar *uri,
                      GError **error)
{
  auto location = GAFLIGHT_LOCATION(g_object_new(GAFLIGHT_TYPE_LOCATION, NULL));
  auto flight_location = gaflight_location_get_raw(location);
  if (garrow::check(error,
                    arrow::flight::Location::Parse(uri, flight_location),
                    "[flight-location][new]")) {
    return location;
  } else {
    g_object_unref(location);
    return NULL;
  }
}

/**
 * gaflight_location_to_string:
 * @location: A #GAFlightLocation.
 *
 * Returns: A representation of this URI as a string.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 5.0.0
 */
gchar *
gaflight_location_to_string(GAFlightLocation *location)
{
  const auto flight_location = gaflight_location_get_raw(location);
  return g_strdup(flight_location->ToString().c_str());
}

/**
 * gaflight_location_get_scheme:
 * @location: A #GAFlightLocation.
 *
 * Returns: The scheme of this URI.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 5.0.0
 */
gchar *
gaflight_location_get_scheme(GAFlightLocation *location)
{
  const auto flight_location = gaflight_location_get_raw(location);
  return g_strdup(flight_location->scheme().c_str());
}

/**
 * gaflight_location_equal:
 * @location: A #GAFlightLocation.
 * @other_location: A #GAFlightLocation to be compared.
 *
 * Returns: %TRUE if both of them represents the same URI, %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
gaflight_location_equal(GAFlightLocation *location,
                        GAFlightLocation *other_location)
{
  const auto flight_location = gaflight_location_get_raw(location);
  const auto flight_other_location = gaflight_location_get_raw(other_location);
  return flight_location->Equals(*flight_other_location);
}


G_END_DECLS


arrow::flight::Location *
gaflight_location_get_raw(GAFlightLocation *location)
{
  auto priv = GAFLIGHT_LOCATION_GET_PRIVATE(location);
  return &(priv->location);
}
