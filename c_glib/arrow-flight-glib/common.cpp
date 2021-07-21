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

#include <arrow-glib/arrow-glib.hpp>

#include <arrow-flight-glib/common.hpp>

G_BEGIN_DECLS

/**
 * SECTION: common
 * @section_id: common
 * @title: Classes both for client and server
 * @include: arrow-flight-glib/arrow-flight-glib.h
 *
 * #GAFlightCriteria is a class for criteria.
 *
 * #GAFlightLocation is a class for location.
 *
 * #GAFlightDescriptor is a base class for all descriptor classes such
 * as #GAFlightPathDescriptor.
 *
 * #GAFlightPathDescriptor is a class for path descriptor.
 *
 * #GAFlightCommandDescriptor is a class for command descriptor.
 *
 * #GAFlightTicket is a class for ticket.
 *
 * #GAFlightEndpoint is a class for endpoint.
 *
 * #GAFlightInfo is a class for flight information.
 *
 * #GAFlightStreamChunk is a class for a chunk in stream.
 *
 * #GAFlightRecordBatchReader is a class for reading record batches.
 *
 * Since: 5.0.0
 */

typedef struct GAFlightCriteriaPrivate_ {
  arrow::flight::Criteria criteria;
  GBytes *expression;
} GAFlightCriteriaPrivate;

enum {
  PROP_EXPRESSION = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightCriteria,
                           gaflight_criteria,
                           G_TYPE_OBJECT)

#define GAFLIGHT_CRITERIA_GET_PRIVATE(obj)            \
  static_cast<GAFlightCriteriaPrivate *>(             \
    gaflight_criteria_get_instance_private(           \
      GAFLIGHT_CRITERIA(obj)))

static void
gaflight_criteria_dispose(GObject *object)
{
  auto priv = GAFLIGHT_CRITERIA_GET_PRIVATE(object);

  if (priv->expression) {
    g_bytes_unref(priv->expression);
    priv->expression = NULL;
  }

  G_OBJECT_CLASS(gaflight_criteria_parent_class)->dispose(object);
}

static void
gaflight_criteria_finalize(GObject *object)
{
  auto priv = GAFLIGHT_CRITERIA_GET_PRIVATE(object);

  priv->criteria.~Criteria();

  G_OBJECT_CLASS(gaflight_criteria_parent_class)->finalize(object);
}

static void
gaflight_criteria_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GAFLIGHT_CRITERIA_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_EXPRESSION:
    if (priv->expression) {
      g_bytes_unref(priv->expression);
    }
    priv->expression = static_cast<GBytes *>(g_value_dup_boxed(value));
    {
      gsize size;
      auto data = g_bytes_get_data(priv->expression, &size);
      priv->criteria.expression.assign(static_cast<const char *>(data),
                                       size);
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_criteria_get_property(GObject *object,
                               guint prop_id,
                               GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GAFLIGHT_CRITERIA_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_EXPRESSION:
    g_value_set_boxed(value, priv->expression);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_criteria_init(GAFlightCriteria *object)
{
  auto priv = GAFLIGHT_CRITERIA_GET_PRIVATE(object);
  new(&priv->criteria) arrow::flight::Criteria;
}

static void
gaflight_criteria_class_init(GAFlightCriteriaClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gaflight_criteria_dispose;
  gobject_class->finalize = gaflight_criteria_finalize;
  gobject_class->set_property = gaflight_criteria_set_property;
  gobject_class->get_property = gaflight_criteria_get_property;

  GParamSpec *spec;
  /**
   * GAFlightCriteria:expression:
   *
   * Opaque criteria expression, dependent on server implementation.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_boxed("expression",
                            "Expression",
                            "Opaque criteria expression, "
                            "dependent on server implementation",
                            G_TYPE_BYTES,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_EXPRESSION, spec);
}

/**
 * gaflight_criteria_new:
 * @expression: A #GBytes.
 *
 * Returns: The newly created #GAFlightCriteria, %NULL on error.
 *
 * Since: 5.0.0
 */
GAFlightCriteria *
gaflight_criteria_new(GBytes *expression)
{
  return GAFLIGHT_CRITERIA(
    g_object_new(GAFLIGHT_TYPE_CRITERIA,
                 "expression", expression,
                 NULL));
}


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


typedef struct GAFlightDescriptorPrivate_ {
  arrow::flight::FlightDescriptor descriptor;
} GAFlightDescriptorPrivate;

enum {
  PROP_DESCRIPTOR = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GAFlightDescriptor,
                                    gaflight_descriptor,
                                    G_TYPE_OBJECT)

#define GAFLIGHT_DESCRIPTOR_GET_PRIVATE(obj)            \
  static_cast<GAFlightDescriptorPrivate *>(             \
    gaflight_descriptor_get_instance_private(           \
      GAFLIGHT_DESCRIPTOR(obj)))

static void
gaflight_descriptor_finalize(GObject *object)
{
  auto priv = GAFLIGHT_DESCRIPTOR_GET_PRIVATE(object);

  priv->descriptor.~FlightDescriptor();

  G_OBJECT_CLASS(gaflight_descriptor_parent_class)->finalize(object);
}

static void
gaflight_descriptor_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GAFLIGHT_DESCRIPTOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DESCRIPTOR:
    priv->descriptor = *static_cast<arrow::flight::FlightDescriptor *>(
      g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_descriptor_init(GAFlightDescriptor *object)
{
  auto priv = GAFLIGHT_DESCRIPTOR_GET_PRIVATE(object);
  new(&priv->descriptor) arrow::flight::FlightDescriptor;
}

static void
gaflight_descriptor_class_init(GAFlightDescriptorClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_descriptor_finalize;
  gobject_class->set_property = gaflight_descriptor_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("descriptor",
                              "Descriptor",
                              "The raw arrow::flight::FlightDescriptor",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_EXPRESSION, spec);
}

/**
 * gaflight_descriptor_to_string:
 * @descriptor: A #GAFlightDescriptor.
 *
 * Returns: A descriptor as a string.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 5.0.0
 */
gchar *
gaflight_descriptor_to_string(GAFlightDescriptor *descriptor)
{
  auto flight_descriptor = gaflight_descriptor_get_raw(descriptor);
  return g_strdup(flight_descriptor->ToString().c_str());
}

/**
 * gaflight_descriptor_equal:
 * @descriptor: A #GAFlightDescriptor.
 * @other_descriptor: A #GAFlightDescriptor to be compared.
 *
 * Returns: %TRUE if both of them represents the same descriptor,
 *   %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
gaflight_descriptor_equal(GAFlightDescriptor *descriptor,
                          GAFlightDescriptor *other_descriptor)
{
  const auto flight_descriptor =
    gaflight_descriptor_get_raw(descriptor);
  const auto flight_other_descriptor =
    gaflight_descriptor_get_raw(other_descriptor);
  return flight_descriptor->Equals(*flight_other_descriptor);
}


G_DEFINE_TYPE(GAFlightPathDescriptor,
              gaflight_path_descriptor,
              GAFLIGHT_TYPE_DESCRIPTOR)

static void
gaflight_path_descriptor_init(GAFlightPathDescriptor *object)
{
}

static void
gaflight_path_descriptor_class_init(GAFlightPathDescriptorClass *klass)
{
}

/**
 * gaflight_path_descriptor_new:
 * @paths: (array length=n_paths): List of paths identifying a
 *   particular dataset.
 * @n_paths: The number of @paths.
 *
 * Returns: The newly created #GAFlightPathDescriptor.
 *
 * Since: 5.0.0
 */
GAFlightPathDescriptor *
gaflight_path_descriptor_new(const gchar **paths,
                             gsize n_paths)
{
  std::vector<std::string> flight_paths;
  for (gsize i = 0; i < n_paths; i++) {
    flight_paths.push_back(paths[i]);
  }
  auto flight_descriptor = arrow::flight::FlightDescriptor::Path(flight_paths);
  return GAFLIGHT_PATH_DESCRIPTOR(
    gaflight_descriptor_new_raw(&flight_descriptor));
}

/**
 * gaflight_path_descriptor_get_paths:
 * @descriptor: A #GAFlightPathDescriptor.
 *
 * Returns: (nullable) (array zero-terminated=1) (transfer full):
 *   The paths in this descriptor.
 *
 *   It must be freed with g_strfreev() when no longer needed.
 *
 * Since: 5.0.0
 */
gchar **
gaflight_path_descriptor_get_paths(GAFlightPathDescriptor *descriptor)
{
  const auto flight_descriptor =
    gaflight_descriptor_get_raw(GAFLIGHT_DESCRIPTOR(descriptor));
  const auto &flight_paths = flight_descriptor->path;
  if (flight_paths.empty()) {
    return NULL;
  } else {
    auto paths = g_new(gchar *, flight_paths.size() + 1);
    gsize i = 0;
    for (const auto &flight_path : flight_paths) {
      paths[i++] = g_strdup(flight_path.c_str());
    }
    paths[i] = NULL;
    return paths;
  }
}


G_DEFINE_TYPE(GAFlightCommandDescriptor,
              gaflight_command_descriptor,
              GAFLIGHT_TYPE_DESCRIPTOR)

static void
gaflight_command_descriptor_init(GAFlightCommandDescriptor *object)
{
}

static void
gaflight_command_descriptor_class_init(GAFlightCommandDescriptorClass *klass)
{
}

/**
 * gaflight_command_descriptor_new:
 * @command: Opaque value used to express a command.
 *
 * Returns: The newly created #GAFlightCommandDescriptor.
 *
 * Since: 5.0.0
 */
GAFlightCommandDescriptor *
gaflight_command_descriptor_new(const gchar *command)
{
  auto flight_descriptor = arrow::flight::FlightDescriptor::Command(command);
  return GAFLIGHT_COMMAND_DESCRIPTOR(
    gaflight_descriptor_new_raw(&flight_descriptor));
}

/**
 * gaflight_command_descriptor_get_command:
 * @descriptor: A #GAFlightCommandDescriptor.
 *
 * Returns: The opaque value used to express a command.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 5.0.0
 */
gchar *
gaflight_command_descriptor_get_command(GAFlightCommandDescriptor *descriptor)
{
  const auto flight_descriptor =
    gaflight_descriptor_get_raw(GAFLIGHT_DESCRIPTOR(descriptor));
  const auto &flight_command = flight_descriptor->cmd;
  return g_strdup(flight_command.c_str());
}


typedef struct GAFlightTicketPrivate_ {
  arrow::flight::Ticket ticket;
  GBytes *data;
} GAFlightTicketPrivate;

enum {
  PROP_DATA = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightTicket,
                           gaflight_ticket,
                           G_TYPE_OBJECT)

#define GAFLIGHT_TICKET_GET_PRIVATE(obj)            \
  static_cast<GAFlightTicketPrivate *>(             \
    gaflight_ticket_get_instance_private(           \
      GAFLIGHT_TICKET(obj)))

static void
gaflight_ticket_dispose(GObject *object)
{
  auto priv = GAFLIGHT_TICKET_GET_PRIVATE(object);

  if (priv->data) {
    g_bytes_unref(priv->data);
    priv->data = NULL;
  }

  G_OBJECT_CLASS(gaflight_ticket_parent_class)->dispose(object);
}

static void
gaflight_ticket_finalize(GObject *object)
{
  auto priv = GAFLIGHT_TICKET_GET_PRIVATE(object);

  priv->ticket.~Ticket();

  G_OBJECT_CLASS(gaflight_ticket_parent_class)->finalize(object);
}

static void
gaflight_ticket_set_property(GObject *object,
                             guint prop_id,
                             const GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GAFLIGHT_TICKET_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATA:
    if (priv->data) {
      g_bytes_unref(priv->data);
    }
    priv->data = static_cast<GBytes *>(g_value_dup_boxed(value));
    {
      gsize size;
      auto data = g_bytes_get_data(priv->data, &size);
      priv->ticket.ticket.assign(static_cast<const char *>(data),
                                 size);
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_ticket_get_property(GObject *object,
                             guint prop_id,
                             GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GAFLIGHT_TICKET_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATA:
    g_value_set_boxed(value, priv->data);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_ticket_init(GAFlightTicket *object)
{
  auto priv = GAFLIGHT_TICKET_GET_PRIVATE(object);
  new(&priv->ticket) arrow::flight::Ticket;
}

static void
gaflight_ticket_class_init(GAFlightTicketClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gaflight_ticket_dispose;
  gobject_class->finalize = gaflight_ticket_finalize;
  gobject_class->set_property = gaflight_ticket_set_property;
  gobject_class->get_property = gaflight_ticket_get_property;

  GParamSpec *spec;
  /**
   * GAFlightTicket:data:
   *
   * Opaque identifier or credential to use when requesting a data
   * stream with the DoGet RPC.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_boxed("data",
                            "Data",
                            "Opaque identifier or credential to use "
                            "when requesting a data stream with the DoGet RPC",
                            G_TYPE_BYTES,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_DATA, spec);
}

/**
 * gaflight_ticket_new:
 * @data: A #GBytes.
 *
 * Returns: The newly created #GAFlightTicket, %NULL on error.
 *
 * Since: 5.0.0
 */
GAFlightTicket *
gaflight_ticket_new(GBytes *data)
{
  return GAFLIGHT_TICKET(
    g_object_new(GAFLIGHT_TYPE_TICKET,
                 "data", data,
                 NULL));
}

/**
 * gaflight_ticket_equal:
 * @ticket: A #GAFlightTicket.
 * @other_ticket: A #GAFlightTicket to be compared.
 *
 * Returns: %TRUE if both of them represents the same ticket, %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
gaflight_ticket_equal(GAFlightTicket *ticket,
                      GAFlightTicket *other_ticket)
{
  const auto flight_ticket = gaflight_ticket_get_raw(ticket);
  const auto flight_other_ticket = gaflight_ticket_get_raw(other_ticket);
  return flight_ticket->Equals(*flight_other_ticket);
}


typedef struct GAFlightEndpointPrivate_ {
  arrow::flight::FlightEndpoint endpoint;
  GAFlightTicket *ticket;
  GList *locations;
} GAFlightEndpointPrivate;

enum {
  PROP_TICKET = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightEndpoint,
                           gaflight_endpoint,
                           G_TYPE_OBJECT)

#define GAFLIGHT_ENDPOINT_GET_PRIVATE(obj)            \
  static_cast<GAFlightEndpointPrivate *>(             \
    gaflight_endpoint_get_instance_private(           \
      GAFLIGHT_ENDPOINT(obj)))

static void
gaflight_endpoint_dispose(GObject *object)
{
  auto priv = GAFLIGHT_ENDPOINT_GET_PRIVATE(object);

  if (priv->ticket) {
    g_object_unref(priv->ticket);
    priv->ticket = NULL;
  }

  if (priv->locations) {
    g_list_free_full(priv->locations, g_object_unref);
    priv->locations = NULL;
  }

  G_OBJECT_CLASS(gaflight_endpoint_parent_class)->dispose(object);
}

static void
gaflight_endpoint_finalize(GObject *object)
{
  auto priv = GAFLIGHT_ENDPOINT_GET_PRIVATE(object);

  priv->endpoint.~FlightEndpoint();

  G_OBJECT_CLASS(gaflight_endpoint_parent_class)->finalize(object);
}

static void
gaflight_endpoint_get_property(GObject *object,
                               guint prop_id,
                               GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GAFLIGHT_ENDPOINT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TICKET:
    g_value_set_object(value, priv->ticket);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_endpoint_init(GAFlightEndpoint *object)
{
  auto priv = GAFLIGHT_ENDPOINT_GET_PRIVATE(object);
  new(&priv->endpoint) arrow::flight::FlightEndpoint;
}

static void
gaflight_endpoint_class_init(GAFlightEndpointClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gaflight_endpoint_dispose;
  gobject_class->finalize = gaflight_endpoint_finalize;
  gobject_class->get_property = gaflight_endpoint_get_property;

  GParamSpec *spec;
  /**
   * GAFlightEndpoint:ticket:
   *
   * Opaque ticket identify; use with DoGet RPC.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("ticket",
                             "Ticket",
                             "Opaque ticket identify; use with DoGet RPC",
                             GAFLIGHT_TYPE_TICKET,
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class, PROP_TICKET, spec);
}

/**
 * gaflight_endpoint_new:
 * @ticket: A #GAFlightTicket.
 * @locations: (element-type GAFlightLocation): A list of #GAFlightLocation.
 *
 * Returns: The newly created #GAFlightEndpoint, %NULL on error.
 *
 * Since: 5.0.0
 */
GAFlightEndpoint *
gaflight_endpoint_new(GAFlightTicket *ticket,
                      GList *locations)
{
  auto endpoint = gaflight_endpoint_new_raw(nullptr, ticket);
  auto priv = GAFLIGHT_ENDPOINT_GET_PRIVATE(endpoint);
  for (auto node = locations; node; node = node->next) {
    auto location = GAFLIGHT_LOCATION(node->data);
    priv->endpoint.locations.push_back(*gaflight_location_get_raw(location));
  }
  return endpoint;
}

/**
 * gaflight_endpoint_equal:
 * @endpoint: A #GAFlightEndpoint.
 * @other_endpoint: A #GAFlightEndpoint to be compared.
 *
 * Returns: %TRUE if both of them represents the same endpoint,
 *   %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
gaflight_endpoint_equal(GAFlightEndpoint *endpoint,
                        GAFlightEndpoint *other_endpoint)
{
  const auto flight_endpoint = gaflight_endpoint_get_raw(endpoint);
  const auto flight_other_endpoint = gaflight_endpoint_get_raw(other_endpoint);
  return flight_endpoint->Equals(*flight_other_endpoint);
}

/**
 * gaflight_endpoint_get_locations:
 * @endpoint: A #GAFlightEndpoint.
 *
 * Returns: (nullable) (element-type GAFlightLocation) (transfer full):
 *   The locations in this endpoint.
 *
 *   It must be freed with g_list_free() and g_object_unref() when no
 *   longer needed. You can use `g_list_free_full(locations,
 *   g_object_unref)`.
 *
 * Since: 5.0.0
 */
GList *
gaflight_endpoint_get_locations(GAFlightEndpoint *endpoint)
{
  const auto flight_endpoint = gaflight_endpoint_get_raw(endpoint);
  GList *locations = NULL;
  for (const auto &flight_location : flight_endpoint->locations) {
    auto location = gaflight_location_new(flight_location.ToString().c_str(),
                                          nullptr);
    locations = g_list_prepend(locations, location);
  }
  return g_list_reverse(locations);
}


typedef struct GAFlightInfoPrivate_ {
  arrow::flight::FlightInfo info;
} GAFlightInfoPrivate;

enum {
  PROP_INFO = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightInfo,
                           gaflight_info,
                           G_TYPE_OBJECT)

#define GAFLIGHT_INFO_GET_PRIVATE(obj)            \
  static_cast<GAFlightInfoPrivate *>(             \
    gaflight_info_get_instance_private(           \
      GAFLIGHT_INFO(obj)))

static void
gaflight_info_finalize(GObject *object)
{
  auto priv = GAFLIGHT_INFO_GET_PRIVATE(object);

  priv->info.~FlightInfo();

  G_OBJECT_CLASS(gaflight_info_parent_class)->finalize(object);
}

static void
gaflight_info_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GAFLIGHT_INFO_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INFO:
    {
      auto info =
        static_cast<arrow::flight::FlightInfo *>(g_value_get_pointer(value));
      new(&(priv->info)) arrow::flight::FlightInfo(*info);
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_info_init(GAFlightInfo *object)
{
}

static void
gaflight_info_class_init(GAFlightInfoClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_info_finalize;
  gobject_class->set_property = gaflight_info_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("info",
                              "Info",
                              "The raw arrow::flight::FlightInfo *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_INFO, spec);
}

/**
 * gaflight_info_new:
 * @schema: A #GArrowSchema.
 * @descriptor: A #GAFlightDescriptor.
 * @endpoints: (element-type GAFlightEndpoint): A list of #GAFlightEndpoint.
 * @total_records: The number of total records.
 * @total_bytes: The number of total bytes.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): The newly created #GAFlightInfo, %NULL on error.
 *
 * Since: 5.0.0
 */
GAFlightInfo *
gaflight_info_new(GArrowSchema *schema,
                  GAFlightDescriptor *descriptor,
                  GList *endpoints,
                  gint64 total_records,
                  gint64 total_bytes,
                  GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto flight_descriptor = gaflight_descriptor_get_raw(descriptor);
  std::vector<arrow::flight::FlightEndpoint> flight_endpoints;
  for (auto node = endpoints; node; node = node->next) {
    auto endpoint = GAFLIGHT_ENDPOINT(node->data);
    flight_endpoints.push_back(*gaflight_endpoint_get_raw(endpoint));
  }
  auto flight_info_result =
    arrow::flight::FlightInfo::Make(*arrow_schema,
                                    *flight_descriptor,
                                    flight_endpoints,
                                    total_records,
                                    total_bytes);
  if (!garrow::check(error,
                     flight_info_result,
                     "[flight-info][new]")) {
    return NULL;
  }
  return gaflight_info_new_raw(&(*flight_info_result));
}

/**
 * gaflight_info_equal:
 * @info: A #GAFlightInfo.
 * @other_info: A #GAFlightInfo to be compared.
 *
 * Returns: %TRUE if both of them represents the same information,
 *   %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
gaflight_info_equal(GAFlightInfo *info,
                    GAFlightInfo *other_info)
{
  const auto flight_info = gaflight_info_get_raw(info);
  const auto flight_other_info = gaflight_info_get_raw(other_info);
  return
    (flight_info->serialized_schema() ==
     flight_other_info->serialized_schema()) &&
    (flight_info->descriptor() ==
     flight_other_info->descriptor()) &&
    (flight_info->endpoints() ==
     flight_other_info->endpoints()) &&
    (flight_info->total_records() ==
     flight_other_info->total_records()) &&
    (flight_info->total_bytes() ==
     flight_other_info->total_bytes());
}

/**
 * gaflight_info_get_schema:
 * @info: A #GAFlightInfo.
 * @options: (nullable): A #GArrowReadOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): Deserialized #GArrowSchema, %NULL on error.
 *
 * Since: 5.0.0
 */
GArrowSchema *
gaflight_info_get_schema(GAFlightInfo *info,
                         GArrowReadOptions *options,
                         GError **error)
{
  const auto flight_info = gaflight_info_get_raw(info);
  arrow::Status status;
  std::shared_ptr<arrow::Schema> arrow_schema;
  if (options) {
    auto arrow_memo = garrow_read_options_get_dictionary_memo_raw(options);
    status = flight_info->GetSchema(arrow_memo, &arrow_schema);
  } else {
    arrow::ipc::DictionaryMemo arrow_memo;
    status = flight_info->GetSchema(&arrow_memo, &arrow_schema);
  }
  if (garrow::check(error, status, "[flight-info][get-schema]")) {
    return garrow_schema_new_raw(&arrow_schema);
  } else {
    return NULL;
  }
}

/**
 * gaflight_info_get_descriptor:
 * @info: A #GAFlightInfo.
 *
 * Returns: (transfer full): The #GAFlightDescriptor of the information.
 *
 * Since: 5.0.0
 */
GAFlightDescriptor *
gaflight_info_get_descriptor(GAFlightInfo *info)
{
  const auto flight_info = gaflight_info_get_raw(info);
  return gaflight_descriptor_new_raw(&(flight_info->descriptor()));
}

/**
 * gaflight_info_get_endpoints:
 * @info: A #GAFlightInfo.
 *
 * Returns: (element-type GAFlightEndpoint) (transfer full):
 *   The list of #GAFlightEndpoint of the information.
 *
 * Since: 5.0.0
 */
GList *
gaflight_info_get_endpoints(GAFlightInfo *info)
{
  const auto flight_info = gaflight_info_get_raw(info);
  GList *endpoints = NULL;
  for (const auto &flight_endpoint : flight_info->endpoints()) {
    auto endpoint = gaflight_endpoint_new_raw(&flight_endpoint, nullptr);
    endpoints = g_list_prepend(endpoints, endpoint);
  }
  return g_list_reverse(endpoints);
}

/**
 * gaflight_info_get_total_records:
 * @info: A #GAFlightInfo.
 *
 * Returns: The number of total records of the information.
 *
 * Since: 5.0.0
 */
gint64
gaflight_info_get_total_records(GAFlightInfo *info)
{
  const auto flight_info = gaflight_info_get_raw(info);
  return flight_info->total_records();
}

/**
 * gaflight_info_get_total_bytes:
 * @info: A #GAFlightInfo.
 *
 * Returns: The number of total bytes of the information.
 *
 * Since: 5.0.0
 */
gint64
gaflight_info_get_total_bytes(GAFlightInfo *info)
{
  const auto flight_info = gaflight_info_get_raw(info);
  return flight_info->total_bytes();
}

typedef struct GAFlightStreamChunkPrivate_ {
  arrow::flight::FlightStreamChunk chunk;
} GAFlightStreamChunkPrivate;

enum {
  PROP_CHUNK = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightStreamChunk,
                           gaflight_stream_chunk,
                           G_TYPE_OBJECT)

#define GAFLIGHT_STREAM_CHUNK_GET_PRIVATE(obj)            \
  static_cast<GAFlightStreamChunkPrivate *>(             \
    gaflight_stream_chunk_get_instance_private(           \
      GAFLIGHT_STREAM_CHUNK(obj)))

static void
gaflight_stream_chunk_finalize(GObject *object)
{
  auto priv = GAFLIGHT_STREAM_CHUNK_GET_PRIVATE(object);

  priv->chunk.~FlightStreamChunk();

  G_OBJECT_CLASS(gaflight_info_parent_class)->finalize(object);
}

static void
gaflight_stream_chunk_set_property(GObject *object,
                                   guint prop_id,
                                   const GValue *value,
                                   GParamSpec *pspec)
{
  auto priv = GAFLIGHT_STREAM_CHUNK_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CHUNK:
    priv->chunk =
      *static_cast<arrow::flight::FlightStreamChunk *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_stream_chunk_init(GAFlightStreamChunk *object)
{
}

static void
gaflight_stream_chunk_class_init(GAFlightStreamChunkClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_stream_chunk_finalize;
  gobject_class->set_property = gaflight_stream_chunk_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("chunk",
                              "Stream chunk",
                              "The raw arrow::flight::FlightStreamChunk *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CHUNK, spec);
}

/**
 * gaflight_stream_chunk_get_data:
 * @chunk: A #GAFlightStreamChunk.
 *
 * Returns: (transfer full): The data of the chunk.
 *
 * Since: 6.0.0
 */
GArrowRecordBatch *
gaflight_stream_chunk_get_data(GAFlightStreamChunk *chunk)
{
  auto flight_chunk = gaflight_stream_chunk_get_raw(chunk);
  return garrow_record_batch_new_raw(&(flight_chunk->data));
}

/**
 * gaflight_stream_chunk_get_metadata:
 * @chunk: A #GAFlightStreamChunk.
 *
 * Returns: (nullable) (transfer full): The metadata of the chunk.
 *
 *   The metadata may be NULL.
 *
 * Since: 6.0.0
 */
GArrowBuffer *
gaflight_stream_chunk_get_metadata(GAFlightStreamChunk *chunk)
{
  auto flight_chunk = gaflight_stream_chunk_get_raw(chunk);
  if (flight_chunk->app_metadata) {
    return garrow_buffer_new_raw(&(flight_chunk->app_metadata));
  } else {
    return NULL;
  }
}


typedef struct GAFlightRecordBatchReaderPrivate_ {
  arrow::flight::MetadataRecordBatchReader *reader;
} GAFlightRecordBatchReaderPrivate;

enum {
  PROP_READER = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightRecordBatchReader,
                           gaflight_record_batch_reader,
                           G_TYPE_OBJECT)

#define GAFLIGHT_RECORD_BATCH_READER_GET_PRIVATE(obj)            \
  static_cast<GAFlightRecordBatchReaderPrivate *>(               \
    gaflight_record_batch_reader_get_instance_private(           \
      GAFLIGHT_RECORD_BATCH_READER(obj)))

static void
gaflight_record_batch_reader_finalize(GObject *object)
{
  auto priv = GAFLIGHT_RECORD_BATCH_READER_GET_PRIVATE(object);

  delete priv->reader;

  G_OBJECT_CLASS(gaflight_info_parent_class)->finalize(object);
}

static void
gaflight_record_batch_reader_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GAFLIGHT_RECORD_BATCH_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_READER:
    priv->reader =
      static_cast<arrow::flight::MetadataRecordBatchReader *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_record_batch_reader_init(GAFlightRecordBatchReader *object)
{
}

static void
gaflight_record_batch_reader_class_init(GAFlightRecordBatchReaderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_record_batch_reader_finalize;
  gobject_class->set_property = gaflight_record_batch_reader_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("reader",
                              "Reader",
                              "The raw arrow::flight::MetadataRecordBatchReader *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_READER, spec);
}

/**
 * gaflight_record_batch_reader_read_next:
 * @reader: A #GAFlightRecordBatchReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The next chunk on success, %NULL on end
 *   of stream, %NULL on error.
 *
 * Since: 6.0.0
 */
GAFlightStreamChunk *
gaflight_record_batch_reader_read_next(GAFlightRecordBatchReader *reader,
                                       GError **error)
{
  auto flight_reader = gaflight_record_batch_reader_get_raw(reader);
  arrow::flight::FlightStreamChunk flight_chunk;
  auto status = flight_reader->Next(&flight_chunk);
  if (garrow::check(error, status, "[flight-record-batch-reader][read-next]")) {
    if (flight_chunk.data) {
      return gaflight_stream_chunk_new_raw(&flight_chunk);
    } else {
      return NULL;
    }
  } else {
    return NULL;
  }
}

/**
 * gaflight_record_batch_reader_read_all:
 * @reader: A #GAFlightRecordBatchReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The all data on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GArrowTable *
gaflight_record_batch_reader_read_all(GAFlightRecordBatchReader *reader,
                                      GError **error)
{
  auto flight_reader = gaflight_record_batch_reader_get_raw(reader);
  std::shared_ptr<arrow::Table> arrow_table;
  auto status = flight_reader->ReadAll(&arrow_table);
  if (garrow::check(error, status, "[flight-record-batch-reader][read-all]")) {
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
}


G_END_DECLS


GAFlightCriteria *
gaflight_criteria_new_raw(const arrow::flight::Criteria *flight_criteria)
{
  auto criteria = g_object_new(GAFLIGHT_TYPE_CRITERIA, NULL);
  auto priv = GAFLIGHT_CRITERIA_GET_PRIVATE(criteria);
  priv->criteria = *flight_criteria;
  priv->expression = g_bytes_new(priv->criteria.expression.data(),
                                 priv->criteria.expression.size());
  return GAFLIGHT_CRITERIA(criteria);
}

arrow::flight::Criteria *
gaflight_criteria_get_raw(GAFlightCriteria *criteria)
{
  auto priv = GAFLIGHT_CRITERIA_GET_PRIVATE(criteria);
  return &(priv->criteria);
}

arrow::flight::Location *
gaflight_location_get_raw(GAFlightLocation *location)
{
  auto priv = GAFLIGHT_LOCATION_GET_PRIVATE(location);
  return &(priv->location);
}

GAFlightDescriptor *
gaflight_descriptor_new_raw(
  const arrow::flight::FlightDescriptor *flight_descriptor)
{
  GType gtype = GAFLIGHT_TYPE_DESCRIPTOR;
  switch (flight_descriptor->type) {
  case arrow::flight::FlightDescriptor::DescriptorType::PATH:
    gtype = GAFLIGHT_TYPE_PATH_DESCRIPTOR;
    break;
  case arrow::flight::FlightDescriptor::DescriptorType::CMD:
    gtype = GAFLIGHT_TYPE_COMMAND_DESCRIPTOR;
    break;
  default:
    break;
  }
  return GAFLIGHT_DESCRIPTOR(g_object_new(gtype,
                                          "descriptor", flight_descriptor,
                                          NULL));
}

arrow::flight::FlightDescriptor *
gaflight_descriptor_get_raw(GAFlightDescriptor *descriptor)
{
  auto priv = GAFLIGHT_DESCRIPTOR_GET_PRIVATE(descriptor);
  return &(priv->descriptor);
}

GAFlightTicket *
gaflight_ticket_new_raw(const arrow::flight::Ticket *flight_ticket)
{
  auto ticket = g_object_new(GAFLIGHT_TYPE_TICKET, NULL);
  auto priv = GAFLIGHT_TICKET_GET_PRIVATE(ticket);
  priv->ticket = *flight_ticket;
  priv->data = g_bytes_new(priv->ticket.ticket.data(),
                           priv->ticket.ticket.size());
  return GAFLIGHT_TICKET(ticket);
}

arrow::flight::Ticket *
gaflight_ticket_get_raw(GAFlightTicket *ticket)
{
  auto priv = GAFLIGHT_TICKET_GET_PRIVATE(ticket);
  return &(priv->ticket);
}

GAFlightEndpoint *
gaflight_endpoint_new_raw(const arrow::flight::FlightEndpoint *flight_endpoint,
                          GAFlightTicket *ticket)
{
  auto endpoint = GAFLIGHT_ENDPOINT(g_object_new(GAFLIGHT_TYPE_ENDPOINT,
                                                 NULL));
  auto priv = GAFLIGHT_ENDPOINT_GET_PRIVATE(endpoint);
  if (ticket) {
    priv->ticket = ticket;
    g_object_ref(priv->ticket);
    priv->endpoint.ticket = *gaflight_ticket_get_raw(priv->ticket);
  } else {
    auto data = g_bytes_new(flight_endpoint->ticket.ticket.data(),
                            flight_endpoint->ticket.ticket.length());
    auto ticket = gaflight_ticket_new(data);
    g_bytes_unref(data);
    priv->ticket = ticket;
    priv->endpoint.ticket.ticket = flight_endpoint->ticket.ticket;
  }
  if (flight_endpoint) {
    priv->endpoint.locations = flight_endpoint->locations;
  }
  return endpoint;
}

arrow::flight::FlightEndpoint *
gaflight_endpoint_get_raw(GAFlightEndpoint *endpoint)
{
  auto priv = GAFLIGHT_ENDPOINT_GET_PRIVATE(endpoint);
  return &(priv->endpoint);
}

GAFlightInfo *
gaflight_info_new_raw(arrow::flight::FlightInfo *flight_info)
{
  return GAFLIGHT_INFO(g_object_new(GAFLIGHT_TYPE_INFO,
                                    "info", flight_info,
                                    NULL));
}

arrow::flight::FlightInfo *
gaflight_info_get_raw(GAFlightInfo *info)
{
  auto priv = GAFLIGHT_INFO_GET_PRIVATE(info);
  return &(priv->info);
}

GAFlightStreamChunk *
gaflight_stream_chunk_new_raw(arrow::flight::FlightStreamChunk *flight_chunk)
{
  return GAFLIGHT_STREAM_CHUNK(
    g_object_new(GAFLIGHT_TYPE_STREAM_CHUNK,
                 "chunk", flight_chunk,
                 NULL));
}

arrow::flight::FlightStreamChunk *
gaflight_stream_chunk_get_raw(GAFlightStreamChunk *chunk)
{
  auto priv = GAFLIGHT_STREAM_CHUNK_GET_PRIVATE(chunk);
  return &(priv->chunk);
}

arrow::flight::MetadataRecordBatchReader *
gaflight_record_batch_reader_get_raw(GAFlightRecordBatchReader *reader)
{
  auto priv = GAFLIGHT_RECORD_BATCH_READER_GET_PRIVATE(reader);
  return priv->reader;
}
