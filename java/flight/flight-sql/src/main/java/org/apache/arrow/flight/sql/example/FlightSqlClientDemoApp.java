/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.sql.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Flight SQL Client Demo CLI Application.
 */
public class FlightSqlClientDemoApp implements AutoCloseable {
  public final List<CallOption> callOptions = new ArrayList<>();
  public final BufferAllocator allocator;
  public FlightSqlClient flightSqlClient;

  public FlightSqlClientDemoApp(final BufferAllocator bufferAllocator) {
    allocator = bufferAllocator;
  }

  public static void main(final String[] args) throws Exception {
    final Options options = new Options();

    options.addRequiredOption("host", "host", true, "Host to connect to");
    options.addRequiredOption("port", "port", true, "Port to connect to");
    options.addRequiredOption("command", "command", true, "Method to run");

    options.addOption("query", "query", true, "Query");
    options.addOption("catalog", "catalog", true, "Catalog");
    options.addOption("schema", "schema", true, "Schema");
    options.addOption("table", "table", true, "Table");

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
      try (final FlightSqlClientDemoApp thisApp = new FlightSqlClientDemoApp(new RootAllocator(Integer.MAX_VALUE))) {
        thisApp.executeApp(cmd);
      }

    } catch (final ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("FlightSqlClientDemoApp -host localhost -port 32010 ...", options);
      throw e;
    }
  }

  /**
   * Gets the current {@link CallOption} as an array; usually used as an
   * argument in {@link FlightSqlClient} methods.
   *
   * @return current {@link CallOption} array.
   */
  public CallOption[] getCallOptions() {
    return callOptions.toArray(new CallOption[0]);
  }

  /**
   * Calls {@link FlightSqlClientDemoApp#createFlightSqlClient(String, int)}
   * in order to create a {@link FlightSqlClient} to be used in future calls,
   * and then calls {@link FlightSqlClientDemoApp#executeCommand(CommandLine)}
   * to execute the command parsed at execution.
   *
   * @param cmd parsed {@link CommandLine}; often the result of {@link DefaultParser#parse(Options, String[])}.
   */
  public void executeApp(final CommandLine cmd) throws Exception {
    final String host = cmd.getOptionValue("host").trim();
    final int port = Integer.parseInt(cmd.getOptionValue("port").trim());

    createFlightSqlClient(host, port);
    executeCommand(cmd);
  }

  /**
   * Parses the "{@code command}" CLI argument and redirects to the appropriate method.
   *
   * @param cmd parsed {@link CommandLine}; often the result of
   *            {@link DefaultParser#parse(Options, String[])}.
   */
  public void executeCommand(CommandLine cmd) throws Exception {
    switch (cmd.getOptionValue("command").trim()) {
      case "Execute":
        exampleExecute(
            cmd.getOptionValue("query")
        );
        break;
      case "ExecuteUpdate":
        exampleExecuteUpdate(
            cmd.getOptionValue("query")
        );
        break;
      case "GetCatalogs":
        exampleGetCatalogs();
        break;
      case "GetSchemas":
        exampleGetSchemas(
            cmd.getOptionValue("catalog"),
            cmd.getOptionValue("schema")
        );
        break;
      case "GetTableTypes":
        exampleGetTableTypes();
        break;
      case "GetTables":
        exampleGetTables(
            cmd.getOptionValue("catalog"),
            cmd.getOptionValue("schema"),
            cmd.getOptionValue("table")
        );
        break;
      case "GetExportedKeys":
        exampleGetExportedKeys(
            cmd.getOptionValue("catalog"),
            cmd.getOptionValue("schema"),
            cmd.getOptionValue("table")
        );
        break;
      case "GetImportedKeys":
        exampleGetImportedKeys(
            cmd.getOptionValue("catalog"),
            cmd.getOptionValue("schema"),
            cmd.getOptionValue("table")
        );
        break;
      case "GetPrimaryKeys":
        exampleGetPrimaryKeys(
            cmd.getOptionValue("catalog"),
            cmd.getOptionValue("schema"),
            cmd.getOptionValue("table")
        );
        break;
      default:
        System.out.println("Command used is not valid! Please use one of: \n" +
            "[\"ExecuteUpdate\",\n" +
            "\"Execute\",\n" +
            "\"GetCatalogs\",\n" +
            "\"GetSchemas\",\n" +
            "\"GetTableTypes\",\n" +
            "\"GetTables\",\n" +
            "\"GetExportedKeys\",\n" +
            "\"GetImportedKeys\",\n" +
            "\"GetPrimaryKeys\"]");
    }
  }

  /**
   * Creates a {@link FlightSqlClient} to be used with the example methods.
   *
   * @param host client's hostname.
   * @param port client's port.
   */
  public void createFlightSqlClient(final String host, final int port) {
    final Location clientLocation = Location.forGrpcInsecure(host, port);
    flightSqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
  }

  private void exampleExecute(final String query) throws Exception {
    printFlightInfoResults(flightSqlClient.execute(query, getCallOptions()));
  }

  private void exampleExecuteUpdate(final String query) {
    System.out.println("Updated: " + flightSqlClient.executeUpdate(query, getCallOptions()) + "rows.");
  }

  private void exampleGetCatalogs() throws Exception {
    printFlightInfoResults(flightSqlClient.getCatalogs(getCallOptions()));
  }

  private void exampleGetSchemas(final String catalog, final String schema) throws Exception {
    printFlightInfoResults(flightSqlClient.getSchemas(catalog, schema, getCallOptions()));
  }

  private void exampleGetTableTypes() throws Exception {
    printFlightInfoResults(flightSqlClient.getTableTypes(getCallOptions()));
  }

  private void exampleGetTables(final String catalog, final String schema, final String table) throws Exception {
    // For now, this won't filter by table types.
    printFlightInfoResults(flightSqlClient.getTables(
        catalog, schema, table, null, false, getCallOptions()));
  }

  private void exampleGetExportedKeys(final String catalog, final String schema, final String table) throws Exception {
    printFlightInfoResults(flightSqlClient.getExportedKeys(TableRef.of(catalog, schema, table), getCallOptions()));
  }

  private void exampleGetImportedKeys(final String catalog, final String schema, final String table) throws Exception {
    printFlightInfoResults(flightSqlClient.getImportedKeys(TableRef.of(catalog, schema, table), getCallOptions()));
  }

  private void exampleGetPrimaryKeys(final String catalog, final String schema, final String table) throws Exception {
    printFlightInfoResults(flightSqlClient.getPrimaryKeys(TableRef.of(catalog, schema, table), getCallOptions()));
  }

  private void printFlightInfoResults(final FlightInfo flightInfo) throws Exception {
    final FlightStream stream =
        flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), getCallOptions());
    while (stream.next()) {
      try (final VectorSchemaRoot root = stream.getRoot()) {
        System.out.println(root.contentToTSVString());
      }
    }
    stream.close();
  }

  @Override
  public void close() throws Exception {
    flightSqlClient.close();
    allocator.close();
  }
}
