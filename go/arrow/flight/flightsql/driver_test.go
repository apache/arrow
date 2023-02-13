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

package flightsql_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql/example"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

const defaultTableName = "drivertest"

var defaultStatements = map[string]string{
	"create table": `
CREATE TABLE %s (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name varchar(100),
  value int
);`,
	"insert":            `INSERT INTO %s (name, value) VALUES ('%s', %d);`,
	"query":             `SELECT * FROM %s;`,
	"constraint query":  `SELECT * FROM %s WHERE name LIKE '%%%s%%'`,
	"placeholder query": `SELECT * FROM %s WHERE name LIKE ?`,
}

type SqlTestSuite struct {
	suite.Suite

	Config     flightsql.DriverConfig
	TableName  string
	Statements map[string]string

	// Feature map of the backend
	supportsNumInputs bool

	createServer func() (flight.Server, string, error)
	startServer  func(flight.Server) error
	stopServer   func(flight.Server)
}

func (s *SqlTestSuite) SetupSuite() {
	if s.TableName == "" {
		s.TableName = defaultTableName
	}

	if s.Statements == nil {
		s.Statements = make(map[string]string)
	}
	// Fill in the statements. Keep statements already defined e.g. by the
	// user or suite-generator.
	for k, v := range defaultStatements {
		if _, found := s.Statements[k]; !found {
			s.Statements[k] = v
		}
	}

	require.Contains(s.T(), s.Statements, "create table")
	require.Contains(s.T(), s.Statements, "insert")
	require.Contains(s.T(), s.Statements, "query")
	require.Contains(s.T(), s.Statements, "constraint query")
	require.Contains(s.T(), s.Statements, "placeholder query")
}

func (s *SqlTestSuite) TestOpenClose() {
	t := s.T()

	// Create and start the server
	server, addr, err := s.createServer()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(s.T(), s.startServer(server))
	}()
	defer s.stopServer(server)
	time.Sleep(100 * time.Millisecond)

	// Configure client
	cfg := s.Config
	cfg.Address = addr
	dsn, err := cfg.ToDSN()
	require.NoError(t, err)

	// Actual test
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

func (s *SqlTestSuite) TestCreateTable() {
	t := s.T()

	// Create and start the server
	server, addr, err := s.createServer()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(s.T(), s.startServer(server))
	}()
	defer s.stopServer(server)
	time.Sleep(100 * time.Millisecond)

	// Configure client
	cfg := s.Config
	cfg.Address = addr
	dsn, err := cfg.ToDSN()
	require.NoError(t, err)

	// Actual test
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	result, err := db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.Equal(t, affected, int64(-1))
	require.ErrorIs(t, err, flightsql.ErrNotSupported)

	last, err := result.LastInsertId()
	require.Equal(t, last, int64(-1))
	require.ErrorIs(t, err, flightsql.ErrNotSupported)

	require.NoError(t, db.Close())

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

func (s *SqlTestSuite) TestInsert() {
	t := s.T()

	// Create and start the server
	server, addr, err := s.createServer()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(s.T(), s.startServer(server))
	}()
	defer s.stopServer(server)
	time.Sleep(100 * time.Millisecond)

	// Configure client
	cfg := s.Config
	cfg.Address = addr
	dsn, err := cfg.ToDSN()
	require.NoError(t, err)

	// Actual test
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create the table
	_, err = db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	// Insert data
	values := map[string]int{
		"zero":      0,
		"one":       1,
		"minus one": -1,
		"twelve":    12,
	}
	var stmts []string
	for k, v := range values {
		stmts = append(stmts, fmt.Sprintf(s.Statements["insert"], s.TableName, k, v))
	}
	_, err = db.Exec(strings.Join(stmts, "\n"))
	require.NoError(t, err)

	require.NoError(t, db.Close())

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

func (s *SqlTestSuite) TestQuery() {
	t := s.T()

	// Create and start the server
	server, addr, err := s.createServer()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(s.T(), s.startServer(server))
	}()
	defer s.stopServer(server)
	time.Sleep(100 * time.Millisecond)

	// Configure client
	cfg := s.Config
	cfg.Address = addr
	dsn, err := cfg.ToDSN()
	require.NoError(t, err)

	// Actual test
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create the table
	_, err = db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	// Insert data
	expected := map[string]int{
		"zero":      0,
		"one":       1,
		"minus one": -1,
		"twelve":    12,
	}
	var stmts []string
	for k, v := range expected {
		stmts = append(stmts, fmt.Sprintf(s.Statements["insert"], s.TableName, k, v))
	}
	_, err = db.Exec(strings.Join(stmts, "\n"))
	require.NoError(t, err)

	rows, err := db.Query(fmt.Sprintf(s.Statements["query"], s.TableName))
	require.NoError(t, err)

	// Check result
	actual := make(map[string]int, len(expected))
	for rows.Next() {
		var name string
		var id, value int
		require.NoError(t, rows.Scan(&id, &name, &value))
		actual[name] = value
	}
	require.NoError(t, db.Close())
	require.EqualValues(t, expected, actual)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

func (s *SqlTestSuite) TestPreparedQuery() {
	t := s.T()

	// Create and start the server
	server, addr, err := s.createServer()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(s.T(), s.startServer(server))
	}()
	defer s.stopServer(server)
	time.Sleep(100 * time.Millisecond)

	// Configure client
	cfg := s.Config
	cfg.Address = addr
	dsn, err := cfg.ToDSN()
	require.NoError(t, err)

	// Actual test
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create the table
	_, err = db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	// Insert data
	expected := map[string]int{
		"zero":      0,
		"one":       1,
		"minus one": -1,
		"twelve":    12,
	}
	var stmts []string
	for k, v := range expected {
		stmts = append(stmts, fmt.Sprintf(s.Statements["insert"], s.TableName, k, v))
	}
	_, err = db.Exec(strings.Join(stmts, "\n"))
	require.NoError(t, err)

	// Do query
	stmt, err := db.Prepare(fmt.Sprintf(s.Statements["query"], s.TableName))
	require.NoError(t, err)

	rows, err := stmt.Query()
	require.NoError(t, err)

	// Check result
	actual := make(map[string]int, len(expected))
	for rows.Next() {
		var name string
		var id, value int
		require.NoError(t, rows.Scan(&id, &name, &value))
		actual[name] = value
	}
	require.NoError(t, db.Close())
	require.EqualValues(t, expected, actual)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

func (s *SqlTestSuite) TestPreparedQueryWithConstraint() {
	t := s.T()

	// Create and start the server
	server, addr, err := s.createServer()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(s.T(), s.startServer(server))
	}()
	defer s.stopServer(server)
	time.Sleep(100 * time.Millisecond)

	// Configure client
	cfg := s.Config
	cfg.Address = addr
	dsn, err := cfg.ToDSN()
	require.NoError(t, err)

	// Actual test
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create the table
	_, err = db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	// Insert data
	data := map[string]int{
		"zero":      0,
		"one":       1,
		"minus one": -1,
		"twelve":    12,
	}
	var stmts []string
	for k, v := range data {
		stmts = append(stmts, fmt.Sprintf(s.Statements["insert"], s.TableName, k, v))
	}
	_, err = db.Exec(strings.Join(stmts, "\n"))
	require.NoError(t, err)

	// Do query
	stmt, err := db.Prepare(fmt.Sprintf(s.Statements["constraint query"], s.TableName, "one"))
	require.NoError(t, err)

	rows, err := stmt.Query()
	require.NoError(t, err)

	// Check result
	expected := map[string]int{
		"one":       1,
		"minus one": -1,
	}
	actual := make(map[string]int, len(expected))
	for rows.Next() {
		var name string
		var id, value int
		require.NoError(t, rows.Scan(&id, &name, &value))
		actual[name] = value
	}
	require.NoError(t, db.Close())
	require.EqualValues(t, expected, actual)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

func (s *SqlTestSuite) TestPreparedQueryWithPlaceholder() {
	t := s.T()

	// Create and start the server
	server, addr, err := s.createServer()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(s.T(), s.startServer(server))
	}()
	defer s.stopServer(server)
	time.Sleep(100 * time.Millisecond)

	// Configure client
	cfg := s.Config
	cfg.Address = addr
	dsn, err := cfg.ToDSN()
	require.NoError(t, err)

	// Actual test
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create the table
	_, err = db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	// Insert data
	data := map[string]int{
		"zero":      0,
		"one":       1,
		"minus one": -1,
		"twelve":    12,
	}
	var stmts []string
	for k, v := range data {
		stmts = append(stmts, fmt.Sprintf(s.Statements["insert"], s.TableName, k, v))
	}
	_, err = db.Exec(strings.Join(stmts, "\n"))
	require.NoError(t, err)

	// Do query
	query := fmt.Sprintf(s.Statements["placeholder query"], s.TableName)
	stmt, err := db.Prepare(query)
	require.NoError(t, err)

	params := []interface{}{"%%one%%"}
	rows, err := stmt.Query(params...)
	require.NoError(t, err)

	// Check result
	expected := map[string]int{
		"one":       1,
		"minus one": -1,
	}
	actual := make(map[string]int, len(expected))
	for rows.Next() {
		var name string
		var id, value int
		require.NoError(t, rows.Scan(&id, &name, &value))
		actual[name] = value
	}
	require.NoError(t, db.Close())
	require.EqualValues(t, expected, actual)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

func (s *SqlTestSuite) TestTxRollback() {
	t := s.T()

	// Create and start the server
	server, addr, err := s.createServer()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(s.T(), s.startServer(server))
	}()
	defer s.stopServer(server)
	time.Sleep(100 * time.Millisecond)

	// Configure client
	cfg := s.Config
	cfg.Address = addr
	dsn, err := cfg.ToDSN()
	require.NoError(t, err)

	// Actual test
	db, err := sql.Open("flightsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Create the table
	_, err = db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	// Insert a test row to be sure the test works
	_, err = db.Exec(fmt.Sprintf(s.Statements["insert"], s.TableName, "test", 42))
	require.NoError(t, err)

	// Insert data in transaction
	data := map[string]int{
		"zero":      0,
		"one":       1,
		"minus one": -1,
		"twelve":    12,
	}
	tx, err := db.Begin()
	require.NoError(t, err)
	for k, v := range data {
		_, err := tx.Exec(fmt.Sprintf(s.Statements["insert"], s.TableName, k, v))
		require.NoError(t, err)
	}
	// Rollback the transaction
	require.NoError(t, tx.Rollback())

	// Check result
	rows, err := db.Query(fmt.Sprintf(s.Statements["query"], s.TableName))
	require.NoError(t, err)

	expected := map[string]int{"test": 42}
	actual := make(map[string]int, len(expected))
	for rows.Next() {
		var name string
		var id, value int
		require.NoError(t, rows.Scan(&id, &name, &value))
		actual[name] = value
	}
	require.NoError(t, db.Close())
	require.EqualValues(t, expected, actual)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

/*** BACKEND tests ***/

func TestSqliteBackend(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	s := &SqlTestSuite{
		Config: flightsql.DriverConfig{
			Backend: "sqlite",
			Timeout: 5 * time.Second,
		},
	}

	s.createServer = func() (flight.Server, string, error) {
		server := flight.NewServerWithMiddleware(nil)

		// Setup the SQLite backend
		sqliteServer, err := example.NewSQLiteFlightSQLServer()
		if err != nil {
			return nil, "", err
		}
		sqliteServer.Alloc = mem

		// Connect the FlightSQL frontend to the backend
		server.RegisterFlightService(flightsql.NewFlightServer(sqliteServer))
		if err := server.Init("localhost:0"); err != nil {
			return nil, "", err
		}
		server.SetShutdownOnSignals(os.Interrupt, os.Kill)
		return server, server.Addr().String(), nil
	}
	s.startServer = func(server flight.Server) error { return server.Serve() }
	s.stopServer = func(server flight.Server) { server.Shutdown() }

	suite.Run(t, s)
}

func TestIOxBackend(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	s := &SqlTestSuite{
		Config: flightsql.DriverConfig{
			Backend:  "iox",
			Database: "sql_test",
			Timeout:  5 * time.Second,
		},
	}

	s.createServer = func() (flight.Server, string, error) { return nil, "127.0.0.1:8082", nil }
	s.startServer = func(server flight.Server) error { return nil }
	s.stopServer = func(server flight.Server) {}

	suite.Run(t, s)
}
