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

//go:build go1.18
// +build go1.18

package driver_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql/driver"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql/example"
	"github.com/apache/arrow/go/v16/arrow/memory"
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

	Config     driver.DriverConfig
	TableName  string
	Statements map[string]string

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
	db, err := sql.Open("flightsql", cfg.DSN())
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
	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)
	defer db.Close()

	result, err := db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.Equal(t, int64(0), affected)
	require.NoError(t, err)

	last, err := result.LastInsertId()
	require.Equal(t, int64(-1), last)
	require.ErrorIs(t, err, driver.ErrNotSupported)

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
	db, err := sql.Open("flightsql", cfg.DSN())
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
	result, err := db.Exec(strings.Join(stmts, "\n"))
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.Equal(t, int64(1), affected)
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
	db, err := sql.Open("flightsql", cfg.DSN())
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

func (s *SqlTestSuite) TestQueryWithEmptyResultset() {
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
	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Create the table
	_, err = db.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	rows, err := db.Query(fmt.Sprintf(s.Statements["query"], s.TableName))
	require.NoError(t, err)
	require.False(t, rows.Next())

	row := db.QueryRow(fmt.Sprintf(s.Statements["query"], s.TableName))
	require.NotNil(t, row)
	require.NoError(t, row.Err())

	target := make(map[string]any)
	err = row.Scan(&target)
	require.ErrorIs(t, err, sql.ErrNoRows)

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
	db, err := sql.Open("flightsql", cfg.DSN())
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

// TestRowsManualPrematureClose tests concurrent rows implementation for closing right after loading.
// Is expected that rows' internal engine update its status, preventing errors and inconsistent further operations.
func (s *SqlTestSuite) TestRowsManualPrematureClose() {
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table
	const tableName = `TestRowsManualPrematureClose`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount int = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do query
	const sqlSelectAll = `SELECT id, name, value FROM ` + tableName

	rows, err := db.QueryContext(context.TODO(), sqlSelectAll)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	// Close Rows normally
	require.NoError(t, rows.Close())

	require.False(t, rows.Next())

	// Safe double-closing
	require.NoError(t, rows.Close())

	// Columns() should return an error after rows.Close() (sql: Rows are closed)
	columns, err := rows.Columns()
	require.Error(t, err)
	require.Empty(t, columns)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsNormalExhaustion tests concurrent rows implementation for normal query/netx/close operation
func (s *SqlTestSuite) TestRowsNormalExhaustion() {
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table
	const tableName = `TestRowsNormalExhaustion`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount int = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do Query
	const sqlSelectAll = `SELECT id, name, value FROM ` + tableName

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	rows, err := db.QueryContext(ctx, sqlSelectAll)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	var (
		actualCount = 0
		xid,
		xvalue int
		xname string
	)

	for rows.Next() {
		require.NoError(t, rows.Scan(&xid, &xname, &xvalue))
		actualCount++
	}

	require.Equal(t, rowCount, actualCount)
	require.NoError(t, rows.Close())

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsPrematureCloseDuringNextLoop ensures that:
// - closing during Next() loop doesn't trigger concurrency errors.
// - the interation is properly/promptly interrupted.
func (s *SqlTestSuite) TestRowsPrematureCloseDuringNextLoop() {
	t := s.T()

	// Create and start the server.
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table.
	const tableName = `TestRowsPrematureCloseDuringNextLoop`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do query
	const sqlSelectAll = `SELECT id, name, value FROM ` + tableName

	rows, err := db.QueryContext(context.TODO(), sqlSelectAll)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	const closeAfterNRows = 10
	var (
		i,
		xid,
		xvalue int
		xname string
	)

	for rows.Next() {
		err = rows.Scan(&xid, &xname, &xvalue)
		require.NoError(t, err)

		i++
		if i >= closeAfterNRows {
			require.NoError(t, rows.Close())
		}
	}

	require.Equal(t, closeAfterNRows, i)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsInterruptionByContextManualCancellation cancels the context before it starts retrieving rows.Next().
// it gives time for cancellation propagation, and ensures that no further data was retrieved.
func (s *SqlTestSuite) TestRowsInterruptionByContextManualCancellation() {
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table
	const tableName = `TestRowsInterruptionByContextManualCancellation`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do query
	const sqlSelectAll = `SELECT id, name, value FROM ` + tableName

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	rows, err := db.QueryContext(ctx, sqlSelectAll)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	defer rows.Close()

	go cancel()

	time.Sleep(100 * time.Millisecond)

	count := 0
	for rows.Next() {
		count++
	}

	require.Zero(t, count)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsInterruptionByContextTimeout forces a timeout, and ensures no further data is retrieved after that.
func (s *SqlTestSuite) TestRowsInterruptionByContextTimeout() {
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table
	const tableName = `TestRowsInterruptionByContextTimeout`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do query
	const (
		timeout      = 1500 * time.Millisecond
		sqlSelectAll = `SELECT id, name, value FROM ` + tableName
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, sqlSelectAll)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	defer rows.Close()

	// eventually, after time.Sleep(), the context will be cancelled.
	// then, rows.Next() should return false, and <-ctx.Done() will never be tested.
	for rows.Next() {
		select {
		case <-ctx.Done():
			t.Fatal("cancellation didn't prevent more records to be read")
		default:
			time.Sleep(time.Second)
		}
	}

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsManualPrematureCloseStmt tests concurrent rows implementation for closing right after loading.
// Is expected that rows' internal engine update its status, preventing errors and inconsistent further operations.
func (s *SqlTestSuite) TestRowsManualPrematureCloseStmt() {
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table
	const tableName = `TestRowsManualPrematureCloseStmt`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount int = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do query
	const sqlSelectAll = `SELECT id, name, value FROM ` + tableName

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, sqlSelectAll)
	require.NoError(t, err)

	rows, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	// Close Rows normally
	require.NoError(t, rows.Close())

	require.False(t, rows.Next())

	// Safe double-closing
	require.NoError(t, rows.Close())

	// Columns() should return an error after rows.Close() (sql: Rows are closed)
	columns, err := rows.Columns()
	require.Error(t, err)
	require.Empty(t, columns)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsNormalExhaustionStmt tests concurrent rows implementation for normal query/netx/close operation
func (s *SqlTestSuite) TestRowsNormalExhaustionStmt() {
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table
	const tableName = `TestRowsNormalExhaustionStmt`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount int = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do Query
	const sqlSelectAll = `SELECT id, name, value FROM ` + tableName

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, sqlSelectAll)
	require.NoError(t, err)

	rows, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	var (
		actualCount = 0
		xid,
		xvalue int
		xname string
	)

	for rows.Next() {
		require.NoError(t, rows.Scan(&xid, &xname, &xvalue))
		actualCount++
	}

	require.Equal(t, rowCount, actualCount)
	require.NoError(t, rows.Close())

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsPrematureCloseDuringNextLoopStmt ensures that:
// - closing during Next() loop doesn't trigger concurrency errors.
// - the interation is properly/promptly interrupted.
func (s *SqlTestSuite) TestRowsPrematureCloseDuringNextLoopStmt() {
	t := s.T()

	// Create and start the server.
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table.
	const tableName = `TestRowsPrematureCloseDuringNextLoopStmt`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do query
	const sqlSelectAll = `SELECT id, name, value FROM ` + tableName

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, sqlSelectAll)
	require.NoError(t, err)

	rows, err := stmt.QueryContext(ctx)

	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	const closeAfterNRows = 10
	var (
		i,
		xid,
		xvalue int
		xname string
	)

	for rows.Next() {
		err = rows.Scan(&xid, &xname, &xvalue)
		require.NoError(t, err)

		i++
		if i >= closeAfterNRows {
			require.NoError(t, rows.Close())
		}
	}

	require.Equal(t, closeAfterNRows, i)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsInterruptionByContextManualCancellationStmt cancels the context before it starts retrieving rows.Next().
// it gives time for cancellation propagation, and ensures that no further data was retrieved.
func (s *SqlTestSuite) TestRowsInterruptionByContextManualCancellationStmt() {
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table
	const tableName = `TestRowsInterruptionByContextManualCancellationStmt`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do query
	const sqlSelectAll = `SELECT id, name, value FROM ` + tableName

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, sqlSelectAll)
	require.NoError(t, err)

	rows, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	defer rows.Close()

	go cancel()

	time.Sleep(100 * time.Millisecond)

	count := 0
	for rows.Next() {
		count++
	}

	require.Zero(t, count)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

// TestRowsInterruptionByContextTimeoutStmt forces a timeout, and ensures no further data is retrieved after that.
func (s *SqlTestSuite) TestRowsInterruptionByContextTimeoutStmt() {
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

	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)

	defer db.Close()

	// Create the table
	const tableName = `TestRowsInterruptionByContextTimeoutStmt`
	const ddlCreateTable = `CREATE TABLE ` + tableName + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(300), value INT);`

	_, err = db.Exec(ddlCreateTable)
	require.NoError(t, err)

	// generate data enough for chunked concurrent test:
	const rowCount = 6000
	const randStringLen = 250
	const sqlInsert = `INSERT INTO ` + tableName + ` (name,value) VALUES `

	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	var sb strings.Builder
	sb.WriteString(sqlInsert)

	for i := 0; i < rowCount; i++ {
		sb.WriteString(fmt.Sprintf(`('%s', %d),`, getRandomString(gen, randStringLen), gen.Int()))
	}

	insertQuery := strings.TrimSuffix(sb.String(), ",")

	rs, err := db.Exec(insertQuery)
	require.NoError(t, err)

	insertedRows, err := rs.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), insertedRows)

	// Do query
	const (
		timeout      = 1500 * time.Millisecond
		sqlSelectAll = `SELECT id, name, value FROM ` + tableName
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, sqlSelectAll)
	require.NoError(t, err)

	rows, err := stmt.QueryContext(ctx)
	require.NoError(t, err)
	require.NotNil(t, rows)
	require.NoError(t, rows.Err())

	defer rows.Close()

	// eventually, after time.Sleep(), the context will be cancelled.
	// then, rows.Next() should return false, and <-ctx.Done() will never be tested.
	for rows.Next() {
		select {
		case <-ctx.Done():
			t.Fatal("cancellation didn't prevent more records to be read")
		default:
			time.Sleep(time.Second)
		}
	}

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
	db, err := sql.Open("flightsql", cfg.DSN())
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
	db, err := sql.Open("flightsql", cfg.DSN())
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
	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	// Create the table
	_, err = tx.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	// Insert data
	data := map[string]int{
		"zero":      0,
		"one":       1,
		"minus one": -1,
		"twelve":    12,
	}
	for k, v := range data {
		stmt := fmt.Sprintf(s.Statements["insert"], s.TableName, k, v)
		_, err = tx.Exec(stmt)
		require.NoError(t, err)
	}

	// Rollback the transaction
	require.NoError(t, tx.Rollback())

	// Check result
	tbls := `SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';`
	rows, err := db.Query(tbls)
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
	}
	require.Equal(t, 0, count)
	require.NoError(t, db.Close())

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

func (s *SqlTestSuite) TestTxCommit() {
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
	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	// Create the table
	_, err = tx.Exec(fmt.Sprintf(s.Statements["create table"], s.TableName))
	require.NoError(t, err)

	// Insert data
	data := map[string]int{
		"zero":      0,
		"one":       1,
		"minus one": -1,
		"twelve":    12,
	}
	for k, v := range data {
		stmt := fmt.Sprintf(s.Statements["insert"], s.TableName, k, v)
		_, err = tx.Exec(stmt)
		require.NoError(t, err)
	}

	// Commit the transaction
	require.NoError(t, tx.Commit())

	// Check if the table exists
	tbls := `SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';`
	rows, err := db.Query(tbls)
	require.NoError(t, err)

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.Contains(t, tables, "drivertest")

	// Check the actual data
	stmt, err := db.Prepare(fmt.Sprintf(s.Statements["query"], s.TableName))
	require.NoError(t, err)

	rows, err = stmt.Query()
	require.NoError(t, err)

	// Check result
	actual := make(map[string]int, len(data))
	for rows.Next() {
		var name string
		var id, value int
		require.NoError(t, rows.Scan(&id, &name, &value))
		actual[name] = value
	}
	require.NoError(t, db.Close())
	require.EqualValues(t, data, actual)

	// Tear-down server
	s.stopServer(server)
	wg.Wait()
}

/*** BACKEND tests ***/

func TestSqliteBackend(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	s := &SqlTestSuite{
		Config: driver.DriverConfig{
			Timeout: 5 * time.Second,
		},
	}

	s.createServer = func() (flight.Server, string, error) {
		server := flight.NewServerWithMiddleware(nil)

		// Setup the SQLite backend
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			return nil, "", err
		}
		sqliteServer, err := example.NewSQLiteFlightSQLServer(db)
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

func TestPreparedStatementSchema(t *testing.T) {
	// Setup the expected test
	backend := &MockServer{
		PreparedStatementParameterSchema: arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}, Nullable: false}}, nil),
		DataSchema: arrow.NewSchema([]arrow.Field{
			{Name: "time", Type: &arrow.Time64Type{Unit: arrow.Nanosecond}, Nullable: true},
			{Name: "value", Type: &arrow.Int64Type{}, Nullable: false},
		}, nil),
		Data: "[]",
	}

	// Instantiate a mock server
	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(backend))
	require.NoError(t, server.Init("localhost:0"))
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go server.Serve()
	defer server.Shutdown()

	// Configure client
	cfg := driver.DriverConfig{
		Timeout: 5 * time.Second,
		Address: server.Addr().String(),
	}
	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Do query
	stmt, err := db.Prepare("SELECT * FROM foo WHERE name LIKE ?")
	require.NoError(t, err)

	_, err = stmt.Query()
	require.ErrorContains(t, err, "expected 1 arguments, got 0")

	// Test for error issues by driver
	_, err = stmt.Query(23)
	require.ErrorContains(t, err, "invalid value type int64 for builder *array.StringBuilder")

	rows, err := stmt.Query("master")
	require.NoError(t, err)
	require.NotNil(t, rows)
}

func TestPreparedStatementNoSchema(t *testing.T) {
	// Setup the expected test
	backend := &MockServer{
		DataSchema: arrow.NewSchema([]arrow.Field{
			{Name: "time", Type: &arrow.Time64Type{Unit: arrow.Nanosecond}, Nullable: true},
			{Name: "value", Type: &arrow.Int64Type{}, Nullable: false},
		}, nil),
		Data:                            "[]",
		ExpectedPreparedStatementSchema: arrow.NewSchema([]arrow.Field{{Type: &arrow.StringType{}, Nullable: false}}, nil),
	}

	// Instantiate a mock server
	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(backend))
	require.NoError(t, server.Init("localhost:0"))
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go server.Serve()
	defer server.Shutdown()

	// Configure client
	cfg := driver.DriverConfig{
		Timeout: 5 * time.Second,
		Address: server.Addr().String(),
	}
	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Do query
	stmt, err := db.Prepare("SELECT * FROM foo WHERE name LIKE ?")
	require.NoError(t, err)

	_, err = stmt.Query()
	require.NoError(t, err, "expected 1 arguments, got 0")

	// Test for error issued by server due to missing parameter schema
	_, err = stmt.Query(23)
	require.ErrorContains(t, err, "parameter schema: unexpected")

	rows, err := stmt.Query("master")
	require.NoError(t, err)
	require.NotNil(t, rows)
}

func TestNoPreparedStatementImplemented(t *testing.T) {
	// Setup the expected test
	backend := &MockServer{
		DataSchema: arrow.NewSchema([]arrow.Field{
			{Name: "time", Type: &arrow.Time64Type{Unit: arrow.Nanosecond}, Nullable: true},
			{Name: "value", Type: &arrow.Int64Type{}, Nullable: false},
		}, nil),
		Data:                   "[]",
		PreparedStatementError: "not supported",
	}

	// Instantiate a mock server
	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(backend))
	require.NoError(t, server.Init("localhost:0"))
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go server.Serve()
	defer server.Shutdown()

	// Configure client
	cfg := driver.DriverConfig{
		Timeout: 5 * time.Second,
		Address: server.Addr().String(),
	}
	db, err := sql.Open("flightsql", cfg.DSN())
	require.NoError(t, err)
	defer db.Close()

	// Do query
	_, err = db.Query("SELECT * FROM foo")
	require.NoError(t, err)
}

// Mockup database server
type MockServer struct {
	flightsql.BaseServer
	DataSchema                       *arrow.Schema
	PreparedStatementParameterSchema *arrow.Schema
	PreparedStatementError           string
	Data                             string

	ExpectedPreparedStatementSchema *arrow.Schema
}

func (s *MockServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	if s.PreparedStatementError != "" {
		return flightsql.ActionCreatePreparedStatementResult{}, errors.New(s.PreparedStatementError)
	}
	return flightsql.ActionCreatePreparedStatementResult{
		Handle:          []byte("prepared"),
		DatasetSchema:   s.DataSchema,
		ParameterSchema: s.PreparedStatementParameterSchema,
	}, nil
}

func (s *MockServer) DoPutPreparedStatementQuery(ctx context.Context, qry flightsql.PreparedStatementQuery, r flight.MessageReader, w flight.MetadataWriter) error {
	if s.ExpectedPreparedStatementSchema != nil {
		if !s.ExpectedPreparedStatementSchema.Equal(r.Schema()) {
			return errors.New("parameter schema: unexpected")
		}
		return nil
	}

	if s.PreparedStatementParameterSchema != nil && !s.PreparedStatementParameterSchema.Equal(r.Schema()) {
		return fmt.Errorf("parameter schema: %w", arrow.ErrInvalid)
	}

	// GH-35328: it's rare, but this function can complete execution and return
	// closing the reader *after* the schema is written but *before* the parameter batch
	// is written (race condition based on goroutine scheduling). In that situation,
	// the client call to Write the parameter record batch will return an io.EOF because
	// this end of the connection will have closed before it attempted to send the batch.
	// This created a flaky test situation that was difficult to reproduce (1-4 failures
	// in 5000 runs). We can avoid this flakiness by simply *explicitly* draining the
	// record batch messages from the reader before returning.
	for r.Next() {
	}

	return nil
}

func (s *MockServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, s.DataSchema, strings.NewReader(s.Data))
	if err != nil {
		return nil, nil, err
	}
	chunk := make(chan flight.StreamChunk)
	go func() {
		defer close(chunk)
		chunk <- flight.StreamChunk{
			Data: record,
			Desc: nil,
			Err:  nil,
		}
	}()
	return s.DataSchema, chunk, nil
}

func (s *MockServer) GetFlightInfoPreparedStatement(ctx context.Context, stmt flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	handle := stmt.GetPreparedStatementHandle()
	ticket, err := flightsql.CreateStatementQueryTicket(handle)
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: ticket}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (s *MockServer) GetFlightInfoStatement(_ context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	handle := query.GetTransactionId()
	ticket, err := flightsql.CreateStatementQueryTicket(handle)
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: ticket}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

const getRandomStringCharset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789. "

var getRandomStringCharsetLen = len(getRandomStringCharset)

func getRandomString(gen *rand.Rand, length int) string {
	result := make([]byte, length)

	for i := range result {
		result[i] = getRandomStringCharset[rand.Intn(getRandomStringCharsetLen)]
	}

	return string(result)
}
