package flightsql

import (
	"context"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrNotImplemented = errors.New("not implemented")
	ErrNotSupported   = errors.New("not supported")
	ErrOutOfRange     = errors.New("index out of range")
)

const dsnPattern = "flightsql://token@address[:port]/bucket[?param1=value1&...&paramN=valueN]"

type Rows struct {
	schema        *arrow.Schema
	records       []arrow.Record
	currentRecord int
	currentRow    int
}

// Columns returns the names of the columns.
func (r *Rows) Columns() []string {
	if len(r.records) == 0 {
		return nil
	}

	// All records have the same columns
	var cols []string
	for _, c := range r.schema.Fields() {
		cols = append(cols, c.Name)
	}

	return cols
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	for _, rec := range r.records {
		rec.Release()
	}
	r.currentRecord = 0
	r.currentRow = 0

	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *Rows) Next(dest []driver.Value) error {
	if r.currentRecord >= len(r.records) {
		return io.EOF
	}
	record := r.records[r.currentRecord]

	if int64(r.currentRow) >= record.NumRows() {
		return ErrOutOfRange
	}

	for i, arr := range record.Columns() {
		switch c := arr.(type) {
		case *array.Boolean:
			dest[i] = c.Value(r.currentRow)
		case *array.Float16:
			dest[i] = float64(c.Value(r.currentRow).Float32())
		case *array.Float32:
			dest[i] = float64(c.Value(r.currentRow))
		case *array.Float64:
			dest[i] = c.Value(r.currentRow)
		case *array.Int8:
			dest[i] = int64(c.Value(r.currentRow))
		case *array.Int16:
			dest[i] = int64(c.Value(r.currentRow))
		case *array.Int32:
			dest[i] = int64(c.Value(r.currentRow))
		case *array.Int64:
			dest[i] = c.Value(r.currentRow)
		case *array.String:
			dest[i] = c.Value(r.currentRow)
		case *array.Time32:
			dt, ok := arr.DataType().(*arrow.Time32Type)
			if !ok {
				return fmt.Errorf("datatype %T not matching time32", arr.DataType())
			}
			v := c.Value(r.currentRow)
			dest[i] = v.ToTime(dt.TimeUnit())
		case *array.Time64:
			dt, ok := arr.DataType().(*arrow.Time64Type)
			if !ok {
				return fmt.Errorf("datatype %T not matching time64", arr.DataType())
			}
			v := c.Value(r.currentRow)
			dest[i] = v.ToTime(dt.TimeUnit())
		case *array.Timestamp:
			dt, ok := arr.DataType().(*arrow.TimestampType)
			if !ok {
				return fmt.Errorf("datatype %T not matching timestamp", arr.DataType())
			}
			v := c.Value(r.currentRow)
			dest[i] = v.ToTime(dt.TimeUnit())
		default:
			return fmt.Errorf("type %T: %w", arr, ErrNotSupported)
		}
	}

	r.currentRow++
	if int64(r.currentRow) >= record.NumRows() {
		r.currentRecord++
		r.currentRow = 0
	}

	return nil
}

type Result struct{}

// LastInsertId returns the database's auto-generated ID after, for example,
// an INSERT into a table with primary key.
func (r *Result) LastInsertId() (int64, error) {
	return 0, ErrNotImplemented
}

// RowsAffected returns the number of rows affected by the query.
func (r *Result) RowsAffected() (int64, error) {
	return 0, ErrNotImplemented
}

type Stmt struct {
	stmt   *PreparedStatement
	client *Client
}

// Close closes the statement.
func (s *Stmt) Close() error {
	ctx := context.Background()
	return s.stmt.Close(ctx)
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	// If NumInput returns >= 0, the sql package will sanity check
	// argument counts from callers and return errors to the caller
	// before the statement's Exec or Query methods are called.
	//
	// NumInput may also return -1, if the driver doesn't know
	// its number of placeholders. In that case, the sql package
	// will not sanity check Exec or Query argument counts.
	return -1
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	//s.stmt.Execute(ctx)
	return nil, ErrNotImplemented
}

// Query executes a query that may return rows, such as a SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	ctx := context.Background()
	info, err := s.stmt.Execute(ctx)
	if err != nil {
		return nil, err
	}

	rows := Rows{}
	for _, endpoint := range info.Endpoint {
		reader, err := s.client.DoGet(ctx, endpoint.GetTicket())
		if err != nil {
			return nil, fmt.Errorf("getting ticket failed: %w", err)
		}
		record, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("reading record failed: %w", err)
		}

		if rows.schema == nil {
			rows.schema = record.Schema()
		}
		if !rows.schema.Equal(record.Schema()) {
			return nil, fmt.Errorf("mixed schemas %w", ErrNotSupported)
		}
		rows.records = append(rows.records, record)
	}

	return &rows, nil
}

type Tx struct {
}

func (t *Tx) Commit() error {
	return ErrNotImplemented
}

func (t *Tx) Rollback() error {
	return ErrNotImplemented
}

type grpcCredentials struct {
	token      string
	bucketName string
}

func (g grpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	md := map[string]string{
		"iox-namespace-name": g.bucketName,
	}
	if g.token != "" {
		md["authorization"] = "Bearer " + g.token
	}
	return md, nil
}

func (g grpcCredentials) RequireTransportSecurity() bool {
	return g.token != ""
}

type Driver struct {
	client *Client
}

// Open returns a new connection to the database.
func (d *Driver) Open(name string) (driver.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}

	// Sanity checks on the given connection string
	var creds credentials.TransportCredentials
	switch u.Scheme {
	case "flightsql":
		creds = insecure.NewCredentials()
	case "flightsqls":
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		credentials.NewClientTLSFromCert(pool, "")
	default:
		return nil, fmt.Errorf("invalid scheme %q; has to be 'flightsql' or 'flightsqls", u.Scheme)
	}
	var token string
	if u.User != nil {
		token = u.User.Username()
	}
	if _, set := u.User.Password(); set {
		return nil, fmt.Errorf("invalid DSN %q; has to follow pattern %q", name, dsnPattern)
	}
	addr := u.Host
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 1 || parts[0] == "" {
		return nil, fmt.Errorf("invalid path in DSN; has to follow pattern %q", dsnPattern)
	}
	bucket := parts[0]

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(grpcCredentials{token: token, bucketName: bucket}),
		grpc.WithBlock(),
	}

	ctx := context.Background()
	if u.Query().Has("timeout") {
		timeout, err := time.ParseDuration(u.Query().Get("timeout"))
		if err != nil {
			return nil, err
		}
		ctx, _ = context.WithTimeout(ctx, timeout)
	}

	d.client, err = NewClientCtx(ctx, addr, nil, nil, options...)
	if err != nil {
		return nil, err
	}

	return d, nil
}

// Prepare returns a prepared statement, bound to this connection.
func (d *Driver) Prepare(query string) (driver.Stmt, error) {
	ctx := context.Background()
	stmt, err := d.client.Prepare(ctx, nil, query)
	if err != nil {
		fmt.Printf("%v (%T)\n", err, err)
		return nil, err
	}
	return &Stmt{stmt: stmt, client: d.client}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (d *Driver) Close() error {
	// Drivers must ensure all network calls made by Close
	// do not block indefinitely (e.g. apply a timeout).
	return d.client.Close()
}

// Begin starts and returns a new transaction.
func (d *Driver) Begin() (driver.Tx, error) {
	return nil, ErrNotImplemented
}

func init() {
	sql.Register("flightsql", &Driver{})
}
