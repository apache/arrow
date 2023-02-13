package flightsql

import (
	"errors"
	"fmt"
	"net/url"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const dsnPatternSQLite = "sqlite://address[:port][?param1=value1&...&paramN=valueN]"

func checkSqliteConfig(config *DriverConfig) error {
	// Check for a valid backend
	if config.Backend != "sqlite" {
		return errors.New("invalid backend")
	}

	// Check if we have a valid host
	if config.Address == "" {
		return errors.New("missing address")
	}

	// We do not expect any username, password or token
	if config.Username != "" || config.Password != "" || config.Token != "" {
		return errors.New("unexpected credentials")
	}

	// We do not expect any database
	if config.Database != "" {
		return errors.New("unexpected database")
	}

	return nil
}

func newSqliteBackend(config *DriverConfig) ([]grpc.DialOption, error) {
	if err := checkSqliteConfig(config); err != nil {
		return nil, fmt.Errorf("%w; DSN has to follow pattern %q", err, dsnPatternSQLite)
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	return options, nil
}

func generateSqliteDSN(config *DriverConfig) (string, error) {
	if err := checkSqliteConfig(config); err != nil {
		return "", err
	}

	u := url.URL{
		Scheme: config.Backend,
		Host:   config.Address,
	}

	q := u.Query()
	if config.Timeout > 0 {
		q.Set("timeout", config.Timeout.String())
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}
