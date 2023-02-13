package flightsql

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcCredentials struct {
	token  string
	bucket string
}

func (g grpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	md := map[string]string{
		"iox-namespace-name": g.bucket,
	}
	if g.token != "" {
		md["authorization"] = "Bearer " + g.token
	}
	return md, nil
}

func (g grpcCredentials) RequireTransportSecurity() bool {
	return g.token != ""
}

const dsnPatternIOx = "iox[s]://token@address[:port]/bucket[?param1=value1&...&paramN=valueN]"

func checkIOxConfig(config *DriverConfig) error {
	// Check for a valid backend
	switch config.Backend {
	case "iox", "ioxs":
	default:
		return errors.New("invalid backend")
	}

	// Check if we have a valid host
	if config.Address == "" {
		return errors.New("missing address")
	}

	// Gracefully handle tokens given as usernames
	if config.Token == "" {
		config.Token = config.Username
		config.Username = ""
	}

	// We do not expect any username or password
	if config.Username != "" || config.Password != "" {
		return errors.New("unexpected username or password")
	}

	// We do expect a database alias bucket
	if config.Database == "" {
		return errors.New("missing atabase")
	}

	return nil
}

func newIOxBackend(config *DriverConfig) ([]grpc.DialOption, error) {
	// Sanity checks on the given connection string
	if err := checkIOxConfig(config); err != nil {
		return nil, fmt.Errorf("%w; DSN has to follow pattern %q", err, dsnPatternIOx)
	}

	var transportCreds credentials.TransportCredentials
	switch config.Backend {
	case "iox":
		transportCreds = insecure.NewCredentials()
	case "ioxs":
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		transportCreds = credentials.NewClientTLSFromCert(pool, "")
	default:
		return nil, fmt.Errorf("invalid backend %q", config.Backend)
	}

	rpcCreds := grpcCredentials{
		token:  config.Token,
		bucket: config.Database,
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithPerRPCCredentials(rpcCreds),
	}

	return options, nil
}

func generateIOxDSN(config *DriverConfig) (string, error) {
	if err := checkIOxConfig(config); err != nil {
		return "", err
	}

	u := url.URL{
		Scheme: config.Backend,
		Host:   config.Address,
		Path:   config.Database,
	}
	if config.Token != "" {
		u.User = url.User(config.Token)
	}
	q := u.Query()
	if config.Timeout > 0 {
		q.Set("timeout", config.Timeout.String())
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}
