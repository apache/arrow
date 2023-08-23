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
package driver_test

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/arrow/go/v14/arrow/flight/flightsql/driver"
)

func TestConfigTLSRegistry(t *testing.T) {
	const cfgname = "bananarama"

	// Check if the 'skip-verify' entry exists
	expected := &tls.Config{InsecureSkipVerify: true}
	actual, found := driver.GetTLSConfig("skip-verify")
	require.True(t, found)
	require.EqualValues(t, expected, actual)

	// Make sure the testing entry does not exist
	_, found = driver.GetTLSConfig(cfgname)
	require.False(t, found)

	// Register a new expected config and check it contains the right config
	expected = &tls.Config{
		ServerName: "myserver.company.org",
		MinVersion: tls.VersionTLS12,
	}
	require.NoError(t, driver.RegisterTLSConfig(cfgname, expected))
	actual, found = driver.GetTLSConfig(cfgname)
	require.True(t, found)
	require.EqualValues(t, expected, actual)

	// Registering the config again will fail
	require.ErrorIs(t, driver.RegisterTLSConfig(cfgname, expected), driver.ErrRegistryEntryExists)

	// Unregister the config
	require.NoError(t, driver.UnregisterTLSConfig(cfgname))
	_, found = driver.GetTLSConfig(cfgname)
	require.False(t, found)

	// Unregistering a non-existing config fails
	require.ErrorIs(t, driver.UnregisterTLSConfig(cfgname), driver.ErrRegistryNoEntry)
}

func TestConfigFromDSNInvalid(t *testing.T) {
	testcases := []struct {
		name     string
		dsn      string
		expected string
	}{
		{
			name:     "empty config",
			expected: "invalid scheme",
		},
		{
			name:     "invalid url",
			dsn:      "flightsql://my host",
			expected: "invalid URL",
		},
		{
			name:     "invalid path",
			dsn:      "flightsql://127.0.0.1/someplace",
			expected: "unexpected path",
		},
		{
			name:     "invalid timeout",
			dsn:      "flightsql://127.0.0.1?timeout=2",
			expected: "missing unit in duration",
		},
		{
			name:     "multiple parameters (timeout)",
			dsn:      "flightsql://127.0.0.1:12345?timeout=123s&timeout=4s",
			expected: "too many values",
		},
		{
			name:     "multiple parameters (other)",
			dsn:      "flightsql://127.0.0.1:12345?foo=1&bar=true&foo=yes",
			expected: "too many values",
		},
		{
			name:     "TLS unregistered config",
			dsn:      "flightsql://127.0.0.1:12345?tls=mycfg",
			expected: "TLS entry not registered",
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := driver.NewDriverConfigFromDSN(tt.dsn)
			require.ErrorContains(t, err, tt.expected)
			require.Nil(t, actual)
		})
	}
}

func TestConfigFromDSN(t *testing.T) {
	// Register a custom TLS config for testing
	tlscfg := &tls.Config{
		ServerName: "myserver.company.org",
		MinVersion: tls.VersionTLS12,
	}
	require.NoError(t, driver.RegisterTLSConfig("mycfg", tlscfg))

	// Define the test-cases
	testcases := []struct {
		name     string
		dsn      string
		expected *driver.DriverConfig
	}{
		{
			name: "no authentication",
			dsn:  "flightsql://127.0.0.1:12345",
			expected: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Params:  make(map[string]string),
			},
		},
		{
			name: "username only authentication",
			dsn:  "flightsql://peter@127.0.0.1:12345",
			expected: &driver.DriverConfig{
				Address:  "127.0.0.1:12345",
				Username: "peter",
				Params:   make(map[string]string),
			},
		},
		{
			name: "username and password authentication",
			dsn:  "flightsql://peter:parker@127.0.0.1:12345",
			expected: &driver.DriverConfig{
				Address:  "127.0.0.1:12345",
				Username: "peter",
				Password: "parker",
				Params:   make(map[string]string),
			},
		},
		{
			name: "token authentication",
			dsn:  "flightsql://127.0.0.1:12345?token=012345abcde6789fgh",
			expected: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Token:   "012345abcde6789fgh",
				Params:  make(map[string]string),
			},
		},
		{
			name: "timeout",
			dsn:  "flightsql://127.0.0.1:12345?timeout=123s",
			expected: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Timeout: 123 * time.Second,
				Params:  make(map[string]string),
			},
		},
		{
			name: "custom parameters",
			dsn:  "flightsql://127.0.0.1:12345?timeout=200ms&database=mydb&pi=3.14",
			expected: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Timeout: 200 * time.Millisecond,
				Params: map[string]string{
					"database": "mydb",
					"pi":       "3.14",
				},
			},
		},
		{
			name: "TLS explicitly disabled",
			dsn:  "flightsql://127.0.0.1:12345?tls=disabled",
			expected: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Params:  make(map[string]string),
			},
		},
		{
			name: "TLS explicitly disabled (false)",
			dsn:  "flightsql://127.0.0.1:12345?tls=false",
			expected: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Params:  make(map[string]string),
			},
		},
		{
			name: "TLS system settings",
			dsn:  "flightsql://127.0.0.1:12345?tls=enabled",
			expected: &driver.DriverConfig{
				Address:    "127.0.0.1:12345",
				TLSEnabled: true,
				Params:     make(map[string]string),
			},
		},
		{
			name: "TLS system settings (true)",
			dsn:  "flightsql://127.0.0.1:12345?tls=true",
			expected: &driver.DriverConfig{
				Address:    "127.0.0.1:12345",
				TLSEnabled: true,
				Params:     make(map[string]string),
			},
		},
		{
			name: "TLS insecure skip-verify",
			dsn:  "flightsql://127.0.0.1:12345?tls=skip-verify",
			expected: &driver.DriverConfig{
				Address:       "127.0.0.1:12345",
				TLSEnabled:    true,
				TLSConfigName: "skip-verify",
				TLSConfig:     &tls.Config{InsecureSkipVerify: true},
				Params:        make(map[string]string),
			},
		},
		{
			name: "TLS custom config",
			dsn:  "flightsql://127.0.0.1:12345?tls=mycfg",
			expected: &driver.DriverConfig{
				Address:       "127.0.0.1:12345",
				TLSEnabled:    true,
				TLSConfigName: "mycfg",
				TLSConfig:     tlscfg,
				Params:        make(map[string]string),
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := driver.NewDriverConfigFromDSN(tt.dsn)
			require.NoError(t, err)
			require.EqualValues(t, tt.expected, actual)
		})
	}
}

func TestDSNFromConfig(t *testing.T) {
	// Define the test-cases
	testcases := []struct {
		name     string
		expected string
		drvcfg   *driver.DriverConfig
	}{
		{
			name:     "no authentication",
			expected: "flightsql://127.0.0.1:12345",
			drvcfg: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Params:  make(map[string]string),
			},
		},
		{
			name:     "username only authentication",
			expected: "flightsql://peter@127.0.0.1:12345",
			drvcfg: &driver.DriverConfig{
				Address:  "127.0.0.1:12345",
				Username: "peter",
				Params:   make(map[string]string),
			},
		},
		{
			name:     "username and password authentication",
			expected: "flightsql://peter:parker@127.0.0.1:12345",
			drvcfg: &driver.DriverConfig{
				Address:  "127.0.0.1:12345",
				Username: "peter",
				Password: "parker",
				Params:   make(map[string]string),
			},
		},
		{
			name:     "token authentication",
			expected: "flightsql://127.0.0.1:12345?token=012345abcde6789fgh",
			drvcfg: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Token:   "012345abcde6789fgh",
				Params:  make(map[string]string),
			},
		},
		{
			name:     "timeout",
			expected: "flightsql://127.0.0.1:12345?timeout=3s",
			drvcfg: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Timeout: 3 * time.Second,
				Params:  make(map[string]string),
			},
		},
		{
			name:     "custom parameters",
			expected: "flightsql://127.0.0.1:12345?database=mydb&pi=3.14&timeout=20ms",
			drvcfg: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Timeout: 20 * time.Millisecond,
				Params: map[string]string{
					"database": "mydb",
					"pi":       "3.14",
				},
			},
		},
		{
			name:     "TLS disabled",
			expected: "flightsql://127.0.0.1:12345",
			drvcfg: &driver.DriverConfig{
				Address: "127.0.0.1:12345",
				Params:  make(map[string]string),
			},
		},
		{
			name:     "TLS system settings",
			expected: "flightsql://127.0.0.1:12345?tls=enabled",
			drvcfg: &driver.DriverConfig{
				Address:    "127.0.0.1:12345",
				TLSEnabled: true,
				Params:     make(map[string]string),
			},
		},
		{
			name:     "TLS insecure skip-verify",
			expected: "flightsql://127.0.0.1:12345?tls=skip-verify",
			drvcfg: &driver.DriverConfig{
				Address:       "127.0.0.1:12345",
				TLSEnabled:    true,
				TLSConfigName: "skip-verify",
				TLSConfig:     &tls.Config{InsecureSkipVerify: true},
				Params:        make(map[string]string),
			},
		},
		{
			name:     "TLS disabled",
			expected: "flightsql://127.0.0.1:12345",
			drvcfg: &driver.DriverConfig{
				Address:       "127.0.0.1:12345",
				TLSEnabled:    false,
				TLSConfigName: "a random cfg",
				TLSConfig: &tls.Config{
					ServerName: "myserver.company.org",
					MinVersion: tls.VersionTLS12,
				},
				Params: make(map[string]string),
			},
		},
		{
			name:     "TLS custom config",
			expected: "flightsql://127.0.0.1:12345?tls=mycfg",
			drvcfg: &driver.DriverConfig{
				Address:       "127.0.0.1:12345",
				TLSEnabled:    true,
				TLSConfigName: "mycfg",
				TLSConfig: &tls.Config{
					ServerName: "myserver.company.org",
					MinVersion: tls.VersionTLS12,
				},
				Params: make(map[string]string),
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.drvcfg.DSN()
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestDSNFromConfigCustomTLS(t *testing.T) {
	expected := "flightsql://127.0.0.1:12345?tls=mycustomcfg"

	tlscfg := &tls.Config{
		ServerName: "myserver.company.org",
		MinVersion: tls.VersionTLS12,
	}

	drvcfg := &driver.DriverConfig{
		Address:       "127.0.0.1:12345",
		TLSEnabled:    true,
		TLSConfigName: "mycustomcfg",
		TLSConfig:     tlscfg,
		Params:        make(map[string]string),
	}

	require.Equal(t, expected, drvcfg.DSN())
	cfg, found := driver.GetTLSConfig("mycustomcfg")
	require.True(t, found)
	require.EqualValues(t, tlscfg, cfg)
}

func TestDSNFromConfigUnnamedCustomTLS(t *testing.T) {
	expected := "flightsql://127.0.0.1:12345?tls="

	tlscfg := &tls.Config{
		ServerName: "myserver.company.org",
		MinVersion: tls.VersionTLS12,
	}

	drvcfg := &driver.DriverConfig{
		Address:    "127.0.0.1:12345",
		TLSEnabled: true,
		TLSConfig:  tlscfg,
		Params:     make(map[string]string),
	}

	actual := drvcfg.DSN()
	require.NotEmpty(t, drvcfg.TLSConfigName)
	// Get the generated UUID and add it to the expected DSN
	expected += drvcfg.TLSConfigName
	require.Equal(t, expected, actual)
	cfg, found := driver.GetTLSConfig(drvcfg.TLSConfigName)
	require.True(t, found)
	require.EqualValues(t, tlscfg, cfg)
}
