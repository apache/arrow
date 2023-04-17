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
package driver

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TLS configuration registry
var (
	tlsConfigRegistry = map[string]*tls.Config{
		"skip-verify": {InsecureSkipVerify: true},
	}
	tlsRegistryMutex sync.Mutex
)

func RegisterTLSConfig(name string, cfg *tls.Config) error {
	tlsRegistryMutex.Lock()
	defer tlsRegistryMutex.Unlock()

	// Prevent name collisions
	if _, found := tlsConfigRegistry[name]; found {
		return ErrRegistryEntryExists
	}
	tlsConfigRegistry[name] = cfg

	return nil
}

func UnregisterTLSConfig(name string) error {
	tlsRegistryMutex.Lock()
	defer tlsRegistryMutex.Unlock()

	if _, found := tlsConfigRegistry[name]; !found {
		return ErrRegistryNoEntry
	}

	delete(tlsConfigRegistry, name)
	return nil
}

func GetTLSConfig(name string) (*tls.Config, bool) {
	tlsRegistryMutex.Lock()
	defer tlsRegistryMutex.Unlock()

	cfg, found := tlsConfigRegistry[name]
	return cfg, found
}

type DriverConfig struct {
	Address  string
	Username string
	Password string
	Token    string
	Timeout  time.Duration
	Params   map[string]string

	TLSEnabled    bool
	TLSConfigName string
	TLSConfig     *tls.Config
}

func NewDriverConfigFromDSN(dsn string) (*DriverConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	// Sanity checks on the given connection string
	if u.Scheme != "flightsql" {
		return nil, fmt.Errorf("invalid scheme %q", u.Scheme)
	}
	if u.Path != "" {
		return nil, fmt.Errorf("unexpected path %q", u.Path)
	}

	// Extract the settings
	var username, password string
	if u.User != nil {
		username = u.User.Username()
		if v, set := u.User.Password(); set {
			password = v
		}
	}

	config := &DriverConfig{
		Address:  u.Host,
		Username: username,
		Password: password,
		Params:   make(map[string]string),
	}

	// Determine the parameters
	for key, values := range u.Query() {
		// We only support single instances
		if len(values) > 1 {
			return nil, fmt.Errorf("too many values for %q", key)
		}
		var v string
		if len(values) > 0 {
			v = values[0]
		}

		switch key {
		case "token":
			config.Token = v
		case "timeout":
			config.Timeout, err = time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
		case "tls":
			switch v {
			case "true", "enabled":
				config.TLSEnabled = true
			case "false", "disabled":
				config.TLSEnabled = false
			default:
				config.TLSEnabled = true
				config.TLSConfigName = v
				cfg, found := GetTLSConfig(config.TLSConfigName)
				if !found {
					return nil, fmt.Errorf("%q TLS %w", config.TLSConfigName, ErrRegistryNoEntry)
				}
				config.TLSConfig = cfg
			}
		default:
			config.Params[key] = v
		}
	}

	return config, nil
}

func (config *DriverConfig) DSN() string {
	u := url.URL{
		Scheme: "flightsql",
		Host:   config.Address,
	}
	if config.Username != "" {
		if config.Password == "" {
			u.User = url.User(config.Username)
		} else {
			u.User = url.UserPassword(config.Username, config.Password)
		}
	}

	// Set the parameters
	values := url.Values{}
	if config.Token != "" {
		values.Add("token", config.Token)
	}
	if config.Timeout > 0 {
		values.Add("timeout", config.Timeout.String())
	}
	if config.TLSEnabled {
		switch config.TLSConfigName {
		case "skip-verify":
			values.Add("tls", "skip-verify")
		case "":
			// Use system defaults if no config is given
			if config.TLSConfig == nil {
				values.Add("tls", "enabled")
				break
			}
			// We got a custom TLS configuration but no name, create a unique one
			config.TLSConfigName = uuid.NewString()
			fallthrough
		default:
			values.Add("tls", config.TLSConfigName)
			if config.TLSConfig != nil {
				// Ignore the returned error as we do not care if the config
				// was registered before. If this fails and the config is not
				// yet registered, the driver will error out when parsing the
				// DSN.
				_ = RegisterTLSConfig(config.TLSConfigName, config.TLSConfig)
			}
		}
	}
	for k, v := range config.Params {
		values.Add(k, v)
	}

	// Check if we do have parameters at all and set them
	if len(values) > 0 {
		u.RawQuery = values.Encode()
	}

	return u.String()
}
