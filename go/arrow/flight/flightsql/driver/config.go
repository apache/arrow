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
	"time"
)

type DriverConfig struct {
	Address  string
	Username string
	Password string
	Token    string
	Timeout  time.Duration
	Params   map[string]string

	TLSEnabled bool
	TLSConfig  *tls.Config
}

func NewDriverConfigFromDSN(dsn string) (*DriverConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
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
	for k, v := range config.Params {
		values.Add(k, v)
	}

	// Check if we do have parameters at all and set them
	if len(values) > 0 {
		u.RawQuery = values.Encode()
	}

	return u.String()
}
