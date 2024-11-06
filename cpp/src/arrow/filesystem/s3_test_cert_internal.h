// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

namespace arrow::fs {
// The below two static strings are generated according to
// https://github.com/minio/minio/tree/RELEASE.2024-09-22T00-33-43Z/docs/tls#323-generate-a-self-signed-certificate
// `openssl req -new -x509 -nodes -days 36500 -keyout private.key -out public.crt -config
// openssl.conf`
static constexpr const char* kMinioPrivateKey = R"(-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCqwKYHsTSciGqP
uU3qkTWpnXIi3iC0eeW7JSzJHGFs880WdR5JdK4WufPK+1xzgiYjMEPfAcuSWz3b
qYyCI61q+a9Iu2nj7cFTW9bfZrmWlnI0YOLJc+q0AAdAjF1lvRKenH8tbjz/2jyl
i/cYQ+I5Tg4nngrX8OmOfluNzwD/nwGLq6/DVbzDUdPI9q1XtVT/0Vf7qwbDG1HD
NkIzKT5B+YdSLaOCRYNK3x7RPsfazKIBrTmRy1v454wKe8TjTmTB7+m5wKqfCJcq
lI253WHcK0lsw6zCNtX/kahPAvm/8mniPolW4qxoD6xwebgMVkrNTs3ztcPIG9O4
pmCbATijAgMBAAECggEACL5swiAU7Z8etdVrZAOjl9f0LEzrp9JGLVst++50Hrwt
WGUO8/wBnjBPh6lvhoq3oT2rfBP/dLMva7w28cMZ8kxu6W6PcZiPOdGOI0qDXm69
0mjTtDU3Y5hMxsVpUvhnp6+j45Otk/x89o1ATgHL59tTZjv1mjFABIf78DsVdgF9
CMi2q6Lv7NLftieyWmz1K3p109z9+xkDNSOkVrv1JFChviKqWgIS0rdFjySvTgoy
rHYT+TweDliKJrZCeoUJmNB0uVW/dM9lXhcvkvkJZKPPurylx1oH5a7K/sWFPf7A
Ed1vjvZQFlaXu/bOUUSOZtkErAir/oCxrUDsHxGsAQKBgQDZghyy7jNGNdjZe1Xs
On1ZVgIS3Nt+OLGCVH7tTsfZsCOb+SkrhB1RQva3YzPMfgoZScI9+bN/pRVf49Pj
qGEHkW/wozutUve7UMzeTOm1aWxUuaKSrmYST7muvAnlYEtO7agd0wrcusYXlMoG
KQwghkufO9I7wXcrudMKXZalIwKBgQDI+FaUwhgfThkgq6bRbdMEeosgohrCM9Wm
E5JMePQq4VaGcgGveWUoNOgT8kvJa0qQwQOqLZj7kUIdj+SCRt0u+Wu3p5IMqdOq
6tMnLNQ3wzUC2KGFLSfISR3L/bo5Bo6Jqz4hVtjMk3PV9bu50MNTNaofYb2xlf/f
/WgiEG0WgQKBgAr8RVLMMQ7EvXUOg6Jwuc//Rg+J1BQl7OE2P0rhBbr66HGCPhAS
liB6j1dnzT/wxbXNQeA7clNqFRBIw3TmFjB5qfuvYt44KIbvZ8l6fPtKncwRrCJY
aJNYL3qhyKYrHOKZojoPZKcNT9/1BdcVz6T842jhbpbSCKDOu9f0Lh2dAoGATZeM
Hh0eISAPFY0QeDV1znnds3jC6g4HQ/q0dnAQnWmo9XmY6v3sr2xV2jWnSxnwjRjo
aFD4itBXfYBr0ly30wYbr6mz+s2q2oeVhL+LJAhrNDEdk4SOooaQSY0p1BCTAdYq
w8Z7J+kaRRZ+J0zRzROgHkOncKQgSYPWK6i55YECgYAC+ECrHhUlPsfusjKpFsEe
stW1HCt3wXtKQn6SJ6IAesbxwALZS6Da/ZC2x1mdBHS3GwWvtGLc0BPnPVfJjr9V
m82qkgJ+p5d7qp7pRA7SFD+5809yVqRnEF3rSLafgGet9ah0ZjZvQ3fwnYZNnNH9
t9pJcv2E5xY7/nFNIorpKg==
-----END PRIVATE KEY-----
)";

static constexpr const char* kMinioCert = R"(-----BEGIN CERTIFICATE-----
MIIDiTCCAnGgAwIBAgIUXbHZ6FAhKSXg4WSGUQySlSyE4U0wDQYJKoZIhvcNAQEL
BQAwXzELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAlZBMQ4wDAYDVQQHDAVBcnJvdzEO
MAwGA1UECgwFQXJyb3cxDjAMBgNVBAsMBUFycm93MRMwEQYDVQQDDApBcnJyb3dU
ZXN0MB4XDTI0MDkyNDA5MzUxNloXDTM0MDkyMjA5MzUxNlowXzELMAkGA1UEBhMC
VVMxCzAJBgNVBAgMAlZBMQ4wDAYDVQQHDAVBcnJvdzEOMAwGA1UECgwFQXJyb3cx
DjAMBgNVBAsMBUFycm93MRMwEQYDVQQDDApBcnJyb3dUZXN0MIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqsCmB7E0nIhqj7lN6pE1qZ1yIt4gtHnluyUs
yRxhbPPNFnUeSXSuFrnzyvtcc4ImIzBD3wHLkls926mMgiOtavmvSLtp4+3BU1vW
32a5lpZyNGDiyXPqtAAHQIxdZb0Snpx/LW48/9o8pYv3GEPiOU4OJ54K1/Dpjn5b
jc8A/58Bi6uvw1W8w1HTyPatV7VU/9FX+6sGwxtRwzZCMyk+QfmHUi2jgkWDSt8e
0T7H2syiAa05kctb+OeMCnvE405kwe/pucCqnwiXKpSNud1h3CtJbMOswjbV/5Go
TwL5v/Jp4j6JVuKsaA+scHm4DFZKzU7N87XDyBvTuKZgmwE4owIDAQABoz0wOzAa
BgNVHREEEzARhwR/AAABgglsb2NhbGhvc3QwHQYDVR0OBBYEFOUNqUSfROf1dz3o
hAVBhgd3UIvKMA0GCSqGSIb3DQEBCwUAA4IBAQBSwWJ2dSw3jlHU0l2V3ozqthTt
XFo07AyWGw8AWNCM6mQ+GKBf0JJ1d7e4lyTf2lCobknS94EgGPORWeiucKYAoCjS
dh1eKGsSevz1rNbp7wsO7DoiRPciK+S95DbsPowloGI6fvOeE12Cf1udeNIpEYWs
OBFwN0HxfYqdPALCtw7l0icpTrJ2Us06UfL9kbkdZwQhXvOscG7JDRtNjBxl9XNm
TFeMNKROmrEPCWaYr6MJ+ItHtb5Cawapea4THz9GCjR9eLq2CbMqLezZ8xBHPzc4
ixI2l0uCfg7ZUSA+90yaScc7bhEQ8CMiPtJgNKaKIqB58DpY7028xJpW7Ma2
-----END CERTIFICATE-----
)";
}  // namespace arrow::fs
