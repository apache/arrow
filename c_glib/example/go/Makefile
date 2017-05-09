# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.

PROGRAMS =					\
	read-batch				\
	read-stream				\
	write-batch				\
	write-stream

all: build

generate:
	$$GOPATH/bin/gir-generator		\
	  -o $$GOPATH/src/gir/arrow-1.0		\
	  -config arrow-1.0/config.json		\
	  arrow-1.0/arrow.go.in

build: $(PROGRAMS)

clean:
	rm -f $(PROGRAMS)

.SUFFIXES: .go

.go:
	go build -o $@ $<
