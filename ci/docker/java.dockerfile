# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM maven:3.9.6-eclipse-temurin-22

COPY --from=maven:3.9.6-eclipse-temurin-8 /opt/java/openjdk /opt/java/openjdk8
COPY --from=maven:3.9.6-eclipse-temurin-11 /opt/java/openjdk /opt/java/openjdk11
COPY --from=maven:3.9.6-eclipse-temurin-17 /opt/java/openjdk /opt/java/openjdk17
COPY --from=maven:3.9.6-eclipse-temurin-21 /opt/java/openjdk /opt/java/openjdk21


env JAVA8_HOME /opt/java/openjdk8
env JAVA11_HOME /opt/java/openjdk11
env JAVA17_HOME /opt/java/openjdk17
env JAVA21_HOME /opt/java/openjdk21
env JAVA22_HOME /opt/java/openjdk

RUN find "$JAVA8_HOME/lib" "$JAVA11_HOME/lib" "$JAVA17_HOME/lib" "$JAVA21_HOME/lib" "$JAVA22_HOME/lib" -name '*.so' -exec dirname '{}' ';' | sort -u > /etc/ld.so.conf.d/docker-openjdk.conf; \
    ldconfig;

COPY ci/maven-toolchains.xml /usr/share/maven/conf/toolchains.xml

CMD ["mvn"]
