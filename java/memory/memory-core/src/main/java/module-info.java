/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module org.apache.arrow.memory.core {
  exports org.apache.arrow.memory;
  exports org.apache.arrow.memory.rounding;
  exports org.apache.arrow.memory.util;
  exports org.apache.arrow.memory.util.hash;
  exports org.apache.arrow.util;

  requires java.compiler;
  requires transitive jdk.unsupported;
  requires jsr305;
  requires static org.checkerframework.checker.qual;
  requires static org.immutables.value.annotations;
  requires org.slf4j;
}
