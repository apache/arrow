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

package org.apache.arrow.flight.sql.util;

import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.ProtocolMessageEnum;

enum AdhocTestOption implements ProtocolMessageEnum {
  OPTION_A, OPTION_B, OPTION_C;

  @Override
  public int getNumber() {
    return ordinal();
  }

  @Override
  public EnumValueDescriptor getValueDescriptor() {
    throw getUnsupportedException();
  }

  @Override
  public EnumDescriptor getDescriptorForType() {
    throw getUnsupportedException();
  }

  private UnsupportedOperationException getUnsupportedException() {
    return new UnsupportedOperationException("Unimplemented method is irrelevant for the scope of this test.");
  }
}
