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
package org.apache.arrow.vector.complex.reader;

import org.apache.arrow.vector.complex.reader.BaseReader.ListReader;
import org.apache.arrow.vector.complex.reader.BaseReader.MapReader;
import org.apache.arrow.vector.complex.reader.BaseReader.RepeatedListReader;
import org.apache.arrow.vector.complex.reader.BaseReader.RepeatedMapReader;
import org.apache.arrow.vector.complex.reader.BaseReader.RepeatedStructReader;
import org.apache.arrow.vector.complex.reader.BaseReader.ScalarReader;
import org.apache.arrow.vector.complex.reader.BaseReader.StructReader;

/**
 * Composite of all Reader types (e.g. {@link StructReader}, {@link ScalarReader}, etc). Each reader
 * type is in essence a way of iterating over a {@link org.apache.arrow.vector.ValueVector}.
 */
public interface FieldReader
    extends StructReader,
        ListReader,
        MapReader,
        ScalarReader,
        RepeatedStructReader,
        RepeatedListReader,
        RepeatedMapReader {}
