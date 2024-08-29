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

package org.apache.arrow.dataset;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class OrcWriteSupport {
  public static void writeTempFile(TypeDescription orcSchema, Path path, Integer[] values) throws IOException {
    Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(new Configuration()).setSchema(orcSchema));
    VectorizedRowBatch batch = orcSchema.createRowBatch();
    LongColumnVector longColumnVector = (LongColumnVector) batch.cols[0];
    for (int idx = 0; idx < values.length; idx++) {
      longColumnVector.vector[idx] = values[idx];
    }
    batch.size = values.length;
    writer.addRowBatch(batch);
    writer.close();
  }
}
