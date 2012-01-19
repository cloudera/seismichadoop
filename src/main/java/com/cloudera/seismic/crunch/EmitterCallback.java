/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.seismic.crunch;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.cloudera.crunch.Emitter;
import com.cloudera.seismic.su.SUCallback;

public class EmitterCallback implements SUCallback {

  private transient Emitter<ByteBuffer> emitter;
  
  public EmitterCallback(Emitter<ByteBuffer> emitter) {
    this.emitter = emitter;
  }
  
  @Override
  public void close() throws IOException {
    emitter.flush();
  }

  @Override
  public void initialize(TaskInputOutputContext context) throws IOException {
  }

  @Override
  public void write(byte[] data, int start, int length) throws IOException {
    emitter.emit(ByteBuffer.wrap(data, start, length));
  }

  @Override
  public void cleanup(TaskInputOutputContext context) throws IOException {
  }

}
