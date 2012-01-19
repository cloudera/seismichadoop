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

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;
import com.cloudera.seismic.su.SUCallback;
import com.cloudera.seismic.su.SUProcess;

public class SUDoFn extends DoFn<ByteBuffer, ByteBuffer> {
  
  private final SUProcess proc;
  
  private SUCallback emitterCallback;
  
  public SUDoFn(SUProcess proc) {
    this.proc = proc;
  }
  
  @Override
  public void initialize() {
    try {
      proc.start();
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }
  
  @Override
  public void process(ByteBuffer input, Emitter<ByteBuffer> emitter) {
    if (emitterCallback == null) {
      this.emitterCallback = new EmitterCallback(emitter);
      proc.addCallback(emitterCallback);
    }
    try {
      proc.write(input.array(), input.arrayOffset(), input.limit());
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }
  
  @Override
  public void cleanup(Emitter<ByteBuffer> emitter) {
    try {
      proc.closeAndWait();
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }
}
