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
package com.cloudera.seismic.su;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.cloudera.seismic.segy.Fields;

/**
 * Handler for reading SU data from a stream and passing it along
 * to one or more callbacks.
 */
public class SUReader implements Runnable {

  private final InputStream stream;
  private final List<SUCallback> callbacks;
  private int bytesRead = 0;
  
  public SUReader(InputStream stream, List<SUCallback> callbacks) {
    this.stream = stream;
    this.callbacks = callbacks;
  }

  public int getBytesRead() {
    return bytesRead;
  }
  
  @Override
  public void run() {
    int bytesInSample = 0;
    byte[] data = new byte[240];
    ByteBuffer headerBuffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    int read = 0;
    try {
      while (read != -1) {
        read = stream.read(data, 0, 240);
        if (read == -1) {
          break;
        }
        while (read < 240) {
          read += stream.read(data, read, 240 - read);
        }
        bytesRead += read;
        int samples = Fields.NUM_SAMPLES.read(headerBuffer);
        if (samples * 4 != bytesInSample) {
          bytesInSample = samples * 4;
          byte[] tmp = new byte[240 + bytesInSample];
          System.arraycopy(data, 0, tmp, 0, 240);
          data = tmp;
          headerBuffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        }
        read = 0;
        while (read < bytesInSample) {
          int thisRead = stream.read(data, 240 + read, bytesInSample - read);
          if (thisRead == -1) {
            throw new IOException("EOF reached unexpectedly in trace");
          }
          read += thisRead;
          bytesRead += thisRead;
        }
        for (SUCallback callback : callbacks) {
          callback.write(data, 0, 240 + bytesInSample);
        }
      }
      if (read != -1) {
        throw new IOException("Non-negative read exit");
      }
      stream.close();
    } catch (IOException e) {
      // Ruh-roh.
      throw new RuntimeException(e);
    }
  }
}
