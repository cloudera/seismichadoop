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
package com.cloudera.seismic.segy;

import java.nio.ByteBuffer;

/**
 * The {@code FieldType} specifies how to encode/decode the bytes at an offset
 * in a SEG-Y trace header.
 * 
 * <p>The SEG-Y standard specifies three integer types for trace header fields:
 * a signed 16-bit integer, an unsigned 16-bit integer, and a signed 32-bit
 * integer. All three types are supported here, but clients only need to
 * work with Java ints.
 *
 */
public enum FieldType {
  INT16 {
    public void encode(ByteBuffer buffer, int offset, int value) {
      buffer.putShort(offset, (short) value);
    }
    public int decode(ByteBuffer buffer, int offset) {
      return buffer.getShort(offset);
    }
  },
  UINT16 {
    public void encode(ByteBuffer buffer, int offset, int value) {
      buffer.putChar(offset, (char) value);
    }
    public int decode(ByteBuffer buffer, int offset) {
      return buffer.getChar(offset);
    }
  },
  INT32 {
    public void encode(ByteBuffer buffer, int offset, int value) {
      buffer.putInt(offset, value);
    }
    public int decode(ByteBuffer buffer, int offset) {
      return buffer.getInt(offset);
    }
  };

  /**
   * Write the given value to the offset of the given buffer.
   * 
   * @param buffer The buffer to write.
   * @param offset The offset into the buffer to begin writing at.
   * @param value The value to write.
   */
  public abstract void encode(ByteBuffer buffer, int offset, int value);

  
  /**
   * Read the value of this type from the given offset into the buffer.
   * 
   * @param buffer The buffer to read from.
   * @param offset The offset into the buffer to read.
   * @return The value that was read.
   */
  public abstract int decode(ByteBuffer buffer, int offset);
}