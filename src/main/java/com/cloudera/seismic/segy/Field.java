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

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * A {@code Field} describes a value in a trace header file in terms of a byte
 * offset and a {@link FieldType} that specifies how to interpret the data at
 * that offset.
 *
 * <p>SEG-Y trace headers are only loosely standardized, so clients can create
 * {@code Field} instances as necessary for the SEG-Y files they are working
 * with.
 */
public class Field implements Serializable {    
  private final int offset;
  private final FieldType type;
  
  public Field(int offset, FieldType type) {
    this.offset = offset;
    this.type = type;
  }
  
  public int read(ByteBuffer buffer) {
    return type.decode(buffer, offset);
  }
  
  public void write(ByteBuffer buffer, int value) {
    type.encode(buffer, offset, value);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Offset: ");
    sb.append(offset).append(" Type: ").append(type);
    return sb.toString();
  }
}