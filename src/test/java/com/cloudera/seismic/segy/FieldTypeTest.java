/**
 * Copyright 2011 Cloudera, Inc. All Rights Reserved.
 */

package com.cloudera.seismic.segy;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

public class FieldTypeTest extends TestCase {

  private byte[] bytes = new byte[4];
  private ByteBuffer buffer = ByteBuffer.wrap(bytes);
  
  public void testUint16() throws Exception {
    FieldType.UINT16.encode(buffer, 0, 52007);
    assertEquals(52007, FieldType.UINT16.decode(buffer, 0));
  }
  
  public void testUint16Negative() throws Exception {
    FieldType.UINT16.encode(buffer, 0, -16);
    assertEquals(65520, FieldType.UINT16.decode(buffer, 0));
  }
  
  public void testInt16() throws Exception {
    FieldType.INT16.encode(buffer, 0, 52007);
    assertEquals(-13529, FieldType.INT16.decode(buffer, 0));
  }
  
  public void testInt16Negative() throws Exception {
    FieldType.INT16.encode(buffer, 0, -16);
    assertEquals(-16, FieldType.INT16.decode(buffer, 0));
  }
}
