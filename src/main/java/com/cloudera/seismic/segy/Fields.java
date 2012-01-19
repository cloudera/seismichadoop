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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Common {@link Field} values that occur in most SEG-Y files.
 */
public class Fields {
  public static final Field TRACE_SEQUENCE_LINE = new Field(0, FieldType.INT32);
  public static final Field TRACE_SEQUENCE_FILE = new Field(4, FieldType.INT32);
  public static final Field FIELD_RECORD = new Field(8, FieldType.INT32);
  public static final Field TRACE_NUMBER = new Field(12, FieldType.INT32);
  public static final Field ENERGY_SOURCE_POINT = new Field(16, FieldType.INT32);
  public static final Field CDP = new Field(20, FieldType.INT32);
  public static final Field CDP_TRACE = new Field(24, FieldType.INT32);
  public static final Field TRACE_IDENTIFICATION_CODE = new Field(28, FieldType.UINT16);
  public static final Field NUM_SUMMED_TRACE = new Field(30, FieldType.INT16);
  public static final Field NUM_STACKED_TRACES = new Field(32, FieldType.INT16);
  public static final Field DATA_USE = new Field(34, FieldType.INT16);
  public static final Field OFFSET = new Field(36, FieldType.INT32);
  public static final Field RECEIVER_GROUP_ELEVATION = new Field(40, FieldType.INT32);
  public static final Field SOURCE_SURFACE_ELEVATION = new Field(44, FieldType.INT32);
  public static final Field SOURCE_DEPTH = new Field(48, FieldType.INT32);
  public static final Field RECEIVER_DATUM_ELEVATION = new Field(52, FieldType.INT32);
  public static final Field SOURCE_DATUM_ELEVATION = new Field(56, FieldType.INT32);
  public static final Field SOURCE_WATER_DEPTH = new Field(60, FieldType.INT32);
  public static final Field GROUP_WATER_DEPTH = new Field(64, FieldType.INT32);
  public static final Field ELEVATION_SCALAR = new Field(68, FieldType.INT16);
  public static final Field SOURCE_GROUP_SCALAR = new Field(70, FieldType.INT16);
  public static final Field SOURCE_X = new Field(72, FieldType.INT32);
  public static final Field SOURCE_Y = new Field(76, FieldType.INT32);
  public static final Field GROUP_X = new Field(80, FieldType.INT32);
  public static final Field GROUP_Y = new Field(84, FieldType.INT32);
  public static final Field COORDINATE_UNITS = new Field(88, FieldType.INT16);
  public static final Field WEATHERING_VELOCITY = new Field(90, FieldType.INT16);
  public static final Field SUB_WEATHERING_VELOCITY = new Field(92, FieldType.INT16);
  public static final Field SOURCE_UPHOLE_TIME = new Field(94, FieldType.INT16);
  public static final Field GROUP_UPHOLE_TIME = new Field(96, FieldType.INT16);
  public static final Field SOURCE_STATIC_CORRECTION = new Field(98, FieldType.INT16);
  public static final Field GROUP_STATIC_CORRECTION = new Field(100, FieldType.INT16);
  public static final Field TOTAL_STATIC_APPLIED = new Field(102, FieldType.INT16);
  public static final Field LAG_TIME_A = new Field(104, FieldType.INT16);
  public static final Field LAG_TIME_B = new Field(106, FieldType.INT16);
  public static final Field DELAY_RECORDING_TIME = new Field(108, FieldType.INT16);
  public static final Field MUTE_TIME_START = new Field(110, FieldType.INT16);
  public static final Field MUTE_TIME_END = new Field(112, FieldType.INT16);
  public static final Field NUM_SAMPLES = new Field(114, FieldType.UINT16);
  public static final Field SAMPLE_INTERVAL = new Field(116, FieldType.UINT16);
  public static final Field COURSE = new Field(124, FieldType.INT16);
  public static final Field SPEED = new Field(126, FieldType.INT16);
  public static final Field YEAR = new Field(156, FieldType.INT16);
  public static final Field DAY_OF_YEAR = new Field(158, FieldType.INT16);
  public static final Field HOUR = new Field(160, FieldType.INT16);
  public static final Field MINUTE = new Field(162, FieldType.INT16);
  public static final Field SECOND = new Field(164, FieldType.INT16);
  public static final Field TIME_BASE_CODE = new Field(166, FieldType.INT16);
  
  private static Map<String, Field> SORT_FIELDS = ImmutableMap.<String, Field>builder()
      .put("ep", ENERGY_SOURCE_POINT)
      .put("cdp", CDP)
      .put("cdpt", CDP_TRACE)
      .put("sx", SOURCE_X)
      .put("sy", SOURCE_Y)
      .put("gx", GROUP_X)
      .put("gy", GROUP_Y)
      .put("offset", OFFSET)
      .build();
  
  public static Field getSortField(String shortName) {
    return SORT_FIELDS.get(shortName);
  }
  
  // Cannot be instantiated
  private Fields() {}
}
