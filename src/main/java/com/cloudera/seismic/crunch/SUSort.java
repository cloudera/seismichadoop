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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.TupleN;
import com.cloudera.crunch.lib.JoinUtils;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.seismic.segy.Field;
import com.cloudera.seismic.segy.Fields;

public class SUSort {

  private static class HeaderExtractor extends MapFn<ByteBuffer, Pair<TupleN, ByteBuffer>> {
    private final Field[] fields;
    private final boolean[] negate;
    private final Object[] headerValues;
    
    public HeaderExtractor(Field[] fields, boolean[] negate) {
      this.fields = fields;
      this.negate = negate;
      this.headerValues = new Integer[fields.length];
    }
    
    @Override
    public Pair<TupleN, ByteBuffer> map(ByteBuffer input) {
      input.order(ByteOrder.BIG_ENDIAN);
      for (int i = 0; i < headerValues.length; i++) {
        int v = fields[i].read(input);
        headerValues[i] = negate[i] ? -v : v;
      }
      int x = input.array().length;
      return Pair.of(new TupleN(headerValues), input);
    } 
  }
  
  public static PGroupedTable<TupleN, ByteBuffer> apply(PCollection<ByteBuffer> traces, List<String> keys) {
    Field[] fields = new Field[keys.size()];
    boolean[] negate = new boolean[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      String key = keys.get(i);
      if (key.charAt(0) == '-') {
        negate[i] = true;
        key = key.substring(1);
      }
      fields[i] = Fields.getSortField(key);
      if (fields[i] == null) {
        throw new IllegalArgumentException("Unrecognized susort key: " + keys.get(i));
      }
    }
    
    PTypeFamily tf = traces.getTypeFamily();
    PType[] headerTypes = new PType[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      headerTypes[i] = tf.ints();
    }
    
    GroupingOptions options = GroupingOptions.builder()
        .partitionerClass(JoinUtils.getPartitionerClass(tf)).build();
    return traces.parallelDo("gethw", new HeaderExtractor(fields, negate),
          tf.tableOf(tf.tuples(headerTypes), tf.bytes())).groupByKey(options);
  }
}
