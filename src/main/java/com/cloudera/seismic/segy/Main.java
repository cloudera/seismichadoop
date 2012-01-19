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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.seismic.crunch.SUPipeline;

public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Please speicfy a command: load|run|unload");
      System.exit(1);
    }
    Configuration conf = new Configuration();
    String cmd = args[0].toLowerCase();
    String[] remaining = new String[args.length - 1];
    System.arraycopy(args, 1, remaining, 0, args.length - 1);
    if ("load".equals(cmd)) {
      ToolRunner.run(conf, new SegyLoader(), remaining);
    } else if ("run".equals(cmd)) {
      ToolRunner.run(conf, new SUPipeline(), remaining); 
    } else if ("unload".equals(cmd)) {
      ToolRunner.run(conf, new SegyUnloader(), remaining);
    } else {
      System.err.println("Unrecognized command: " + cmd);
      System.exit(1);
    }
  }
}
