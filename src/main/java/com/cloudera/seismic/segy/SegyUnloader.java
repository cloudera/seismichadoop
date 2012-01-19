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

import java.io.DataOutputStream;
import java.io.FileOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SegyUnloader extends Configured implements Tool {

  private void write(Path path, DataOutputStream out, Configuration conf) throws Exception {
    System.out.println("Reading: " + path);
    SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);
    BytesWritable value = new BytesWritable();
    while (reader.next(NullWritable.get(), value)) {
      out.write(value.getBytes(), 0, value.getLength());
    }
    reader.close();
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("input", true, "SU sequence files to export from Hadoop");
    options.addOption("output", true, "The local SU file to write");

    // Parse the commandline and check for required arguments.
    CommandLine cmdLine = new PosixParser().parse(options, args, false);
    if (!cmdLine.hasOption("input") || !cmdLine.hasOption("output")) {
      System.out.println("Mising required input/output arguments");
      new HelpFormatter().printHelp("SegyUnloader", options);
      System.exit(1);
    }

    Configuration conf = getConf();
    FileSystem hdfs = FileSystem.get(conf);
    Path inputPath = new Path(cmdLine.getOptionValue("input"));  
    if (!hdfs.exists(inputPath)) {
      System.out.println("Input path does not exist");
      System.exit(1);
    }
    
    PathFilter pf = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return !path.getName().startsWith("_");
      }
    };
    
    DataOutputStream os = new DataOutputStream(
        new FileOutputStream(cmdLine.getOptionValue("output")));    
    for (FileStatus fs : hdfs.listStatus(inputPath, pf)) {
      write(fs.getPath(), os, conf);
    }
    os.close();

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SegyUnloader(), args);
  }
}
