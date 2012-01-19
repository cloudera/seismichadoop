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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.seismic.su.SUCallback;
import com.cloudera.seismic.su.SUProcess;
import com.cloudera.seismic.su.SUReader;
import com.cloudera.seismic.su.SequenceFileCallback;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Command-line utility for reading in SEG-Y formatted trace data and writing
 * the data into a block-compressed {@link SequenceFile} for processing by
 * Hadoop.
 *
 */
public class SegyLoader extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("cwproot", true, "The path to CWPROOT on this machine");
    options.addOption("input", true, "SEG-Y files to import into Hadoop");
    options.addOption("output", true, "The path of the sequence file to write in Hadoop");
    
    // Parse the commandline and check for required arguments.
    CommandLine cmdLine = new PosixParser().parse(options, args, false);
    if (!cmdLine.hasOption("input") || !cmdLine.hasOption("output")) {
      System.out.println("Mising required input/output arguments");
      new HelpFormatter().printHelp("SegyLoader", options);
      System.exit(1);
    }
    
    String cwproot = System.getenv("CWPROOT");
    if (cmdLine.hasOption("cwproot")) {
      cwproot = cmdLine.getOptionValue("cwproot");
    }
    if (cwproot == null || cwproot.isEmpty()) {
      System.out.println("Could not determine CWPROOT value, using /usr/local/su...");
      cwproot = "/usr/local/su";
    }
    
    // Assume any remaining args are for segyread
    List<String> segyReadArgs = Lists.newArrayList();
    for (String arg : cmdLine.getArgs()) {
      if (arg.contains("=")) {
        segyReadArgs.add(arg);
      }
    }
    
    // Open the output sequence file.
    Configuration conf = getConf();
    Path outputPath = new Path(cmdLine.getOptionValue("output"));
    SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(conf), conf, 
        outputPath, NullWritable.class, BytesWritable.class, CompressionType.BLOCK);
    int rc = 0;
    SequenceFileCallback sfc = new SequenceFileCallback(writer);
    try {
      for (String filename : cmdLine.getOptionValues("input")) {
        System.out.println("Reading input file: " + filename);
        if (filename.endsWith(".su")) {
          SUReader reader = new SUReader(new BufferedInputStream(new FileInputStream(filename)),
              ImmutableList.<SUCallback>of(sfc));
          reader.run();
          System.out.println("Bytes read: " + reader.getBytesRead());
        } else {
          SUProcess proc = new SUProcess(cwproot, "segyread");
          for (String arg : segyReadArgs) {
            proc.addArg(arg);
          }
          proc.addArg(String.format("tape=%s", filename));
          proc.addCallback(sfc);
          proc.start();
          rc += proc.closeAndWait();
          System.out.println("Bytes read: " + proc.getTotalBytesRead());
        }
      }
      System.out.println("Bytes written: " + sfc.getBytesWritten());
    } catch (Throwable t) {
      t.printStackTrace();
      rc = 1;
    } finally {
      writer.close();
    }
    return rc;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new SegyLoader(), args);
  }
}
