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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.TupleN;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.From;
import com.cloudera.crunch.io.To;
import com.cloudera.crunch.lib.PTables;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.crunch.type.writable.Writables;
import com.cloudera.seismic.su.SUProcess;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class SUPipeline extends Configured implements Tool {

  private static final Set<String> X_COMMANDS = ImmutableSet.of(
      "suxcontour", "suxgraph", "suximage", "suxmax", "suxmovie", "suxpicker", "suxwigb",
      "xcontour", "ximage", "xpicker", "xwigb");
  
  public PCollection<ByteBuffer> constructPipeline(PCollection<ByteBuffer> input, String cwproot, List<String> steps) {
    PTypeFamily ptf = input.getTypeFamily();
    PGroupedTable<TupleN, ByteBuffer> sorted = null;
    for (String step : steps) {
      String[] pieces = step.split("\\s+");
      if ("susort".equals(pieces[0])) {
        if (sorted != null) {
          throw new IllegalArgumentException("Cannot have susort followed by susort");
        } else {
          List<String> keys = Lists.newArrayList();
          for (int i = 1; i < pieces.length; i++) {
            if (!pieces[i].isEmpty()) {
              keys.add(pieces[i]);
            }
          }
          if (keys.isEmpty()) {
            throw new IllegalArgumentException("susort must have at least one key");
          }
          sorted = SUSort.apply(input, keys);
        }
      } else {
        SUProcess proc = new SUProcess(cwproot, pieces[0]);
        for (int i = 1; i < pieces.length; i++) {
          proc.addArg(pieces[i]);
        }
        if (sorted == null) {
          input = input.parallelDo(pieces[0], new SUDoFn(proc), ptf.bytes());
        } else {
          input = sorted.parallelDo(pieces[0], new SUPostGroupFn(proc), ptf.bytes());
          sorted = null;
        }
      }
    }
    if (sorted != null) {
      input = PTables.values(sorted.ungroup());
    }
    return input;
  }
  
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("cwproot", true, "The path to CWPROOT on the cluster machines");
    options.addOption("input", true, "SU files in Hadoop");
    options.addOption("output", true, "The path of the SU files to write out to Hadoop");
    options.addOption("command", true, "A pipeline of SU commands to run on the data");
    
    // Parse the commandline and check for required arguments.
    CommandLine cmdLine = new PosixParser().parse(options, args, false);
    if (!cmdLine.hasOption("input") || !cmdLine.hasOption("command")) {
      System.out.println("Mising required input/command arguments");
      new HelpFormatter().printHelp("SUPipeline", options);
      System.exit(1);
    }

    String clusterCwproot = null;
    if (cmdLine.hasOption("cwproot")) {
      clusterCwproot = cmdLine.getOptionValue("cwproot");
    }
    if (clusterCwproot == null || clusterCwproot.isEmpty()) {
      System.out.println("Could not determine cluster's CWPROOT value");
      new HelpFormatter().printHelp("SUPipeline", options);
      System.exit(1);
    }

    Pipeline pipeline = new MRPipeline(SUPipeline.class);
    PCollection<ByteBuffer> traces = pipeline.read(From.sequenceFile(
        cmdLine.getOptionValue("input"), Writables.bytes()));
    Pair<List<String>, String> cmd = parse(cmdLine.getOptionValue("command"));
    PCollection<ByteBuffer> result = constructPipeline(traces, clusterCwproot, cmd.first());
    
    if (cmdLine.hasOption("output")) {
      result.write(To.sequenceFile(cmdLine.getOptionValue("output")));
    }
    
    if (cmd.second() != null) {
      String localCwproot = System.getenv("CWPROOT");
      if (localCwproot == null) {
        System.out.println("To use local SU commands, the CWPROOT environment variable must be set");
        System.exit(1);
      }
      String[] pieces = cmd.second().split("\\s+");
      SUProcess x = new SUProcess(localCwproot, pieces[0]);
      for (int i = 1; i < pieces.length; i++) {
        x.addArg(pieces[i]);
      }
      x.addEnvironment(ImmutableMap.of("DISPLAY", System.getenv("DISPLAY")));
      Iterator<ByteBuffer> iter = result.materialize().iterator();
      x.start();
      while (iter.hasNext()) {
        ByteBuffer bb = iter.next();
        x.write(bb.array(), bb.arrayOffset(), bb.limit());
      }
      x.closeAndWait();
    }    
    
    if (!cmdLine.hasOption("output") && cmd.second() == null) {
      System.out.println("No output destination specified");
      System.exit(1);
    }
    
    pipeline.done();
    return 0;
  }
  
  private Pair<List<String>, String> parse(String command) {
    List<String> hCmds = Lists.newArrayList();
    String xCmd = null;
    for (String arg : command.toLowerCase().split("\\|\\s+")) {
      if (X_COMMANDS.contains(arg.split("\\s+")[0])) {
        xCmd = arg;
        break;
      } else {
        hCmds.add(arg);
      }
    }
    return Pair.of(hCmds, xCmd);
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SUPipeline(), args);
  }
}
