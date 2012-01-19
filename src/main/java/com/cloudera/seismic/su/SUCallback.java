package com.cloudera.seismic.su;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public interface SUCallback extends Serializable, Closeable {
  public void initialize(TaskInputOutputContext context) throws IOException;

  public void write(byte[] data, int start, int length) throws IOException;
  
  public void cleanup(TaskInputOutputContext context) throws IOException;
}
