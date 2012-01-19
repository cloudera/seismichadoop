package com.cloudera.seismic.su;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class SequenceFileCallback implements SUCallback {

  private final SequenceFile.Writer writer;
  private final BytesWritable value;
  private int bytesWritten = 0;
  
  public SequenceFileCallback(SequenceFile.Writer writer) {
    this.writer = writer;
    this.value = new BytesWritable();
  }
  
  @Override
  public void close() throws IOException {
    writer.close();
  }

  public int getBytesWritten() {
    return bytesWritten;
  }
  
  @Override
  public void write(byte[] data, int start, int length) throws IOException {
    value.set(data, start, length);
    writer.append(NullWritable.get(), value);
    bytesWritten += length;
  }

  @Override
  public void initialize(TaskInputOutputContext context) throws IOException {
    // No-op in this implementation
  }

  @Override
  public void cleanup(TaskInputOutputContext context) throws IOException {
    // No-op
  }

}
