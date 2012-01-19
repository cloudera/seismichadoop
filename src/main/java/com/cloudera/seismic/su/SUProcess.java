package com.cloudera.seismic.su;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.LineReader;

public class SUProcess implements SUCallback {

  private final String cwproot;
  private final List<String> command;
  private final Map<String, String> environment;
  private final List<SUCallback> callbacks;
  
  private transient Process process;
  private transient OutputStream outputStream;
  private transient OutputThread outputThread;
  private transient Thread errorThread;
  
  public SUProcess(String cwproot, String command) {
    this.cwproot = cwproot;
    this.command = new ArrayList<String>();
    this.command.add(cwproot + "/bin/" + command);
    this.environment = new HashMap<String, String>();
    this.callbacks = new ArrayList<SUCallback>();
  }
  
  public SUProcess addArg(String arg) {
    this.command.add(arg);
    return this;
  }
  
  public SUProcess addEnvironment(Map<String, String> environment) {
    this.environment.putAll(environment);
    return this;
  }
  
  public SUProcess addCallback(SUCallback callback) {
    this.callbacks.add(callback);
    return this;
  }
  
  @Override
  public void initialize(TaskInputOutputContext context) throws IOException {
    for (SUCallback callback : callbacks) {
      callback.initialize(context);
    }
    start();
  }

  @Override
  public void cleanup(TaskInputOutputContext context) throws IOException {
    closeAndWait();
    for (SUCallback callback : callbacks) {
      cleanup(context);
    }
  }
  
  public void start() throws IOException {
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.environment().putAll(environment);
    pb.environment().put("CWPROOT", cwproot);
    this.process = pb.start();

    this.outputStream = new BufferedOutputStream(process.getOutputStream());
    this.errorThread = new ErrorThread(process.getErrorStream());
    this.errorThread.start();
    
    this.outputThread = new OutputThread(new BufferedInputStream(process.getInputStream()),
        callbacks);
    this.outputThread.start();    
  }

  public int closeAndWait() throws IOException {
    int rc = 1;
    close();
    try { outputThread.join(); } catch (InterruptedException ignore) {}
    try { errorThread.join(); } catch (InterruptedException ignore) {}
    try { rc = process.waitFor(); } catch (InterruptedException ignore) {}
    process.destroy();
    return rc;
  }
  
  public int getTotalBytesRead() {
    return outputThread.getBytesRead();
  }
  
  @Override
  public void write(byte[] data, int start, int len) throws IOException {
    if (outputStream == null) {
      return;
    }
    try {
      outputStream.write(data, start, len);
    } catch (IOException e) {
      // A hack for the case when an SU process exits early.
      if ("Broken pipe".equals(e.getMessage())) {
        outputStream = null;
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (outputStream != null) {
      outputStream.flush();
      outputStream.close();
    }
  }
  
  private static class OutputThread extends Thread {
    private final SUReader reader;
    
    public OutputThread(InputStream stream, List<SUCallback> callbacks) {
      this.reader = new SUReader(stream, callbacks);
    }
    
    public int getBytesRead() {
      return reader.getBytesRead();
    }
    
    public void run() {
      reader.run();
    }
  }
  
  private static class ErrorThread extends Thread {
    private final InputStream stream;
    
    public ErrorThread(InputStream stream) {
      this.stream = stream;
    }
    
    public void run() {
      Text line = new Text();
      LineReader lineReader = new LineReader(stream);
      try {
        while (lineReader.readLine(line) > 0) {
          System.err.println(line);
        }
        lineReader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
