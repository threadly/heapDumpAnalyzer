package org.threadly.heap.parser;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.threadly.concurrent.PriorityScheduler;

@SuppressWarnings("javadoc")
public class Parse {
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java -cp build/libs/heapDumpAnalyzer.java " + 
                            Parse.class.getName() + " dump.hprof");
      System.exit(1);
    }

    PriorityScheduler scheduler = new PriorityScheduler(Runtime.getRuntime().availableProcessors() * 2);
    DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(args[0])));
    try {
      HprofParser parser = new HprofParser(scheduler, new File(args[0]));
      parser.parse();
    } finally {
      in.close();
      scheduler.shutdown();
    }
  }
}
