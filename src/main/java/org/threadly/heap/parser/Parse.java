package org.threadly.heap.parser;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.util.Clock;
import org.threadly.util.debug.Profiler;

@SuppressWarnings("javadoc")
public class Parse {
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java -cp build/libs/heapDumpAnalyzer.java " + 
                            Parse.class.getName() + " dump.hprof");
      System.exit(1);
    }
    Profiler pf = new Profiler(10);
    PriorityScheduler scheduler = new PriorityScheduler(Runtime.getRuntime().availableProcessors() * 2);
    long start = Clock.accurateForwardProgressingMillis();
    try {
      HprofParser parser = new HprofParser(scheduler, new File(args[0]));
      if(args.length >= 2) {
        pf.start();
      }
      parser.parse();
      System.out.println("ParseTime:"+(Clock.accurateForwardProgressingMillis()-start));
      System.out.println("---------------------");
      long at = Clock.accurateForwardProgressingMillis();
      parser.analyse();
      System.out.println("---------------------");
      System.out.println("AnalyseTime:"+(Clock.accurateForwardProgressingMillis()-at));
    } finally {
      if(args.length >= 2) {
        pf.stop();
        File f = File.createTempFile("profile", "out");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.write(pf.dump().getBytes());
        raf.close();
      }
      System.out.println("---------------------");
      System.out.println("TotalTime:"+(Clock.accurateForwardProgressingMillis()-start));
      System.gc();
      System.out.println("---------------------");
      System.out.println("Used Memory:" + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
      scheduler.shutdown();
    }
  }
}
