package org.threadly.heap.parser;

import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.future.FutureUtils;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.heap.parser.DataStructures.ArraySummary;
import org.threadly.heap.parser.DataStructures.ClassDefinition;
import org.threadly.heap.parser.DataStructures.ClassDefinition.ClassField;
import org.threadly.heap.parser.DataStructures.InstanceSummary;
import org.threadly.heap.parser.DataStructures.Instance;
import org.threadly.heap.parser.DataStructures.Summary;
import org.threadly.heap.parser.DataStructures.Type;
import org.threadly.heap.parser.DataStructures.Value;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

/**
 * <p>This class parses a binary format hprof heap dump file.  These files can be generated using 
 * "jmap", which is distributed with the JDK.  The structure of the file is documented in the 
 * src/share/demo/jvmti/hprof/hprof_b_spec.h source file.</p>
 * 
 * @author jent - Mike Jensen
 */
public class HprofParser {
  private static final boolean VERBOSE = false;
  private static final InstanceSummary UNKNOWN_CLASS = new InstanceSummary(-1, new HashMap<Long, ClassDefinition>(), "Unknown");
  
  // TODO - this limits to only one parser per VM
  private static int pointerSize = -1;
  
  protected static int getPointerSize() {
    return pointerSize;
  }
  
  private final SubmitterExecutor executor;
  private final File hprofFile;
  private final List<ListenableFuture<?>> parsingFutures;
  private final Map<Long, ClassDefinition> classMap;
  private final Map<Long, Instance> instances;
  private final Map<Long, String> stringMap;
  private final Map<Long, InstanceSummary> instanceSummary;
  private final Map<Long, ArraySummary> arraySummary;
  private final ArrayList<Instance> leafInstances;
  private DataInput mainIn;
  private long currentMainParsePosition = 0;
  /**
   * Constructs a new parser for a given file.
   * 
   * @param executor Executor that computation can be threaded out to
   * @param hprofFile File that should be parsed
   */
  public HprofParser(SubmitterExecutor executor, File hprofFile) {
    ArgumentVerifier.assertNotNull(hprofFile, "hprofFile");
    if (! hprofFile.exists()) {
      throw new IllegalArgumentException("File does not exist: " + hprofFile);
    } else if (! hprofFile.canRead()) {
      throw new IllegalArgumentException("Can not read file: " + hprofFile);
    }
    if (VERBOSE) {  // use single thread in verbose so the out makes sense
      this.executor = SameThreadSubmitterExecutor.instance();
    } else {
      this.executor = executor;
    }
    this.hprofFile = hprofFile;
    parsingFutures = Collections.synchronizedList(new ArrayList<ListenableFuture<?>>());
    classMap = Collections.synchronizedMap(new HashMap<Long, ClassDefinition>());
    instances = Collections.synchronizedMap(new HashMap<Long, Instance>());
    stringMap = new HashMap<>();
    instanceSummary = new HashMap<>();
    arraySummary = new HashMap<>();
    leafInstances = new ArrayList<>();
  }
  
  /**
   * Parse the file, this will block until the file has been fully parsed into memory.
   * 
   * @throws IOException Thrown if there is an error reading from the file
   */
  @SuppressWarnings("unused")
  public void parse() throws IOException {
    mainIn = new BufferedRandomAccessFile(hprofFile, "r");
    /* header:
     *   [u1]* - a null-terminated String of for the name and version
     *   u4 - size of pointers
     *   u8 - time in millis
     */
    String format = readString(mainIn);
    pointerSize = mainIn.readInt();
    long startTime = mainIn.readLong();
    
    currentMainParsePosition += format.getBytes().length + 1 + 12;
    
    while (parseNextRecord()) {
      // keep parsing
    }

    try {
      FutureUtils.blockTillAllCompleteOrFirstError(parsingFutures);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    } catch (ExecutionException e) {
      throw ExceptionUtils.makeRuntime(e.getCause());
    }
    parsingFutures.clear();
  }
  
  public void analyze() throws IOException {
    System.out.println("Done parsing file, now analyzing...");
    
    analyzeInstances();
    
    List<Summary> summaryList = new ArrayList<>();
    summaryList.addAll(instanceSummary.values());
    summaryList.addAll(arraySummary.values());
    Collections.sort(summaryList, (s1, s2) -> {
      return s2.getTotalBytesUsed() - s1.getTotalBytesUsed();
    });
    
    Iterator<Summary> it = summaryList.iterator();
    while (it.hasNext()) {
      Summary summary = it.next();
      if (summary.getInstanceCount() == 0 || summary.getTotalBytesUsed() < 1024) {
        continue;
      }
      System.out.println(summary.toString());
    }
    pointerSize = -1;
  }
  
  private void analyzeInstances() throws IOException {
    // right now calculating the summary count is the only processing done here
    Iterator<Instance> it = instances.values().iterator();
    while (it.hasNext()) {
      Instance i = it.next();
      BufferedRandomAccessFile raf = new BufferedRandomAccessFile(hprofFile, "r");
      try {
        raf.seek(i.valuesFilePos);
  
        ArrayList<Long> objectValues = new ArrayList<>();
        // superclass of Object has a pointer of 0
        long nextClass = i.instancePointer;
        while (nextClass != 0) {
          ClassDefinition ci = classMap.get(nextClass);
          if (ci != null) { // TODO - this should not happen, investigate
            nextClass = ci.superClassPointer;
            for (ClassField field: ci.fields) {
              if (field.type == Type.OBJECT) {
                objectValues.add(readPointer(getPointerSize(), raf));
              } else {
                // discard data
                skip(raf, field.type.getSizeInBytes());
              }
            }
          } else {
            nextClass = 0;
          }
        }
        if (objectValues.isEmpty()) {
          leafInstances.add(i);
        } else {
          Iterator<Long> childReferences = objectValues.iterator();
          while (childReferences.hasNext()) {
            instances.get(childReferences.next()).addParent(i);
          }
        }
        instanceSummary.get(i.classDef.classPointer).incrementInstanceCount();
      } finally {
        raf.close();
      }
    }
    // we can clear instances now since we will traverse from the leaf instances
    instances.clear();
    leafInstances.trimToSize();
    
    System.out.println("Leafs to start analysis from: " + leafInstances.size());
    List<ListenableFuture<?>> processingFutures = new ArrayList<>(leafInstances.size());
    it  = leafInstances.iterator();
    while (it.hasNext()) {
      Instance leaf = it.next();
      processingFutures.add(executor.submit(new Runnable() {
        @Override
        public void run() {
          traverseParents(leaf, 0);
        }
        
        private void traverseParents(Instance start, int retainedSize) {
          retainedSize += start.valuesLength;
          for (Instance p : leaf.getParentInstances()) {
            if (! p.traversed()) {
              System.out.println("Traversing: " + p.instancePointer + " - " + retainedSize);
              p.trackRetainedSize(retainedSize);
              instanceSummary.get(p.classDef.classPointer).incrementRetained(retainedSize);
              traverseParents(p, retainedSize);
            } else {
              System.out.println("alread traversed: " + p.instancePointer);
            }
          }
        }
      }));
    }
    try {
      FutureUtils.blockTillAllCompleteOrFirstError(processingFutures);
      // TODO - in theory we should have some results to display now
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    } catch (ExecutionException e) {
      throw ExceptionUtils.makeRuntime(e.getCause());
    }
  }
  
  private static String readString(DataInput in) throws IOException {
    int bytesRead = 0;
    byte[] bytes = new byte[32];
    
    byte readByte;
    while ((readByte = in.readByte()) != 0) {
      if (++bytesRead > bytes.length) {
        byte[] newBytes = new byte[bytesRead * 2];
        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
        bytes = newBytes;
      }
      bytes[bytesRead - 1] = readByte;
    }
    // intern string to reduce memory usage
    return new String(bytes, 0, bytesRead).intern();
  }

  /**
   * Parses the next record from the class {@link #mainIn} input.
   * 
   * @return true if there are no more records to parse
   */
  private boolean parseNextRecord() throws IOException {
    /* record format:
     *   u1 - type of record
     *   u4 - microseconds since header timestamp
     *   u4 - length of record in bytes
     *   [u1]* - body
     */
    
    byte tag;
    try {
      tag = mainIn.readByte();
    } catch (EOFException e) {
      // only EOF that would indicate a done, otherwise a partial file was provided
      return false;
    }
    long time = mainIn.readInt();
    long recordSize = Integer.toUnsignedLong(mainIn.readInt());
    currentMainParsePosition += 9;
    if (VERBOSE) {
      System.out.println("Record...Time: " + time + ", size: " + recordSize);
    }
    
    parseNextRecordBody(recordSize, tag);
    currentMainParsePosition += recordSize;
    return true;
  }
  
  /**
   * Parses the body for the next record from {@link #mainIn} input.
   */
  @SuppressWarnings("unused")
  private void parseNextRecordBody(long recordSize, byte tag) throws IOException {
    switch (tag) {
      case 0x1: {
        long pointer = readPointer();
        byte[] data = new byte[(int)(recordSize - getPointerSize())];
        mainIn.readFully(data);
        String str = new String(data).intern();
        stringMap.put(pointer, str);
        if (VERBOSE) {
          System.out.println("String: " + pointer + " = " + str);
        }
      } break;
      
      case 0x2: {
        int classIdentifier = mainIn.readInt();
        long classPointer = readPointer();
        int stackTraceIdentifier = mainIn.readInt();
        long classNameStringId = readPointer();
        instanceSummary.put(classPointer, new InstanceSummary(classPointer, classMap, 
                                                              stringMap.get(classNameStringId)));
        if (VERBOSE) {
          System.out.println("Load class: " + classIdentifier + " / " + classPointer);
        }
      } break;
      
      case 0x3: {
        int classIdentifier = mainIn.readInt();
        if (VERBOSE) {
          System.out.println("Unload class: " + classIdentifier);
        }
        // currently ignored
      } break;
      
      case 0x4: {
        long stackFrameId = readPointer();
        long methodNameStringId = readPointer();
        long methodSigStringId = readPointer();
        long sourceFileNameStringId = readPointer();
        int classIdentifier = mainIn.readInt();
        int location = mainIn.readInt();
        if (VERBOSE) {
          System.out.println("Stack frame: " + stackFrameId);
        }
        // stacktraces currently ignored
      } break;
      
      case 0x5: {
        int stackTraceIdentifier = mainIn.readInt();
        int threadIdentifier = mainIn.readInt();
        int numFrames = mainIn.readInt();
        long[] stackPointers = new long[(int)((recordSize - 12) / getPointerSize())];
        for (int i = 0; i < stackPointers.length; i++) {
          stackPointers[i] = readPointer();
        }
        if (VERBOSE) {
          System.out.println("Stacktrace: " + stackTraceIdentifier);
        }
        // stacktraces currently ignored
      } break;
      
      case 0x6: {
        short bitMaskFlags = mainIn.readShort();
        float cutoffRatio = mainIn.readFloat();
        int totalLiveBytes = mainIn.readInt();
        int totalLiveInstances = mainIn.readInt();
        long totalBytesAllocated = mainIn.readLong();
        long totalInstancesAllocated = mainIn.readLong();
        
        int allocSiteCount = mainIn.readInt();
        for (int i = 0; i < allocSiteCount; i++) {
          byte arrayIndicator = mainIn.readByte();
          int classIdentifier = mainIn.readInt();
          int stackTraceIdentifier = mainIn.readInt();
          int numLiveBytes = mainIn.readInt();
          int numLiveInstances = mainIn.readInt();
          int numBytesAllocated = mainIn.readInt();
          int numInstancesAllocated = mainIn.readInt();
        }
        if (VERBOSE) {
          System.out.println("Alloc site");
        }
        // currently ignored
      } break;
      
      case 0x7: {
        int totalLiveBytes = mainIn.readInt();
        int totalLiveInstances = mainIn.readInt();
        long totalBytesAllocated = mainIn.readLong();
        long totalInstancesAllocated = mainIn.readLong();
        if (VERBOSE) {
          System.out.println("Heap summary...Total live bytes: " + totalLiveBytes + 
                               ", Total bytes allocated: " + totalBytesAllocated +
                               ", Total live instances: " + totalLiveInstances + 
                               ", Total instances allocated: " + totalInstancesAllocated);
        }
        // TODO - provide heap summary statistics
      } break;
      
      case 0xa: {
        int threadIdentifier = mainIn.readInt();
        long threadObjectId = readPointer();
        int stackTraceIdentifier = mainIn.readInt();
        long threadNameStringId = readPointer();
        long threadGroupNameId = readPointer();
        long threadParentGroupNameId = readPointer();
        if (VERBOSE) {
          System.out.println("Start thread: " + threadIdentifier);
        }
        // TODO - record thread count statistics at a minimum
      } break;
      
      case 0xb: {
        int threadIdentifier = mainIn.readInt();
        if (VERBOSE) {
          System.out.println("End thread: " + threadIdentifier);
        }
      } break;
      
      case 0xc: {
        if (VERBOSE) {
          System.out.println("Heap dump");
        }
        new HeapDumpSegmentParser(getPointerSize(), recordSize, currentMainParsePosition, mainIn).run();
      } break;
      
      case 0x1c: {
        if (VERBOSE) {
          System.out.println("Heap dump segment");
        }
        @SuppressWarnings("resource")
        final BufferedRandomAccessFile raf = new BufferedRandomAccessFile(hprofFile, "r");
        raf.seek(currentMainParsePosition);
        HeapDumpSegmentParser hdsp = new HeapDumpSegmentParser(getPointerSize(), recordSize, currentMainParsePosition, raf);
        final ListenableFuture<?> future = executor.submit(hdsp);
        parsingFutures.add(future);
        future.addListener(() -> {
          try {
            raf.close();
          } catch (IOException e) {
            // ignored
          }
        });
        skip(mainIn, recordSize);
      } break;
      
      case 0x2c: {
        if (VERBOSE) {
          System.out.println("Heap dump segment end");
        }
      } break;
      
      case 0xd: {
        int totalNumOfSamples = mainIn.readInt();
        
        int cpuSampleCount = mainIn.readInt();
        for (int i = 0; i < cpuSampleCount; i++) {
          int numSamples = mainIn.readInt();
          int stackTraceIdentifier = mainIn.readInt();
        }
        // cpu samples currently ignored
        if (VERBOSE) {
          System.out.println("CPU Samples: " + totalNumOfSamples);
        }
      } break;
      
      case 0xe: {
        int bitMaskFlags = mainIn.readInt();
        short stackTraceDepth = mainIn.readShort();
        // control settings currently ignored
        if (VERBOSE) {
          System.out.println("Control settings");
        }
      } break;
      
      default:
        throw new UnsupportedOperationException("Unsupported top-level record type: " + tag);
    }
  }
  
  private static void skip(DataInput in, long recordSize) throws IOException {
    while (recordSize > 0) {
      int toSkipCount = (int)Math.min(recordSize, 1024 * 1024 * 1024);
      int skippedAmount = in.skipBytes(toSkipCount);
      if (skippedAmount == 0) {
        throw new IllegalStateException("Not advancing");
      }
      recordSize -= skippedAmount;
    }
  }
  
  private long readPointer() throws IOException {
    return readPointer(getPointerSize(), mainIn);
  }
  
  private static long readPointer(int pointerSize, DataInput in) throws IOException {
    if (pointerSize == 4) {
      return Integer.toUnsignedLong(in.readInt());
    } else if (pointerSize == 8) {
      return in.readLong();
    } else {
      throw new IllegalStateException("Invalid pointer size: " + pointerSize);
    }
  }
  
  private static Type getType(byte type) {
    switch (type) {
      case 2:
        return Type.OBJECT;
      case 4:
        return Type.BOOL;
      case 5:
        return Type.CHAR;
      case 6:
        return Type.FLOAT;
      case 7:
        return Type.DOUBLE;
      case 8:
        return Type.BYTE;
      case 9:
        return Type.SHORT;
      case 10:
        return Type.INT;
      case 11:
        return Type.LONG;
      default:
        throw new UnsupportedOperationException("Unsupported type in heap dump: " + type);
    }
  }
  
  private static Value<?> readValue(DataInput in, Type type) throws IOException {
    switch (type) {
      case OBJECT:
        return new Value<>(type, readPointer(getPointerSize(), in));
      case CHAR:
        return new Value<>(type, in.readChar());
      case LONG:
        return new Value<>(type, in.readLong());
      case INT:
        return new Value<>(type, in.readInt());
      case SHORT:
        return new Value<>(type, in.readShort());
      case BOOL:
        if (in.readBoolean()) {
          return new Value<>(type, Boolean.TRUE);
        } else {
          return new Value<>(type, Boolean.FALSE);
        }
      case BYTE:
        return  new Value<>(type, in.readByte());
      case DOUBLE:
        return new Value<>(type, in.readDouble());
      case FLOAT:
        return new Value<>(type, in.readFloat());
      default:
        throw new UnsupportedOperationException();
    }
  }
  
  private static Value<?>[] readValueArray(DataInput in, int size, Type type) throws IOException {
    Value<?>[] va = new Value<?>[size];
    switch (type) {
      case OBJECT:
        for(int i=0; i<size; i++) {
          va[i] = new Value<>(type, readPointer(getPointerSize(), in));
        } break;
      case CHAR:
        for(int i=0; i<size; i++) {
          va[i] = new Value<>(type, in.readChar());
        } break;
      case LONG:
        for(int i=0; i<size; i++) {
          va[i] = new Value<>(type, in.readLong());
        } break;
      case INT:
        for(int i=0; i<size; i++) {
          va[i] = new Value<>(type, in.readInt());
        } break;
      case SHORT:
        for(int i=0; i<size; i++) {
          va[i] = new Value<>(type, in.readShort());
        } break;
      case BOOL:
        for(int i=0; i<size; i++) {
          if (in.readBoolean()) {
            va[i] = new Value<>(type, Boolean.TRUE);
          } else {
            va[i] = new Value<>(type, Boolean.FALSE);
          }
        } break;
      case BYTE:
        for(int i=0; i<size; i++) {
          va[i] = new Value<>(type, in.readByte());
        } break;
      case DOUBLE:
        for(int i=0; i<size; i++) {
          va[i] = new Value<>(type, in.readDouble());
        } break;
      case FLOAT:
        for(int i=0; i<size; i++) {
          va[i] = new Value<>(type, in.readFloat());
        } break;
      default:
        throw new UnsupportedOperationException();
    }
    return va;
  }
  
  private class HeapDumpSegmentParser implements Runnable {
    private final int pointerSize;
    private final DataInput in;
    private long loopStartFilePos;
    private long bytesLeft;
    
    public HeapDumpSegmentParser(int pointerSize, long recordSize, long startingFilePos, DataInput in) {
      this.pointerSize = pointerSize;
      this.in = in;
      this.bytesLeft = recordSize;
      this.loopStartFilePos = startingFilePos;
    }
    
    private long readPointer() throws IOException {
      return HprofParser.readPointer(pointerSize, in);
    }
    
    @Override
    public void run() {
      try {
        doParse();
      } catch (Exception e) {
        throw ExceptionUtils.makeRuntime(e);
      }
    }
    
    @SuppressWarnings("unused")
    private void doParse() throws IOException {
      while (bytesLeft > 0) {
        final long bytesLeftAtStart = bytesLeft;
        if (VERBOSE) {
          System.out.println("Remaining bytes in dump: " + bytesLeft);
        }
        
        byte heapDumpTag = in.readByte();
        bytesLeft--;
        switch (heapDumpTag) {
          case (byte)0xFF: {
            long objectPointer = readPointer();
            bytesLeft -= pointerSize;
            if (VERBOSE) {
              System.out.println("Unkown root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x01: {
            long objectPointer = readPointer();
            long JNIGlobalRefId = readPointer();
            bytesLeft -= 2 * pointerSize;
            if (VERBOSE) {
              System.out.println("JNI global root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x02: {
            long objectPointer = readPointer();
            int threadIdentifier = in.readInt();
            int frameNum = in.readInt();
            bytesLeft -= pointerSize + 8;
            if (VERBOSE) {
              System.out.println("JNI local root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x03: {
            long objectPointer = readPointer();
            int threadIdentifier = in.readInt();
            int frameNum = in.readInt();
            bytesLeft -= pointerSize + 8;
            if (VERBOSE) {
              System.out.println("Java frame root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x04: {
            long objectPointer = readPointer();
            int threadIdentifier = in.readInt();
            bytesLeft -= pointerSize + 4;
            if (VERBOSE) {
              System.out.println("Native stack root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x05: {
            long objectPointer = readPointer();
            bytesLeft -= pointerSize;
            if (VERBOSE) {
              System.out.println("Stick class root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x06: {
            long objectPointer = readPointer();
            int threadIdentifier = in.readInt();
            bytesLeft -= pointerSize + 4;
            if (VERBOSE) {
              System.out.println("Thread block root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x07: {
            long objectPointer = readPointer();
            bytesLeft -= pointerSize;
            if (VERBOSE) {
              System.out.println("Monitor used root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x08: {
            long objectPointer = readPointer();
            int threadIdentifier = in.readInt();
            int stackTraceIdentifier = in.readInt();
            bytesLeft -= pointerSize + 8;
            if (VERBOSE) {
              System.out.println("Thread object root: " + objectPointer);
            }
            // currently ignored
          } break;
          
          case 0x20: {
            long objectPointer = readPointer();
            int stackTraceIdentifier = in.readInt();
            long superClassPointer = readPointer();
            long classLoaderPointer = readPointer();
            long signersPointer = readPointer();
            long protectionDomainPointer = readPointer();
            // skip two reserved pointers
            readPointer();
            readPointer();
            int instanceSize = in.readInt();
            bytesLeft -= (pointerSize * 7) + 8;
            
            short counstantCount = in.readShort();
            bytesLeft -= 2;
            for (short s = 0; s < counstantCount; s++) {
              short constantPoolIndex = in.readShort();
              byte btype = in.readByte();
              bytesLeft -= 3;
              Type type = getType(btype);
              Value<?> v = readValue(in, type);
              bytesLeft -= type.getSizeInBytes();
            }
            
            short staticCount = in.readShort();
            bytesLeft -= 2;
            for (int i = 0; i < staticCount; i++) {
              long staticFieldNameStringId = readPointer();
              byte btype = in.readByte();
              bytesLeft -= pointerSize + 1;
              Type type = getType(btype);
              Value<?> v = readValue(in, type);
              bytesLeft -= type.getSizeInBytes();
            }
            
            ClassField[] instanceFields = new ClassField[in.readShort()];
            bytesLeft -= 2;
            for (int i = 0; i < instanceFields.length; i++) {
              instanceFields[i] = new ClassField(stringMap.get(readPointer()), 
                                                 getType(in.readByte()));
              bytesLeft -= pointerSize + 1;
            }
            
            // TODO - improve thread access
            classMap.put(objectPointer, new ClassDefinition(objectPointer, superClassPointer, 
                                                            instanceSize, instanceFields));
            if (VERBOSE) {
              System.out.println("Class dump: " + objectPointer);
            }
          } break;
          
          case 0x21: {
            long instancePointer = readPointer();
            int stackTraceIdentifier = in.readInt();
            long classPointer = readPointer();
            int valuesLength = in.readInt();
            skip(in, valuesLength);
            bytesLeft -= (pointerSize * 2) + 8 + valuesLength;
            
            // TODO - improve thread access
            instances.put(instancePointer, new Instance(instancePointer, classMap.get(classPointer), 
                                                        loopStartFilePos + (pointerSize * 2) + 8, valuesLength));
            if (VERBOSE) {
              System.out.println("Instance dump: " + instancePointer);
            }
          } break;
          
          case 0x22: {
            long arrayPointer = readPointer();
            int stackTraceIdentifier = in.readInt();
            long[] objPointers = new long[in.readInt()];
            long elemClassPointer = readPointer();
            for (int i = 0; i < objPointers.length; i++) {
              objPointers[i] = readPointer();
            }
            bytesLeft -= ((2 + objPointers.length) * pointerSize) + 8;
            
            // TODO - improve thread access
            synchronized (arraySummary) {
              ArraySummary ai = arraySummary.get(elemClassPointer);
              if (ai == null) {
                InstanceSummary tmp = instanceSummary.get(elemClassPointer);
                if (tmp == null) {
                  ai = new ArraySummary(UNKNOWN_CLASS.className + "[]");
                  System.out.println("unknown class array:"+arrayPointer+":"+elemClassPointer);
                } else {
                  ai = new ArraySummary(tmp.getFormattedName() + "[]");
                }
                arraySummary.put(elemClassPointer, ai);
              }
              ai.addInstanceSize(objPointers.length * Type.OBJECT.getSizeInBytes());
            }
            if (VERBOSE) {
              System.out.println("Object array dump: " + arrayPointer);
            }
          } break;
          
          case 0x23: {
            long arrayPointer = readPointer();
            int stackTraceIdentifier = in.readInt();
            int arraySize = in.readInt();
            Value<?>[] elements;
            Type type = getType(in.readByte());
            bytesLeft -= pointerSize + 9;

            elements = readValueArray(in, arraySize, type);
            bytesLeft -= type.getSizeInBytes()*arraySize;
            
            // TODO - improve thread access
            synchronized (arraySummary) {
              ArraySummary ai = arraySummary.get(0L - type.ordinal());
              if (ai == null) {
                ai = new ArraySummary(type + "[]");
                arraySummary.put(0L - type.ordinal(), ai);
              }
              ai.addInstanceSize(elements.length * type.getSizeInBytes());
            }
            if (VERBOSE) {
              System.out.println("Primitive array dump: " + arrayPointer);
            }
          } break;
          
          default:
            throw new UnsupportedOperationException("Unsupported heap dump sub-record type: " + heapDumpTag);
        }
        
        loopStartFilePos += (bytesLeftAtStart - bytesLeft);
      }
    }
  }
}
