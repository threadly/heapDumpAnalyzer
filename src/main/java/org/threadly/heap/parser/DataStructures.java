package org.threadly.heap.parser;

import java.util.Arrays;
import java.util.Map;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>Class which just holds a collection of other classes representing data structures within the 
 * hprof file.</p>
 * 
 * @author jent - Mike Jensen
 */
public abstract class DataStructures {
  /**
   * <p>Enum which represents the different field types.</p>
   * 
   * @author jent - Mike Jensen
   */
  @SuppressWarnings("javadoc")
  public static enum Type {
    OBJECT("Object", -1), 
    BOOL("boolean", 1), 
    CHAR("char", 2), 
    FLOAT("float", 4), 
    DOUBLE("double", 8), 
    BYTE("byte", 1), 
    SHORT("short", 2), 
    INT("int", 4), 
    LONG("long", 8);
    
    private final String name;
    private final int sizeInBytes;
    
    private Type(String name, int sizeInBytes) {
      this.name = name;
      if (sizeInBytes < 0) {
        this.sizeInBytes = HprofParser.getPointerSize();
      } else {
        this.sizeInBytes = sizeInBytes;
      }
    }
    
    /**
     * Check how much space within the heap dump this type uses.
     * 
     * @return Number of bytes inside the heap dump used
     */
    public int getSizeInBytes() {
      return sizeInBytes;
    }
    
    @Override
    public String toString() {
      return name;
    }
  }
  
  /**
   * <p>Definition of a class within the heap.</p>
   * 
   * @author jent - Mike Jensen
   */
  @SuppressWarnings("javadoc")
  public static class ClassDefinition {
    public final long classPointer;
    public final long superClassPointer;
    public final int instanceSize;
    public final ClassField[] fields;
    
    public ClassDefinition(long classPointer, long superClassPointer, int instanceSize, 
                           ClassField[] fields) {
      this.classPointer = classPointer;
      this.superClassPointer = superClassPointer;
      this.instanceSize = instanceSize;
      this.fields = fields;
    }
    
    /**
     * <p>Field definition within a given class definition.</p>
     * 
     * @author jent - Mike Jensen
     */
    public static class ClassField {
      public final String name;
      public final Type type;
      
      public ClassField(String name, Type type) {
        this.name = name;
        this.type = type;
      }
    }
  }
  
  /**
   * <p>Structure representing an actual instance within the heap</p>
   * 
   * @author jent - Mike Jensen
   */
  @SuppressWarnings("javadoc")
  public static class Instance {
    public final long instancePointer;
    public final ClassDefinition classDef;
    public final long valuesFilePos;
    public final int valuesLength;
    private Instance[] parentReferences;
    
    public Instance(long instancePointer, 
                    ClassDefinition classDef, long valuesFilePos, int valuesLength) {
      this.instancePointer = instancePointer;
      this.classDef = classDef;
      this.valuesFilePos = valuesFilePos;
      this.valuesLength = valuesLength;
      parentReferences = new Instance[0];
    }

    public void addParent(Instance parent) {
      if (Arrays.stream(parentReferences).anyMatch(p -> {
        return p.equals(parent);
      })) {
        // don't add parent references if we already have one for this parent
        return;
      }
      
      // painful, but done to be memory conscious
      Instance[] newParentArray = new Instance[parentReferences.length + 1];
      System.arraycopy(parentReferences, 0, newParentArray, 0, parentReferences.length);
      newParentArray[parentReferences.length] = parent;
      parentReferences = newParentArray;
    }
    
    public Instance[] getParentInstances() {
      return parentReferences;
    }
  }
  
  /**
   * <p>Structure representing a stored value in the heap.</p>
   * 
   * @author jent - Mike Jensen
   */
  @SuppressWarnings("javadoc")
  public static class Value<T> {
    public final Type type;
    public final T value;
    
    public Value(Type type, T value) {
      this.value = value; 
      this.type = type;
    }
    
    @Override
    public String toString() {
      return value.toString();
    }
  }
  
  /**
   * <p>Abstract class for summary within a category of heap objects.</p>
   * 
   * @author jent - Mike Jensen
   */
  @SuppressWarnings("javadoc")
  public static abstract class Summary {
    public final String className;
    private int instanceCount = 0;
    
    protected Summary(String className) {
      this.className = className;
    }
    
    /**
     * Called to indicate that another instance of this class was detected within the heap dump.
     */
    public void incrementInstanceCount() {
      instanceCount++;
    }
    
    /**
     * Check how many instances were counted.  Instances are counted by invocations to 
     * {@link #incrementInstanceCount()}.
     * 
     * @return Count of instances on heap
     */
    public int getInstanceCount() {
      return instanceCount;
    }
    
    /**
     * Check how many bytes on the heap this instance occupied.
     * 
     * @return Number of bytes used in heap
     */
    public abstract int getTotalBytesUsed();
    
    @Override
    public String toString() {
      return className + " - " + String.format("%.2f", getTotalBytesUsed() / 1024.) + "KB, " + 
               instanceCount + " instances";
    }
  }
  /**
   * <p>Class which is a summary for all array instances of a given class.</p>
   * 
   * @author jent - Mike Jensen
   */
  @SuppressWarnings("javadoc")
  public static class ArraySummary extends Summary {
    private int totalSize = 0;
    
    public ArraySummary(String className) {
      super(className);
    }
    
    /**
     * Increment the size for an array instance of this class.  This implicitly calls 
     * {@link #incrementInstanceCount()}, since this is to indicate a new array size for a given 
     * instance.
     * 
     * @param size Size of array instance
     */
    public void addInstanceSize(int size) {
      ArgumentVerifier.assertNotNegative(size, "size");
      
      totalSize += size;
      incrementInstanceCount();
    }
    
    @Override
    public int getTotalBytesUsed() {
      return totalSize;
    }
  }
  
  /**
   * <p>Summary of instances for a given class on the heap.</p>
   * 
   * @author jent - Mike Jensen
   */
  @SuppressWarnings("javadoc")
  public static class InstanceSummary extends Summary {
    private final long classPointer;
    private final Map<Long, ClassDefinition> classMap;
    
    public InstanceSummary(long classPointer, Map<Long, ClassDefinition> classMap, 
                           String className) {
      super(className);
      this.classPointer = classPointer;
      this.classMap = classMap;
    }
    
    @Override
    public int getTotalBytesUsed() {
      return getInstanceCount() * classMap.get(classPointer).instanceSize;
    }
  }
}
