package org.threadly.heap.parser;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * <p>A small class which allows us to buffer from a {@link RandomAccessFile}, and still 
 * implement the {@link java.io.DataInput}.</p>
 * 
 * @author jent - Mike Jensen
 */
public class BufferedRandomAccessFile extends DataInputStream {
  private final RandomAccessFile raf;

  public BufferedRandomAccessFile(File file, String ops) throws FileNotFoundException {
    this(new RandomAccessFile(file, ops));
  }
  
  @SuppressWarnings("resource")
  public BufferedRandomAccessFile(RandomAccessFile raf) {
    super(new BufferedInputStream(new RandomAccessFileInputStream(raf)));
    this.raf = raf;
  }
  
  @Override
  public void close() throws IOException {
    raf.close();
  }
  
  /**
   * Seeks to a given position, see {@link RandomAccessFile#seek(long)}.
   * 
   * @param position Position in the file to seek to
   * @throws IOException thrown if the internal RandomAccessFile throws
   */
  public void seek(long position) throws IOException {
    raf.seek(position);
  }
  
  private static class RandomAccessFileInputStream extends InputStream {
    private final RandomAccessFile raf;
    
    private RandomAccessFileInputStream(RandomAccessFile raf) {
      this.raf = raf;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return raf.read(b, off, len);
    }
    
    @Override
    public int read() throws IOException {
      return raf.read();
    }
  }
}
