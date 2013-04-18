/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Output stream, buffers data on local disk.
 * Writes to Swift on the close() method, unless the
 * file is significantly large that it is being written as partitions.
 * In this case, the first partition is written on the first write that puts
 * data over the partition, as may later writes. The close() then causes
 * the final partition to be written, along with a partition manifest.
 */
class SwiftNativeOutputStream extends OutputStream {
  private long filePartSize;
  private static final Log LOG =
          LogFactory.getLog(SwiftNativeOutputStream.class);
  private Configuration conf;
  private String key;
  private File backupFile;
  private OutputStream backupStream;
  private SwiftNativeFileSystemStore nativeStore;
  private boolean closed;
  private int partNumber;
  private long blockOffset;
  private boolean partUpload = false;

  /**
   * Create an output stream
   * @param conf configuration to use
   * @param nativeStore native store to write through
   * @param key the key to write
   * @param partSizeKB the partition size
   * @throws IOException
   */
  public SwiftNativeOutputStream(Configuration conf,
                                 SwiftNativeFileSystemStore nativeStore,
                                 String key,
                                 long partSizeKB) throws IOException {
    this.conf = conf;
    this.key = key;
    this.backupFile = newBackupFile();
    this.nativeStore = nativeStore;
    this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    this.partNumber = 1;
    this.blockOffset = 0;
    this.filePartSize = 1024L * partSizeKB;
  }

  private File newBackupFile() throws IOException {
    File dir = new File(conf.get("hadoop.tmp.dir"));
    if (!dir.mkdirs() && !dir.exists()) {
      throw new SwiftException("Cannot create Swift buffer directory: " + dir);
    }
    File result = File.createTempFile("output-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  /**
   * Flush the local backing stream.
   * This does not trigger a flush of data to the remote blobstore.
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    backupStream.flush();
  }

  /**
   * check that the output stream is open
   *
   * @throws SwiftException if it is not
   */
  private synchronized void verifyOpen() throws SwiftException {
    if (closed) {
      throw new SwiftException("Output stream is closed");
    }
  }

  /**
   * Close the stream. This will trigger the upload of all locally cached
   * data to the remote blobstore.
   * @throws IOException IO problems uploading the data.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      closed = true;
      //formally declare as closed.
      backupStream.close();
      Path keypath = new Path(key);
      if (partUpload) {
        partUpload();
        nativeStore.createManifestForPartUpload(keypath);
      } else {
        uploadOnClose(keypath);
      }
    } finally {
      delete(backupFile);
      backupStream = null;
      backupFile = null;
    }
  }

  /**
   * Upload a file when closed, either in one go, or, if the file is
   * already partitioned, by uploading the remaining partition and a manifest.
   * @param keypath key as a path
   * @throws IOException IO Problems
   */
  private void uploadOnClose(Path keypath) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Closing write of file %s;" +
                              " localfile=%s of length %d",
                              key,
                              backupFile,
                              backupFile.length()));
    }

    nativeStore.uploadFile(keypath,
                           new FileInputStream(backupFile),
                           backupFile.length());
  }

  @Override
  protected void finalize() throws Throwable {
    if(!closed) {
      LOG.warn("stream not closed");
    }
    if(backupFile!=null) {
      LOG.warn("Leaking backing file "+ backupFile);
    }
  }

  private void delete(File file) {
    if (!file.delete()) {
      LOG.warn("Could not delete " + file);
    }
  }

  @Override
  public void write(int b) throws IOException {
    verifyOpen();
    backupStream.write(b);
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    //validate args
    if (off < 0 || len < 0 || (off + len) > b.length) {
      throw new IndexOutOfBoundsException("Invalid offset/length for write");
    }
    verifyOpen();

    // if the size of file is greater than the partition limit
    if (blockOffset + len >= filePartSize) {
      // - then partition the blob and upload the first part
      // (this also sets blockOffset=0)
      partUpload();
    }

    //now update
    blockOffset += len;
    backupStream.write(b, off, len);
  }

  /**
   * Upload a single partition. This deletes the local backing-file,
   * and re-opens it to create a new one.
   * @throws IOException on IO problems
   */
  private void partUpload() throws IOException {
    partUpload = true;
    backupStream.close();
    if(LOG.isDebugEnabled()) {
      LOG.debug(String.format("Uploading part %d of file %s;" +
                              " localfile=%s of length %d",
                              partNumber,
                              key,
                              backupFile,
                              backupFile.length()));
    }
    nativeStore.uploadFilePart(new Path(key),
            partNumber,
            new FileInputStream(backupFile),
            backupFile.length());
    delete(backupFile);
    backupFile = newBackupFile();
    backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    blockOffset = 0;
    partNumber++;
  }

  /**
   * Get the file partition size
   * @return the partition size
   */
  long getFilePartSize() {
    return filePartSize;
  }

  /**
   * Query the number of partitions written
   * This is intended for testing
   * @return the of partitions already written to the remote FS
   */
  synchronized int getPartitionsWritten() {
    return partNumber - 1;
  }

  @Override
  public String toString() {
    return "SwiftNativeOutputStream{" +
           ", key='" + key + '\'' +
           ", backupFile=" + backupFile +
           ", closed=" + closed +
           ", filePartSize=" + filePartSize +
           ", partNumber=" + partNumber +
           ", blockOffset=" + blockOffset +
           ", partUpload=" + partUpload +
           ", nativeStore=" + nativeStore +
           '}';
  }
}