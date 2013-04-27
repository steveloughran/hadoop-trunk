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
package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.apache.hadoop.fs.swift.util.SwiftUtils;
import org.apache.hadoop.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.readDataset;


/**
 * Test partitioned uploads.
 * This is done by forcing a very small partition size and verifying that it
 * is picked up.
 */
public class TestSwiftFileSystemPartitionedUploads extends
                                                   SwiftFileSystemBaseTest {

  public static final String WRONG_PARTITION_COUNT =
    "wrong number of partitions written into ";
  public static final int PART_SIZE = 1;
  public static final int PART_SIZE_BYTES = PART_SIZE * 1024;
  public static final int BLOCK_SIZE = 1024;
  private URI uri;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    //set the partition size to 1 KB
    conf.setInt(SwiftProtocolConstants.SWIFT_PARTITION_SIZE, PART_SIZE);
    return conf;
  }

  @Test(timeout = SWIFT_BULK_IO_TEST_TIMEOUT)
  public void testPartitionPropertyPropagatesToConf() throws Throwable {
    assertEquals(1,
                 getConf().getInt(SwiftProtocolConstants.SWIFT_PARTITION_SIZE, 0));
  }

  @Test(timeout = SWIFT_BULK_IO_TEST_TIMEOUT)
  public void testPartionPropertyPropagatesToStore() throws Throwable {
    assertEquals(1, fs.getStore().getPartsizeKB());
  }

  /**
   * tests functionality for big files ( > 5Gb) upload
   */
  @Test(timeout = SWIFT_BULK_IO_TEST_TIMEOUT)
  public void testFilePartUpload() throws IOException, URISyntaxException {

    final Path path = new Path("/test/testFilePartUpload");

    int len = 8192;
    final byte[] src = SwiftTestUtils.dataset(len, 32, 144);
    FSDataOutputStream out = fs.create(path,
                               false,
                               getBufferSize(),
                               (short) 1,
                               BLOCK_SIZE);

    try {
      int totalPartitionsToWrite = len / PART_SIZE_BYTES;
      assertPartitionsWritten("Startup", out, 0);
      //write 2048
      int firstWriteLen = 2048;
      out.write(src, 0, firstWriteLen);
      //assert
      long expected = getExpectedPartitionsWritten(firstWriteLen,
                                                   PART_SIZE_BYTES,
                                                   false);
      SwiftUtils.debug(LOG,"First write: predict %d partitions written",expected);
      assertPartitionsWritten("First write completed", out, expected);
      //write the rest
      int remainder = len - firstWriteLen;
      SwiftUtils.debug(LOG, "remainder: writing: %d bytes",remainder);

      out.write(src, firstWriteLen, remainder);
      expected =
        getExpectedPartitionsWritten(len, PART_SIZE_BYTES, false);
      assertPartitionsWritten("Remaining data", out, expected);
      out.close();
      expected =
        getExpectedPartitionsWritten(len, PART_SIZE_BYTES, true);
      assertPartitionsWritten("Stream closed", out, expected);

      assertTrue("Exists", fs.exists(path));
      FileStatus status = fs.getFileStatus(path);
      assertEquals("Length of written file", len, status.getLen());
      String fileInfo = path + "  " + status;
      assertFalse("File claims to be a directory " + fileInfo,
                  status.isDir());
      byte[] dest = readDataset(fs, path, len);
      //compare data
      SwiftTestUtils.compareByteArrays(src, dest, len);

      //now see what block location info comes back.
      //This will vary depending on the Swift version, so the results
      //aren't checked -merely that the test actually worked
      BlockLocation[] locations = fs.getFileBlockLocations(status, 0, len);
      assertNotNull("Null getFileBlockLocations()", locations);
      assertTrue("empty array returned for getFileBlockLocations()",
                 locations.length > 0);
    } finally {
      IOUtils.closeStream(out);
    }
  }

  private int getExpectedPartitionsWritten(long uploaded,
                                            int partSizeBytes,
                                            boolean closed) {
    //#of partitions in total
    int partitions = (int) (uploaded / partSizeBytes);
    //#of bytes past the last partition
    int remainder = (int) (uploaded % partSizeBytes);
    if (closed) {
      //all data is written, so if there was any remainder, it went up
      //too
      return partitions + ((remainder > 0) ? 1 : 0);
    } else {
      //not closed. All the remainder is buffered,
      return partitions;
    }
  }

  private int getBufferSize() {
    return fs.getConf().getInt("io.file.buffer.size", 4096);
  }

  /**
   * Test sticks up a very large partitioned file and verifies that
   * it comes back unchanged. 
   * @throws Throwable
   */
  @Test(timeout = SWIFT_BULK_IO_TEST_TIMEOUT)
  public void testManyPartitionedFile() throws Throwable {
    final Path path = new Path("/test/testManyPartionedFile");

    int len = PART_SIZE_BYTES * 15;
    final byte[] src = SwiftTestUtils.dataset(len, 32, 144);
    FSDataOutputStream out = fs.create(path,
                                       false,
                                       getBufferSize(),
                                       (short) 1,
                                       BLOCK_SIZE);

    out.write(src, 0, src.length);
    int expected =
      getExpectedPartitionsWritten(len, PART_SIZE_BYTES, true);
    out.close();
    assertPartitionsWritten("write completed", out, expected);
    assertEquals("too few bytes written", len,
                 SwiftNativeFileSystem.getBytesWritten(out));
    assertEquals("too few bytes uploaded",len,
                 SwiftNativeFileSystem.getBytesUploaded(out));
    //now we verify that the data comes back. If it
    //doesn't, it means that the ordering of the partitions
    //isn't right
    byte[] dest = readDataset(fs, path, len);
    //compare data
    SwiftTestUtils.compareByteArrays(src, dest, len);
  }
  /**
   * Test that when a partitioned file is overwritten by a smaller one,
   * all the old partitioned files go away
   * @throws Throwable
   */
  @Ignore
  @Test(timeout = SWIFT_BULK_IO_TEST_TIMEOUT)
  public void testOverwritePartitionedFile() throws Throwable {
    final Path path = new Path("/test/testOverwritePartitionedFile");

    int len = 8192;
    final byte[] src = SwiftTestUtils.dataset(len, 32, 144);
    FSDataOutputStream out = fs.create(path,
                                       false,
                                       getBufferSize(),
                                       (short) 1,
                                       1024);
    out.write(src, 0, len);
    out.close();
    assertPartitionsWritten("", out, 3);

    assertTrue("Exists", fs.exists(path));
    FileStatus status = fs.getFileStatus(path);
    assertEquals("Length", len, status.getLen());
  }

}
