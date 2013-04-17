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
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
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
  private URI uri;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    //set the partition size to 1 KB
    conf.setInt(SwiftProtocolConstants.SWIFT_PARTITION_SIZE, 1);
    return conf;
  }

  protected int getPartitionsWritten(FSDataOutputStream out) {
    return SwiftNativeFileSystem.getPartitionsWritten(out);
  }

  
@Test
  public void testPartitionPropertyPropagatesToConf() throws Throwable {
    assertEquals(1,
                 getConf().getInt(SwiftProtocolConstants.SWIFT_PARTITION_SIZE, 0));
  }
 

  @Test
  public void testPartionPropertyPropagatesToStore() throws Throwable {
    assertEquals(1, fs.getStore().getPartsizeKB());
  }
 
  
  /**
   * tests functionality for big files ( > 5Gb) upload
   */
  @Test
  public void testFilePartUpload() throws IOException, URISyntaxException {

    final Path path = new Path("/test/huge-file");

    int len = 8192;
    final byte[] src = SwiftTestUtils.dataset(len, 32, 144);
    FSDataOutputStream out = fs.create(path,
                               false,
                               fs.getConf().getInt("io.file.buffer.size", 4096),
                               (short) 1,
                               1024);
    
    assertPartitionsWritten(out, 0);
    //write first half
    out.write(src, 0, len / 2);
    assertPartitionsWritten(out, 1);
    //write second half
    out.write(src, len / 2, len / 2);
    assertPartitionsWritten(out, 2);
    out.close();
    assertPartitionsWritten(out, 3);

    assertTrue("Exists", fs.exists(path));
    FileStatus status = fs.getFileStatus(path);
    assertEquals("Length", len, status.getLen());


    byte[] dest = readDataset(fs, path, len);
    SwiftTestUtils.compareByteArrays(src, dest, len);

    //now see what block location info comes back. 
    //This will vary depending on the Swift version, so the results
    //aren't checked -merely that the test actually worked
    BlockLocation[] locations = fs.getFileBlockLocations(status, 0, len);
    assertNotNull("Null getFileBlockLocations()", locations);
    assertTrue("empty array returned for getFileBlockLocations()",
               locations.length > 0);
  }

  private void assertPartitionsWritten(FSDataOutputStream out, int expected) {
    OutputStream nativeStream = out.getWrappedStream();
    assertEquals(WRONG_PARTITION_COUNT + nativeStream ,
                 expected, getPartitionsWritten(out));
  }


}
