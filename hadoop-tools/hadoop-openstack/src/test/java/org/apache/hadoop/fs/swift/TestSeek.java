/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

/**
 * Seek tests verify that
 * <ol>
 *   <li>When you seek on a 0 byte file to byte (0), it's not an error.</li>
 *   <li>When you seek past the end of a file, it's an error that should
 *   raise -what- EOFException?</li>
 *   <li>when you seek forwards, you get new data</li>
 *   <li>when you seek backwards, you get the previous data</li>
 *   <li>That this works for big multi-MB files as well as small ones.</li>
 * </ol>
 * These may seem "obvious", but the more the input streams try to be clever
 * about offsets and buffering, the more likely it is that seek() will start
 * to get confused.
 */
public class TestSeek extends SwiftFileSystemBaseTest {
  protected static final Log LOG =
    LogFactory.getLog(TestSeek.class);

  private Path testPath;
  private Path smallSeekFile;
  private FSDataInputStream hFile;

  /**
   * Setup creates dirs under test/hadoop
   *
   * @throws Exception
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    //delete the test directory
    testPath = path("/test");
/*
    Path testSeekFile = new Path(testPath, "seekfile.txt");
    createFile(testSeekFile);
*/
    smallSeekFile = new Path(testPath, "seekfile.txt");
    byte[] block = SwiftTestUtils.dataset(256, 0, 255);
    //this file now has a simple rule: offset => value
    createFile(smallSeekFile, block);
  }

  @After
  public void cleanFile() {
    IOUtils.closeStream(hFile);
    hFile = null;
  }

  @Test
  public void testSeekZeroByteFile() throws Throwable {
    Path testEmptyFile = new Path(testPath, "empty");
    createEmptyFile(testEmptyFile);

    hFile = fs.open(testEmptyFile);
    assertEquals(0, hFile.getPos());
    //expect that seek to 0 works
    hFile.seek(0);
    int result = hFile.read();
    assertEquals(-1, result);
  }

  @Test
  public void testSeekClosedFile() throws Throwable {
    Path testEmptyFile = new Path(testPath, "empty");
    createEmptyFile(testEmptyFile);

    hFile = fs.open(testEmptyFile);
    hFile.seek(0);
    hFile.close();
    try {
      hFile.seek(0);
    } catch (IOException e) {
      //expected a closed file
    }
  }


  @Test
  public void testNegativeSeek() throws Throwable {
    hFile = fs.open(smallSeekFile);
    assertEquals(0, hFile.getPos());
    //expect that seek to 0 works
    try {
      hFile.seek(-1);
      long p = hFile.getPos();
      LOG.warn("Seek to -1 returned a position of " + p);
      int result = hFile.read();
      fail(
        "expected an exception, got data " + result + " at a position of " + p);
    } catch (IOException e) {
      //bad seek -expected
    }
    assertEquals(0, hFile.getPos());
  }

  @Test
  public void testSeekFile() throws Throwable {
    FSDataInputStream hFile = fs.open(smallSeekFile);
    assertEquals(0, hFile.getPos());
    //expect that seek to 0 works
    hFile.seek(0);
    int result = hFile.read();
    assertEquals(0, result);
    assertEquals(1, hFile.read());
    assertEquals(2, hFile.getPos());
    assertEquals(2, hFile.read());
    assertEquals(3, hFile.getPos());
    hFile.seek(128);
    assertEquals(128, hFile.getPos());
    assertEquals(128, hFile.read());
    hFile.seek(63);
    assertEquals(63, hFile.read());
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(SwiftProtocolConstants.SWIFT_REQUEST_SIZE, "1");
    return conf;
  }

  @Test
  public void testSeekBigFile() throws Throwable {
    Path testSeekFile = new Path(testPath, "bigseekfile.txt");
    byte[] block = SwiftTestUtils.dataset(65536, 0, 255);
    createFile(testSeekFile, block);
    FSDataInputStream hFile = fs.open(testSeekFile);
    assertEquals(0, hFile.getPos());
    //expect that seek to 0 works
    hFile.seek(0);
    int result = hFile.read();
    assertEquals(0, result);
    assertEquals(1, hFile.read());
    assertEquals(2, hFile.read());

    //do seek 32KB ahead
    hFile.seek(32768);
    assertEquals("@32768", block[32768], (byte) hFile.read());
    hFile.seek(40000);
    assertEquals("@40000", block[40000], (byte) hFile.read());
    hFile.seek(8191);
    assertEquals("@8191", block[8191], (byte) hFile.read());
    hFile.seek(0);
    assertEquals("@0", 0, (byte) hFile.read());
  }

  @Test
  public void testBulkReadDoesntChangePosition() throws Throwable {
    Path testSeekFile = new Path(testPath, "bigseekfile.txt");
    byte[] block = SwiftTestUtils.dataset(65536, 0, 255);
    createFile(testSeekFile, block);
    FSDataInputStream hFile = fs.open(testSeekFile);
    hFile.seek(40000);
    assertEquals(40000, hFile.getPos());
    byte[] readBuffer = new byte[256];
    hFile.read(128, readBuffer, 0, readBuffer.length);
    //have gone back
    assertEquals(40000, hFile.getPos());
    //content is the same too
    assertEquals("@40000", block[40000], (byte) hFile.read());
    //now verify the picked up data
    for (int i = 0; i < 256; i++) {
      assertEquals("@" + i, block[i + 128], readBuffer[i]);
    }
  }

  /**
   * work out the expected byte from a specific offset
   * @param offset offset in the file
   * @return the value
   */
  int expectedByte(int offset) {
    return offset & 0xff;
  }
}

