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

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

/**
 * Test creating files, overwrite options &c
 */
public abstract class AbstractRenameContractTest extends
                                                 AbstractFSContractTestBase {

  boolean rename(Path src, Path dst) throws IOException {
    return getFileSystem().rename(src, dst);
  }

  /**
   * Expect a rename exception to raise an exception
   * @param src source path
   * @param dst destination path
   * @throws IOException any exception raised during the rename operation
   */
  public void expectRenameToFault(Path src, Path dst) throws IOException {
    boolean renamed = rename(src, dst);
    //expected an exception
    String destDirLS = generateAndLogErrorListing(src, dst);
    fail("expected rename(" + src + ", " + dst + " ) to fail," +
         " got a result of " + renamed
         + " and a destination of " + destDirLS);
  }

  protected String generateAndLogErrorListing(Path src, Path dst) throws
                                                                  IOException {
    FileSystem fs = getFileSystem();
    getLog().error(
      "src dir " + ContractTestUtils.ls(fs, src.getParent()));
    String destDirLS = ContractTestUtils.ls(fs, dst.getParent());
    if (fs.isDirectory(dst)) {
      //include the dir into the listing
      destDirLS = destDirLS + "\n" + ContractTestUtils.ls(fs, dst);
    }
    return destDirLS;
  }

  @Test
  public void testRenameNewFileSameDir() throws Throwable {
    describe("rename a file into a new file in the same directory");
    Path path = path("testRenameNewFile");
    Path path2 = path("testRenameNewFile2");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, false);
    boolean rename = rename(path, path2);
    ContractTestUtils.verifyFileContents(getFileSystem(), path2, data);
    assertTrue("rename returned false though the contents were copied", rename);
  }

  @Test
  public void testRenameNonexistentFile() throws Throwable {
    describe("rename a file into a new file in the same directory");
    Path path = path("testRenameNonexistentFileSrc");
    Path path2 = path("testRenameNonexistentFileDest");
    mkdirs(path.getParent());
    try {
      expectRenameToFault(path, path2);
      fail("rename a missing file unexpectedly succeeded");
    } catch (FileNotFoundException e) {
      handleExpectedException(e);
    }
  }

  /**
   * Rename test -handles filesystems that will overwrite the destination
   * as well as those that do not (i.e. HDFS). 
   * @throws Throwable
   */
  @Test
  public void testRenameFileOverExistingFile() throws Throwable {
    describe("Verify renaming a file onto an existing file fails");
    Path path1 = path("source-256.txt");
    byte[] data1 = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path1, data1, data1.length, 1024, false);
    Path path2 = path("dest-512.txt");
    byte[] data2 = dataset(512, 'A', 'Z');
    writeDataset(getFileSystem(), path2, data2, data2.length, 1024, false);
    assertIsFile(path2);
    boolean expectOverwrite = !isSupported(SUPPORTS_OVERWRITE_ON_RENAME);
    if (expectOverwrite) {
      // the filesystem supports rename(file, file2) by overwriting file2
      
      boolean renamed = rename(path1, path2);
      assertTrue("Rename returned false", renamed);
      //now verify that the data has been overwritten
      ContractTestUtils.verifyFileContents(getFileSystem(), path2, data1);
    } else {
      try {
        // rename is rejected by returning 'false' or throwing an exception
        boolean renamed = rename(path1, path2);
        if (renamed) {
          //expected an exception
          String destDirLS = generateAndLogErrorListing(path1, path2);
          getLog().error("dest dir " + destDirLS);
          fail("expected rename(" + path1 + ", " + path2 + " ) to fail," +
               " but got success and destination of " + destDirLS);
        }
      } catch (FileAlreadyExistsException e) {
        handleExpectedException(e);
      }
      //verify that the destination file is as before
      ContractTestUtils.verifyFileContents(getFileSystem(), path2, data2);
    }
  }
  
  @Test
  public void testRenameDirIntoExistingDir() throws Throwable {
    describe("Verify renaming a dir into an existing dir puts it underneath"
             +" and leaves existing files alone");
    Path srcDir = path("source");
    Path path = new Path(srcDir, "source-256.txt");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024, false);
    Path destDir = path("dest");

    Path path2 = new Path(destDir, "dest-512.txt");
    byte[] data2 = dataset(512, 'A', 'Z');
    writeDataset(getFileSystem(), path2, data2, data2.length, 1024, false);
    assertIsFile(path2);

    boolean rename = rename(srcDir, destDir);
    Path renamedSrc = new Path(destDir, "source");
    assertIsFile(path2);
    assertIsDirectory(renamedSrc);
    ContractTestUtils.verifyFileContents(getFileSystem(), path2, data2);
    assertTrue("rename returned false though the contents were copied", rename);
  }

}
