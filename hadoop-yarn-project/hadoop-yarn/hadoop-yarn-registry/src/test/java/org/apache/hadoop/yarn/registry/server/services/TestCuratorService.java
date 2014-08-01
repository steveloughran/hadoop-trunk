/*
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

package org.apache.hadoop.yarn.registry.server.services;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.yarn.registry.AbstractZKRegistryTest;
import org.apache.hadoop.yarn.registry.client.exceptions.RESTIOException;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Test the curator service
 */
public class TestCuratorService extends AbstractZKRegistryTest {

  public static final String MISSING = "/missing";

  @Test
  public void testLs() throws Throwable {
    curatorService.ls("/");
  }

  @Test(expected = FileNotFoundException.class)
  public void testLsNotFound() throws Throwable {
    List<String> ls = curatorService.ls(MISSING);
  }

  @Test
  public void testExists() throws Throwable {
    assertTrue(curatorService.pathExists("/"));
  }

  @Test
  public void testExistsMissing() throws Throwable {
    assertFalse(curatorService.pathExists(MISSING));
  }

  @Test
  public void testVerifyExists() throws Throwable {
    curatorService.pathMustExist("/");
  }

  @Test(expected = FileNotFoundException.class)
  public void testVerifyExistsMissing() throws Throwable {
    curatorService.pathMustExist(MISSING);
  }

  @Test
  public void testMkdirs() throws Throwable {
    curatorService.mkdir("/p1", CreateMode.PERSISTENT);
    curatorService.pathMustExist("/p1");
    curatorService.mkdir("/p1/p2", CreateMode.EPHEMERAL);
    curatorService.pathMustExist("/p1/p2");
  }

  @Test(expected = FileNotFoundException.class)
  public void testMkdirChild() throws Throwable {
    curatorService.mkdir("/testMkdirChild/child", CreateMode.PERSISTENT);
  }

  @Test
  public void testMaybeCreate() throws Throwable {
    assertTrue(curatorService.maybeCreate("/p3", CreateMode.PERSISTENT));
    assertFalse(curatorService.maybeCreate("/p3", CreateMode.PERSISTENT));
  }

  @Test
  public void testRM() throws Throwable {
    curatorService.mkdir("/rm", CreateMode.PERSISTENT);
    curatorService.rm("/rm", false);
    verifyNotExists("/rm");
    curatorService.rm("/rm", false);
  }

  @Test
  public void testRMNonRf() throws Throwable {
    curatorService.mkdir("/rm", CreateMode.PERSISTENT);
    curatorService.mkdir("/rm/child", CreateMode.PERSISTENT);
    try {
      curatorService.rm("/rm", false);
      fail("expected a failure");
    } catch (RESTIOException expected) {

    }
  }

  @Test
  public void testRMNRf() throws Throwable {
    curatorService.mkdir("/rm", CreateMode.PERSISTENT);
    curatorService.mkdir("/rm/child", CreateMode.PERSISTENT);
    curatorService.rm("/rm", true);
    verifyNotExists("/rm");
    curatorService.rm("/rm", true);
  }

  @Test
  public void testCreate() throws Throwable {
    curatorService.create("/testCreate",
        CreateMode.PERSISTENT, getTestBuffer(),
        curatorService.getRootACL()
    );
    curatorService.pathMustExist("/testCreate");
  }

  @Test
  public void testCreateTwice() throws Throwable {
    byte[] buffer = getTestBuffer();
    curatorService.create("/testCreateTwice",
        CreateMode.PERSISTENT, buffer,
        curatorService.getRootACL()
    );
    try {
      curatorService.create("/testCreateTwice",
          CreateMode.PERSISTENT, buffer,
          curatorService.getRootACL()
      );
      fail();
    } catch (FileAlreadyExistsException e) {

    }
  }

  @Test
  public void testCreateUpdate() throws Throwable {
    byte[] buffer = getTestBuffer();
    curatorService.create("/testCreateUpdate",
        CreateMode.PERSISTENT, buffer,
        curatorService.getRootACL()
    );
    curatorService.update("/testCreateUpdate", buffer);
  }

  @Test(expected = FileNotFoundException.class)
  public void testUpdateMissing() throws Throwable {
    curatorService.update("/testUpdateMissing", getTestBuffer());
  }

  @Test
  public void testUpdateDirectory() throws Throwable {
    curatorService.mkdir("/testUpdateDirectory", CreateMode.PERSISTENT);
    curatorService.update("/testUpdateDirectory", getTestBuffer());
  }

  @Test
  public void testUpdateDirectorywithChild() throws Throwable {
    curatorService.mkdir("/testUpdateDirectorywithChild", CreateMode.PERSISTENT);
    curatorService.mkdir("/testUpdateDirectorywithChild/child",
        CreateMode.PERSISTENT);
    curatorService.update("/testUpdateDirectorywithChild", getTestBuffer());
  }


  protected byte[] getTestBuffer() {
    byte[] buffer = new byte[1];
    buffer[0] = '0';
    return buffer;
  }


  public void verifyNotExists(String path) throws IOException {
    if (curatorService.pathExists(path)) {
      fail("Path should not exist: " + path);
    }
  }
}
