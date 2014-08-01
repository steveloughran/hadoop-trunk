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
    registry.ls("/");
  }

  @Test(expected = FileNotFoundException.class)
  public void testLsNotFound() throws Throwable {
    List<String> ls = registry.ls(MISSING);
  }

  @Test
  public void testExists() throws Throwable {
    assertTrue(registry.pathExists("/"));
  }

  @Test
  public void testExistsMissing() throws Throwable {
    assertFalse(registry.pathExists(MISSING));
  }

  @Test
  public void testVerifyExists() throws Throwable {
    registry.pathMustExist("/");
  }

  @Test(expected = FileNotFoundException.class)
  public void testVerifyExistsMissing() throws Throwable {
    registry.pathMustExist(MISSING);
  }

  @Test
  public void testMkdirs() throws Throwable {
    registry.mkdir("/p1", CreateMode.PERSISTENT);
    registry.pathMustExist("/p1");
    registry.mkdir("/p1/p2", CreateMode.EPHEMERAL);
    registry.pathMustExist("/p1/p2");
  }

  @Test(expected = FileNotFoundException.class)
  public void testMkdirChild() throws Throwable {
    registry.mkdir("/testMkdirChild/child", CreateMode.PERSISTENT);
  }

  @Test
  public void testMaybeCreate() throws Throwable {
    assertTrue(registry.maybeCreate("/p3", CreateMode.PERSISTENT));
    assertFalse(registry.maybeCreate("/p3", CreateMode.PERSISTENT));
  }

  @Test
  public void testRM() throws Throwable {
    registry.mkdir("/rm", CreateMode.PERSISTENT);
    registry.rm("/rm", false);
    verifyNotExists("/rm");
    registry.rm("/rm", false);
  }

  @Test
  public void testRMNonRf() throws Throwable {
    registry.mkdir("/rm", CreateMode.PERSISTENT);
    registry.mkdir("/rm/child", CreateMode.PERSISTENT);
    try {
      registry.rm("/rm", false);
      fail("expected a failure");
    } catch (RESTIOException expected) {

    }
  }


  @Test
  public void testRMNRf() throws Throwable {
    registry.mkdir("/rm", CreateMode.PERSISTENT);
    registry.mkdir("/rm/child", CreateMode.PERSISTENT);
    registry.rm("/rm", true);
    verifyNotExists("/rm");
    registry.rm("/rm", true);
  }

  @Test
  public void testCreate() throws Throwable {
    registry.create("/testCreate",
        CreateMode.PERSISTENT, getTestBuffer(),
        registry.getRootACL()
    );
    registry.pathMustExist("/testCreate");
  }

  @Test
  public void testCreateTwice() throws Throwable {
    byte[] buffer = getTestBuffer();
    registry.create("/testCreateTwice",
        CreateMode.PERSISTENT, buffer,
        registry.getRootACL()
    );
    try {
      registry.create("/testCreateTwice",
          CreateMode.PERSISTENT, buffer,
          registry.getRootACL()
      );
      fail();
    } catch (FileAlreadyExistsException e) {

    }
  }

  @Test
  public void testCreateUpdate() throws Throwable {
    byte[] buffer = getTestBuffer();
    registry.create("/testCreateUpdate",
        CreateMode.PERSISTENT, buffer,
        registry.getRootACL()
    );
    registry.update("/testCreateUpdate", buffer);
  }

  @Test(expected = FileNotFoundException.class)
  public void testUpdateMissing() throws Throwable {
    registry.update("/testUpdateMissing", getTestBuffer());
  }

  @Test
  public void testUpdateDirectory() throws Throwable {
    registry.mkdir("/testUpdateDirectory", CreateMode.PERSISTENT);
    registry.update("/testUpdateDirectory", getTestBuffer());
  }

  @Test
  public void testUpdateDirectorywithChild() throws Throwable {
    registry.mkdir("/testUpdateDirectorywithChild", CreateMode.PERSISTENT);
    registry.mkdir("/testUpdateDirectorywithChild/child",
        CreateMode.PERSISTENT);
    registry.update("/testUpdateDirectorywithChild", getTestBuffer());
  }


  protected byte[] getTestBuffer() {
    byte[] buffer = new byte[1];
    buffer[0] = '0';
    return buffer;
  }


  public void verifyNotExists(String path) throws IOException {
    if (registry.pathExists(path)) {
      fail("Path should not exist: " + path);
    }
  }
}
