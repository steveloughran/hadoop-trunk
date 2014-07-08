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
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.registry.AbstractZKRegistryTest;
import org.apache.hadoop.yarn.registry.client.exceptions.RESTIOException;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class TestRegistryZKService extends AbstractZKRegistryTest {


  public static final String MISSING = "/missing";
  private RegistryZKService registry;

  @Before
  public void startRegistry() {
    createRegistry();
  }

  @After
  public void stopRegistry() {
    ServiceOperations.stop(registry);
  }

  protected void createRegistry() {
    registry = new RegistryZKService("registry");
    registry.init(createRegistryConfiguration());
    registry.start();
  }
/*

  @Test
  public void testRegistryStartStop() throws Throwable {
    createRegistry();
    stopRegistry();
  }
*/

  @Test
  public void testLs() throws Throwable {
    registry.ls("/");
  }

  @Test
  public void testLsNotFound() throws Throwable {
    try {
      List<String> ls = registry.ls(MISSING);
      fail("Expected an exception");
    } catch (FileNotFoundException expected) {

    }
  }

  @Test
  public void testExists() throws Throwable {
    assertTrue(registry.exists("/"));
  }

  @Test
  public void testExistsMissing() throws Throwable {
    assertFalse(registry.exists(MISSING));
  }

  @Test
  public void testVerifyExists() throws Throwable {
    registry.verifyExists("/");
  }

  @Test
  public void testVerifyExistsMissing() throws Throwable {
    try {
      registry.verifyExists(MISSING);
      fail("Expected an exception");
    } catch (FileNotFoundException expected) {

    }
  }

  @Test
  public void testMkdirs() throws Throwable {
    registry.mkdir("/p1", CreateMode.PERSISTENT);
    registry.verifyExists("/p1");
    registry.mkdir("/p1/p2", CreateMode.EPHEMERAL);
    registry.verifyExists("/p1/p2");

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
    } catch (RESTIOException e) {

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
    registry.verifyExists("/testCreate");
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
    registry.mkdir("/testUpdateDirectory",CreateMode.PERSISTENT);
    registry.update("/testUpdateDirectory", getTestBuffer());
  }

  @Test
  public void testUpdateDirectorywithChild() throws Throwable {
    registry.mkdir("/testUpdateDirectorywithChild",CreateMode.PERSISTENT_SEQUENTIAL);
    registry.mkdir("/testUpdateDirectorywithChild/child",CreateMode.PERSISTENT);
    registry.update("/testUpdateDirectorywithChild", getTestBuffer());
  }

  
  protected byte[] getTestBuffer() {
    byte[] buffer = new byte[1];
    buffer[0] = '0';
    return buffer;
  }


  public void verifyNotExists(String path) throws IOException {
    if (registry.exists(path)) {
      fail("Path should not exist: " + path);
    }
  }
}
