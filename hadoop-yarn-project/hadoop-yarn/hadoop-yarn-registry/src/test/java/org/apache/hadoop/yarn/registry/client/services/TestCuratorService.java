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

package org.apache.hadoop.yarn.registry.client.services;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
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

/**
 * Test the curator service
 */
public class TestCuratorService extends AbstractZKRegistryTest {
  protected CuratorService curatorService;

  public static final String MISSING = "/missing";

  @Before
  public void startCurator() {
    createCuratorService();
  }

  @After
  public void stopCurator() {
    ServiceOperations.stop(curatorService);
  }

  /**
   * Create an instance
   */
  protected void createCuratorService() {
    curatorService = new CuratorService("curatorService");
    curatorService.init(createRegistryConfiguration());
    curatorService.start();
  }
  @Test
  public void testLs() throws Throwable {
    curatorService.zkList("/");
  }

  @Test(expected = PathNotFoundException.class)
  public void testLsNotFound() throws Throwable {
    List<String> ls = curatorService.zkList(MISSING);
  }

  @Test
  public void testExists() throws Throwable {
    assertTrue(curatorService.zkPathExists("/"));
  }

  @Test
  public void testExistsMissing() throws Throwable {
    assertFalse(curatorService.zkPathExists(MISSING));
  }

  @Test
  public void testVerifyExists() throws Throwable {
    curatorService.zkPathMustExist("/");
  }

  @Test(expected = PathNotFoundException.class)
  public void testVerifyExistsMissing() throws Throwable {
    curatorService.zkPathMustExist(MISSING);
  }

  @Test
  public void testMkdirs() throws Throwable {
    curatorService.zkMkPath("/p1", CreateMode.PERSISTENT);
    curatorService.zkPathMustExist("/p1");
    curatorService.zkMkPath("/p1/p2", CreateMode.EPHEMERAL);
    curatorService.zkPathMustExist("/p1/p2");
  }

  @Test(expected = PathNotFoundException.class)
  public void testMkdirChild() throws Throwable {
    curatorService.zkMkPath("/testMkdirChild/child", CreateMode.PERSISTENT);
  }

  @Test
  public void testMaybeCreate() throws Throwable {
    assertTrue(curatorService.maybeCreate("/p3", CreateMode.PERSISTENT));
    assertFalse(curatorService.maybeCreate("/p3", CreateMode.PERSISTENT));
  }

  @Test
  public void testRM() throws Throwable {
    curatorService.zkMkPath("/rm", CreateMode.PERSISTENT);
    curatorService.zkDelete("/rm", false);
    verifyNotExists("/rm");
    curatorService.zkDelete("/rm", false);
  }

  @Test
  public void testRMNonRf() throws Throwable {
    curatorService.zkMkPath("/rm", CreateMode.PERSISTENT);
    curatorService.zkMkPath("/rm/child", CreateMode.PERSISTENT);
    try {
      curatorService.zkDelete("/rm", false);
      fail("expected a failure");
    } catch (PathIsNotEmptyDirectoryException expected) {

    }
  }

  @Test
  public void testRMNRf() throws Throwable {
    curatorService.zkMkPath("/rm", CreateMode.PERSISTENT);
    curatorService.zkMkPath("/rm/child", CreateMode.PERSISTENT);
    curatorService.zkDelete("/rm", true);
    verifyNotExists("/rm");
    curatorService.zkDelete("/rm", true);
  }

  @Test
  public void testCreate() throws Throwable {
    curatorService.zkCreate("/testCreate",
        CreateMode.PERSISTENT, getTestBuffer(),
        curatorService.getRootACL()
    );
    curatorService.zkPathMustExist("/testCreate");
  }

  @Test
  public void testCreateTwice() throws Throwable {
    byte[] buffer = getTestBuffer();
    curatorService.zkCreate("/testCreateTwice",
        CreateMode.PERSISTENT, buffer,
        curatorService.getRootACL()
    );
    try {
      curatorService.zkCreate("/testCreateTwice",
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
    curatorService.zkCreate("/testCreateUpdate",
        CreateMode.PERSISTENT, buffer,
        curatorService.getRootACL()
    );
    curatorService.zkUpdate("/testCreateUpdate", buffer);
  }

  @Test(expected = PathNotFoundException.class)
  public void testUpdateMissing() throws Throwable {
    curatorService.zkUpdate("/testUpdateMissing", getTestBuffer());
  }

  @Test
  public void testUpdateDirectory() throws Throwable {
    curatorService.zkMkPath("/testUpdateDirectory", CreateMode.PERSISTENT);
    curatorService.zkUpdate("/testUpdateDirectory", getTestBuffer());
  }

  @Test
  public void testUpdateDirectorywithChild() throws Throwable {
    curatorService.zkMkPath("/testUpdateDirectorywithChild",
        CreateMode.PERSISTENT);
    curatorService.zkMkPath("/testUpdateDirectorywithChild/child",
        CreateMode.PERSISTENT);
    curatorService.zkUpdate("/testUpdateDirectorywithChild", getTestBuffer());
  }


  protected byte[] getTestBuffer() {
    byte[] buffer = new byte[1];
    buffer[0] = '0';
    return buffer;
  }


  public void verifyNotExists(String path) throws IOException {
    if (curatorService.zkPathExists(path)) {
      fail("Path should not exist: " + path);
    }
  }
}
