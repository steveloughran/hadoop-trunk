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

package org.apache.hadoop.yarn.registry.operations;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.yarn.registry.AbstractRegistryTest;
import org.apache.hadoop.yarn.registry.client.api.CreateFlags;
import org.apache.hadoop.yarn.registry.client.binding.RecordOperations;
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.types.PersistencePolicies;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TestRegistryOperations extends AbstractRegistryTest {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryOperations.class);

  @Test
  public void testPutGetServiceEntry() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0,
        PersistencePolicies.APPLICATION);
    ServiceRecord resolved = operations.resolve(ENTRY_PATH);
    validateEntry(resolved);
    assertMatches(written, resolved);
  }

  @Test
  public void testDeleteServiceEntry() throws Throwable {
    putExampleServiceEntry(ENTRY_PATH, 0);
    operations.delete(ENTRY_PATH, false);
  }

  @Test
  public void testDeleteNonexistentEntry() throws Throwable {
    operations.delete(ENTRY_PATH, false);
    operations.delete(ENTRY_PATH, true);
  }

  @Test
  public void testStat() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    RegistryPathStatus stat = operations.stat(ENTRY_PATH);
    assertTrue(stat.size > 0);
    assertTrue(stat.time > 0);
    assertEquals(ENTRY_PATH, stat.path);
  }

  @Test
  public void testLsParent() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    RegistryPathStatus stat = operations.stat(ENTRY_PATH);

    RegistryPathStatus[] statuses =
        operations.list(PARENT_PATH);
    assertEquals(1, statuses.length);
    assertEquals(stat, statuses[0]);

    Map<String, ServiceRecord> records =
        RecordOperations.extractServiceRecords(operations, statuses);
    assertEquals(1, records.size());
    ServiceRecord record = records.get(ENTRY_PATH);
    assertMatches(written, record);
  }

  @Test
  public void testDeleteNonEmpty() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    RegistryPathStatus stat = operations.stat(ENTRY_PATH);
    try {
      operations.delete(PARENT_PATH, false);
      fail("Expected a failure");
    } catch (PathIsNotEmptyDirectoryException e) {

    }
    operations.delete(PARENT_PATH, true);
  }

  @Test(expected = PathNotFoundException.class)
  public void testStatEmptyPath() throws Throwable {
    RegistryPathStatus stat = operations.stat(ENTRY_PATH);
  }

  @Test(expected = PathNotFoundException.class)
  public void testLsEmptyPath() throws Throwable {
    RegistryPathStatus[] statuses =
        operations.list(PARENT_PATH);
  }

  @Test(expected = PathNotFoundException.class)
  public void testResolveEmptyPath() throws Throwable {
    operations.resolve(ENTRY_PATH);
  }

  @Test
  public void testMkdirNoParent() throws Throwable {
    String path = ENTRY_PATH + "/missing";
    try {
      operations.mknode(path, false);
      RegistryPathStatus stat = operations.stat(path);
      fail("Got a status " + stat);
    } catch (PathNotFoundException expected) {

    }
  }

  @Test
  public void testDoubleMkdir() throws Throwable {
    operations.mknode(USERPATH, false);
    String path = USERPATH + "newentry";
    assertTrue(operations.mknode(path, false));
    RegistryPathStatus stat = operations.stat(path);
    assertFalse(operations.mknode(path, false));
  }

  @Test
  public void testPutNoParent() throws Throwable {
    ServiceRecord record = new ServiceRecord();
    record.id = "testPutNoParent";
    String path = "/path/without/parent";
    try {
      operations.create(path, record, 0);
      // didn't get a failure
      // trouble
      RegistryPathStatus stat = operations.stat(path);
      fail("Got a status " + stat);
    } catch (PathNotFoundException expected) {
    }
  }

  @Test(expected = PathNotFoundException.class)
  public void testPutNoParent2() throws Throwable {
    ServiceRecord record = new ServiceRecord();
    record.id = "testPutNoParent";
    String path = "/path/without/parent";
    operations.create(path, record, 0);
  }

  @Test
  public void testStatDirectory() throws Throwable {
    String empty = "/empty";
    operations.mknode(empty, false);
    RegistryPathStatus stat = operations.stat(empty);
  }

  @Test
  public void testStatRootPath() throws Throwable {
    operations.mknode("/", false);
    RegistryPathStatus stat = operations.stat("/");
  }

  @Test
  public void testStatOneLevelDown() throws Throwable {
    operations.mknode("/subdir", true);
    RegistryPathStatus stat = operations.stat("/subdir");
  }


  @Test
  public void testLsRootPath() throws Throwable {
    String empty = "/";
    operations.mknode(empty, false);
    RegistryPathStatus stat = operations.stat(empty);
  }


  @Test
  public void testResolvePathThatHasNoEntry() throws Throwable {
    String empty = "/empty2";
    operations.mknode(empty, false);
    try {
      ServiceRecord record = operations.resolve(empty);
      fail("expected an exception");
    } catch (InvalidRecordException expected) {

    }
  }


  @Test
  public void testOverwrite() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    ServiceRecord resolved1 = operations.resolve(ENTRY_PATH);
    resolved1.description = "resolved1";
    try {
      operations.create(ENTRY_PATH, resolved1, 0);
      fail("overwrite succeeded when it should have failed");
    } catch (FileAlreadyExistsException expected) {

    }

    // verify there's no changed
    ServiceRecord resolved2 = operations.resolve(ENTRY_PATH);
    assertMatches(written, resolved2);
    operations.create(ENTRY_PATH, resolved1, CreateFlags.OVERWRITE);
    ServiceRecord resolved3 = operations.resolve(ENTRY_PATH);
    assertMatches(resolved1, resolved3);
  }


  @Test
  public void testPutGetContainerPersistenceServiceEntry() throws Throwable {

    String path = ENTRY_PATH;
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.CONTAINER);

    operations.mknode(RegistryPathUtils.parentOf(path), true);
    operations.create(path, written, CreateFlags.CREATE);
    ServiceRecord resolved = operations.resolve(path);
    validateEntry(resolved);
    assertMatches(written, resolved);
  }

  @Test
  public void testAddingWriteAccessIsNoOpEntry() throws Throwable {

    assertFalse(operations.addWriteAccessor("id","pass"));
    operations.clearWriteAccessors();
  }

}
