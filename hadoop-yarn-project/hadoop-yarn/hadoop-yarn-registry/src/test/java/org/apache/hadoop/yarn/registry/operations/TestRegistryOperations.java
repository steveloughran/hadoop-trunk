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
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;

import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.*;

import org.apache.hadoop.yarn.registry.client.binding.RecordOperations;
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.binding.ZKPathDumper;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.api.CreateFlags;
import org.apache.hadoop.yarn.registry.client.services.CuratorEventCatcher;
import org.apache.hadoop.yarn.registry.client.types.PersistencePolicies;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.server.services.RMRegistryOperationsService;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.Future;

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
        operations.listDir(PARENT_PATH);
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
        operations.listDir(PARENT_PATH);
  }

  @Test(expected = PathNotFoundException.class)
  public void testResolveEmptyPath() throws Throwable {
    operations.resolve(ENTRY_PATH);
  }

  @Test
  public void testMkdirNoParent() throws Throwable {
    String path = ENTRY_PATH + "/missing";
    try {
      operations.mkdir(path, false);
      RegistryPathStatus stat = operations.stat(path);
      fail("Got a status " + stat);
    } catch (PathNotFoundException expected) {

    }
  }
  
  @Test
  public void testDoubleMkdir() throws Throwable {

    operations.mkdir(USERPATH, false);
    String path = USERPATH +"newentry";
    assertTrue(operations.mkdir(path, false));
    RegistryPathStatus stat = operations.stat(path);
    assertFalse(operations.mkdir(path, false));
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
  public void testStatPathThatHasNoEntry() throws Throwable {
    String empty = "/empty";
    operations.mkdir(empty, false);
    RegistryPathStatus stat = operations.stat(empty);
  }


  @Test
  public void testResolvePathThatHasNoEntry() throws Throwable {
    String empty = "/empty2";
    operations.mkdir(empty, false);
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

  /**
   * Create a complex example app
   * @throws Throwable
   */
  @Test
  public void testCreateComplexApplication() throws Throwable {
    String appId = "application_1408631738011_0001";
    String cid = "container_1408631738011_0001_01_";
    String cid1 = cid +"000001";
    String cid2 = cid +"000002";
    String appPath = USERPATH + "tomcat";

    ServiceRecord webapp = new ServiceRecord(appId,
        "tomcat-based web application", 
        PersistencePolicies.APPLICATION, null);
    webapp.addExternalEndpoint(restEndpoint("www",
        new URI("http","//loadbalancer/", null)));

    ServiceRecord comp1 = new ServiceRecord(cid1, null,
        PersistencePolicies.CONTAINER, null);
    comp1.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//rack4server3:43572", null)));
    comp1.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack4server3", 43573));
    
    // Component 2 has a container lifespan
    ServiceRecord comp2 = new ServiceRecord(cid2, null,
        PersistencePolicies.CONTAINER, null);
    comp2.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//rack1server28:35881",null)));
    comp2.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack1server28", 35882));

    operations.mkdir(USERPATH, false);
    operations.create(appPath, webapp, CreateFlags.OVERWRITE);
    String components = appPath + RegistryConstants.SUBPATH_COMPONENTS + "/";
    operations.mkdir(components, false);
    String dns1 = RegistryPathUtils.encodeYarnID(cid1);
    String dns1path = components + dns1;
    operations.create(dns1path, comp1, CreateFlags.CREATE);
    String dns2 = RegistryPathUtils.encodeYarnID(cid2);
    String dns2path = components + dns2;
    operations.create(dns2path, comp2, CreateFlags.CREATE );

    ZKPathDumper pathDumper = registry.dumpPath();
    LOG.info(pathDumper.toString());

    logRecord("tomcat", webapp);
    logRecord(dns1, comp1);
    logRecord(dns2, comp2);

    ServiceRecord dns1resolved = operations.resolve(dns1path);
    assertEquals("Persistence policies on resolved entry",
        PersistencePolicies.CONTAINER, dns1resolved.persistence);


    RegistryPathStatus[] componentStats = operations.listDir(components);
    assertEquals(2, componentStats.length);
    Map<String, ServiceRecord> records =
        RecordOperations.extractServiceRecords(operations, componentStats);
    assertEquals(2, records.size());
    ServiceRecord retrieved1 = records.get(dns1path);
    logRecord(retrieved1.id, retrieved1);
    assertMatches(dns1resolved, retrieved1);
    assertEquals(PersistencePolicies.CONTAINER, retrieved1.persistence);

    // create a listing under components/
    operations.mkdir(components + "subdir", false);
    RegistryPathStatus[] componentStatsUpdated = operations.listDir(components);
    assertEquals(3, componentStatsUpdated.length);
    Map<String, ServiceRecord> recordsUpdated =
        RecordOperations.extractServiceRecords(operations, componentStats);
    assertEquals(2, recordsUpdated.size());

    
    
    // now do some deletions.
    
    // synchronous delete container ID 2
    
    // fail if the app policy is chosen
    assertEquals(0, registry.purgeRecords("/", cid2,
        PersistencePolicies.APPLICATION,
        RMRegistryOperationsService.PurgePolicy.FailOnChildren,
        null));
    // succeed for container
    assertEquals(1, registry.purgeRecords("/", cid2,
        PersistencePolicies.CONTAINER,
        RMRegistryOperationsService.PurgePolicy.FailOnChildren,
        null));
    assertPathNotFound(dns2path);
    assertPathExists(dns1path);
    
    // attempt to delete root with policy of fail on children
    try {
      registry.purgeRecords("/",
          appId,
          PersistencePolicies.APPLICATION,
          RMRegistryOperationsService.PurgePolicy.FailOnChildren, null);
      fail("expected a failure");
    } catch (PathIsNotEmptyDirectoryException expected) {
     // expected
    }
    assertPathExists(appPath);
    assertPathExists(dns1path);

    // downgrade to a skip on children
    assertEquals(0,
        registry.purgeRecords("/", appId,
            PersistencePolicies.APPLICATION,
            RMRegistryOperationsService.PurgePolicy.SkipOnChildren,
            null));
    assertPathExists(appPath);
    assertPathExists(dns1path);

    // now trigger recursive delete
    assertEquals(1,
        registry.purgeRecords("/",
            appId,
            PersistencePolicies.APPLICATION,
            RMRegistryOperationsService.PurgePolicy.PurgeAll,
            null));
    assertPathNotFound(appPath);
    assertPathNotFound(dns1path);

  }


  @Test
  public void testPurgeEntryCuratorCallback() throws Throwable {

    String path = "/users/example/hbase/hbase1/";
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.APPLICATION_ATTEMPT);
    written.id = "testAsyncPurgeEntry_attempt_001";

    operations.mkdir(RegistryPathUtils.parentOf(path), true);
    operations.create(path, written, 0);

    ZKPathDumper dump = registry.dumpPath();
    CuratorEventCatcher events = new CuratorEventCatcher();

    LOG.info("Initial state {}", dump);

    // container query
    int opcount = registry.purgeRecords("/",
        written.id,
        PersistencePolicies.CONTAINER,
        RMRegistryOperationsService.PurgePolicy.PurgeAll,
        events);
    assertPathExists(path);
    assertEquals(0, opcount);
    assertEquals("Event counter", 0, events.getCount());


    // now the application attempt
    opcount = registry.purgeRecords("/",
        written.id,
        -1,
        RMRegistryOperationsService.PurgePolicy.PurgeAll,
        events);

    LOG.info("Final state {}", dump);

    assertPathNotFound(path);
    assertEquals("wrong no of delete operations in " + dump, 1, opcount);
    // and validate the callback event
    assertEquals("Event counter", 1, events.getCount());

  }
  
  @Test
  public void testAsyncPurgeEntry() throws Throwable {

    String path = "/users/example/hbase/hbase1/";
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.APPLICATION_ATTEMPT);
    written.id = "testAsyncPurgeEntry_attempt_001";

    operations.mkdir(RegistryPathUtils.parentOf(path), true);
    operations.create(path, written, 0);

    ZKPathDumper dump = registry.dumpPath();

    LOG.info("Initial state {}", dump);

    Future<Integer> future = registry.purgeRecordsAsync("/",
        written.id,
        PersistencePolicies.CONTAINER);

    int opcount = future.get();
    assertPathExists(path);
    assertEquals(0, opcount);


    // now all matching entries
    future = registry.purgeRecordsAsync("/",
        written.id,
        -1);
    opcount = future.get();
    LOG.info("Final state {}", dump);

    assertPathNotFound(path);
    assertEquals("wrong no of delete operations in " + dump, 1, opcount);
    // and validate the callback event

  }
  

  @Test
  public void testPutGetEphemeralServiceEntry() throws Throwable {

    String path = ENTRY_PATH;
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.CONTAINER);

    operations.mkdir(RegistryPathUtils.parentOf(path), true);
    operations.create(path, written, CreateFlags.CREATE);
    ServiceRecord resolved = operations.resolve(path);
    validateEntry(resolved);
    assertMatches(written, resolved);
  }
  
}
