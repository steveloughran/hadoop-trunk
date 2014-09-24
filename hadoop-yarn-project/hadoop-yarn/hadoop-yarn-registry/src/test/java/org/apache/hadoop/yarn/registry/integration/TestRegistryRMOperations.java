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

package org.apache.hadoop.yarn.registry.integration;

import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.yarn.registry.AbstractRegistryTest;
import org.apache.hadoop.yarn.registry.client.api.CreateFlags;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.binding.RecordOperations;
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.binding.ZKPathDumper;
import org.apache.hadoop.yarn.registry.client.services.CuratorEventCatcher;
import org.apache.hadoop.yarn.registry.client.types.PersistencePolicies;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.server.services.DeleteCompletionCallback;
import org.apache.hadoop.yarn.registry.server.services.RegistryAdminService;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.inetAddrEndpoint;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.restEndpoint;

public class TestRegistryRMOperations extends AbstractRegistryTest {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryRMOperations.class);

  /**
   * trigger a purge operation
   * @param path path
   * @param id yarn ID
   * @param policyMatch policy to match ID on
   * @param purgePolicy policy when there are children under a match
   * @return the number purged
   * @throws IOException
   */
  public int purge(String path,
      String id,
      int policyMatch,
      RegistryAdminService.PurgePolicy purgePolicy) throws
      IOException,
      ExecutionException,
      InterruptedException {
    return purge(path, id, policyMatch, purgePolicy, null);
  }

  /**
   *
   * trigger a purge operation
   * @param path pathn
   * @param id yarn ID
   * @param policyMatch policy to match ID on
   * @param purgePolicy policy when there are children under a match
   * @param callback optional callback
   * @return the number purged
   * @throws IOException
   */
  public int purge(String path,
      String id,
      int policyMatch,
      RegistryAdminService.PurgePolicy purgePolicy,
      BackgroundCallback callback) throws
      IOException,
      ExecutionException,
      InterruptedException {

    Future<Integer> future = registry.purgeRecordsAsync(path,
        id, policyMatch, purgePolicy, callback);
    try {
      return future.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testPurgeEntryCuratorCallback() throws Throwable {

    String path = "/users/example/hbase/hbase1/";
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.APPLICATION_ATTEMPT);
    written.id = "testAsyncPurgeEntry_attempt_001";

    operations.mknode(RegistryPathUtils.parentOf(path), true);
    operations.create(path, written, 0);

    ZKPathDumper dump = registry.dumpPath();
    CuratorEventCatcher events = new CuratorEventCatcher();

    LOG.info("Initial state {}", dump);

    // container query
    int opcount = purge("/",
        written.id,
        PersistencePolicies.CONTAINER,
        RegistryAdminService.PurgePolicy.PurgeAll,
        events);
    assertPathExists(path);
    assertEquals(0, opcount);
    assertEquals("Event counter", 0, events.getCount());

    // now the application attempt
    opcount = purge("/",
        written.id,
        -1,
        RegistryAdminService.PurgePolicy.PurgeAll,
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

    operations.mknode(RegistryPathUtils.parentOf(path), true);
    operations.create(path, written, 0);

    ZKPathDumper dump = registry.dumpPath();

    LOG.info("Initial state {}", dump);

    DeleteCompletionCallback deletions = new DeleteCompletionCallback();
    int opcount = purge("/",
        written.id,
        PersistencePolicies.CONTAINER,
        RegistryAdminService.PurgePolicy.PurgeAll,
        deletions);
    assertPathExists(path);

    dump = registry.dumpPath();

    assertEquals("wrong no of delete operations in " + dump, 0,
        deletions.getEventCount());
    assertEquals("wrong no of delete operations in " + dump, 0, opcount);


    // now all matching entries
    deletions = new DeleteCompletionCallback();
    opcount = purge("/",
        written.id,
        -1,
        RegistryAdminService.PurgePolicy.PurgeAll,
        deletions);

    dump = registry.dumpPath();
    LOG.info("Final state {}", dump);

    assertPathNotFound(path);
    assertEquals("wrong no of delete operations in " + dump, 1,
        deletions.getEventCount());
    assertEquals("wrong no of delete operations in " + dump, 1, opcount);
    // and validate the callback event

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

  /**
   * Create a complex example app
   * @throws Throwable
   */
  @Test
  public void testCreateComplexApplication() throws Throwable {
    String appId = "application_1408631738011_0001";
    String cid = "container_1408631738011_0001_01_";
    String cid1 = cid + "000001";
    String cid2 = cid + "000002";
    String appPath = USERPATH + "tomcat";

    ServiceRecord webapp = new ServiceRecord(appId,
        "tomcat-based web application",
        PersistencePolicies.APPLICATION, null);
    webapp.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//loadbalancer/", null)));

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
        new URI("http", "//rack1server28:35881", null)));
    comp2.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack1server28", 35882));

    operations.mknode(USERPATH, false);
    operations.create(appPath, webapp, CreateFlags.OVERWRITE);
    String components = appPath + RegistryConstants.SUBPATH_COMPONENTS;
    operations.mknode(components, false);
    String dns1 = RegistryPathUtils.encodeYarnID(cid1);
    String dns1path = components + dns1;
    operations.create(dns1path, comp1, CreateFlags.CREATE);
    String dns2 = RegistryPathUtils.encodeYarnID(cid2);
    String dns2path = components + dns2;
    operations.create(dns2path, comp2, CreateFlags.CREATE);

    ZKPathDumper pathDumper = registry.dumpPath();
    LOG.info(pathDumper.toString());

    logRecord("tomcat", webapp);
    logRecord(dns1, comp1);
    logRecord(dns2, comp2);

    ServiceRecord dns1resolved = operations.resolve(dns1path);
    assertEquals("Persistence policies on resolved entry",
        PersistencePolicies.CONTAINER, dns1resolved.persistence);

    List<RegistryPathStatus> componentStats = operations.list(components);
    assertEquals(2, componentStats.size());
    Map<String, ServiceRecord> records =
        RecordOperations.extractServiceRecords(operations, componentStats);
    assertEquals(2, records.size());
    ServiceRecord retrieved1 = records.get(dns1path);
    logRecord(retrieved1.id, retrieved1);
    assertMatches(dns1resolved, retrieved1);
    assertEquals(PersistencePolicies.CONTAINER, retrieved1.persistence);

    // create a listing under components/
    operations.mknode(components + "subdir", false);
    List<RegistryPathStatus> componentStatsUpdated =
        operations.list(components);
    assertEquals(3, componentStatsUpdated.size());
    Map<String, ServiceRecord> recordsUpdated =
        RecordOperations.extractServiceRecords(operations, componentStats);
    assertEquals(2, recordsUpdated.size());

    // now do some deletions.

    // synchronous delete container ID 2

    // fail if the app policy is chosen
    assertEquals(0, purge("/", cid2, PersistencePolicies.APPLICATION,
        RegistryAdminService.PurgePolicy.FailOnChildren));
    // succeed for container
    assertEquals(1, purge("/", cid2, PersistencePolicies.CONTAINER,
        RegistryAdminService.PurgePolicy.FailOnChildren));
    assertPathNotFound(dns2path);
    assertPathExists(dns1path);

    // expect a skip on children to skip
    assertEquals(0,
        purge("/", appId, PersistencePolicies.APPLICATION,
            RegistryAdminService.PurgePolicy.SkipOnChildren));
    assertPathExists(appPath);
    assertPathExists(dns1path);

    // attempt to delete app with policy of fail on children
    try {
      int p = purge("/",
          appId,
          PersistencePolicies.APPLICATION,
          RegistryAdminService.PurgePolicy.FailOnChildren);
      fail("expected a failure, got a purge count of " + p);
    } catch (PathIsNotEmptyDirectoryException expected) {
      // expected
    }
    assertPathExists(appPath);
    assertPathExists(dns1path);


    // now trigger recursive delete
    assertEquals(1,
        purge("/", appId, PersistencePolicies.APPLICATION,
            RegistryAdminService.PurgePolicy.PurgeAll));
    assertPathNotFound(appPath);
    assertPathNotFound(dns1path);

  }

  @Test
  public void testChildDeletion() throws Throwable {
    ServiceRecord app = new ServiceRecord("app1",
        "app",
        PersistencePolicies.APPLICATION, null);
    ServiceRecord container = new ServiceRecord("container1",
        "container",
        PersistencePolicies.CONTAINER, null);

    operations.create("/app", app, CreateFlags.OVERWRITE);
    operations.create("/app/container", container, CreateFlags.OVERWRITE);

    try {
      int p = purge("/",
          "app1",
          PersistencePolicies.APPLICATION,
          RegistryAdminService.PurgePolicy.FailOnChildren);
      fail("expected a failure, got a purge count of " + p);
    } catch (PathIsNotEmptyDirectoryException expected) {
      // expected
    }

  }

}

