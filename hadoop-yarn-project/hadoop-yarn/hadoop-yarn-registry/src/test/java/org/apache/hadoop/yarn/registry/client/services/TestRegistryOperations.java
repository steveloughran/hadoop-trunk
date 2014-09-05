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
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.*;

import org.apache.hadoop.yarn.registry.client.binding.RecordOperations;
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.binding.ZKPathDumper;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.exceptions.NoChildrenForEphemeralsException;
import org.apache.hadoop.yarn.registry.client.types.AddressTypes;
import org.apache.hadoop.yarn.registry.client.types.CreateFlags;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.PersistencePolicies;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.server.services.ResourceManagerRegistryService;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class TestRegistryOperations extends AbstractZKRegistryTest {
  public static final String SC_HADOOP = "org-apache-hadoop";
  public static final String USER = "devteam/";
  public static final String NAME = "hdfs";
  public static final String API_WEBHDFS = "org_apache_hadoop_namenode_webhdfs";
  public static final String API_HDFS = "org_apache_hadoop_namenode_dfs";

  public static final String USERPATH =
      "/" + RegistryConstants.PATH_USERS + USER;
  public static final String PARENT_PATH = USERPATH + SC_HADOOP + "/";
  public static final String ENTRY_PATH = PARENT_PATH + NAME;
  public static final String NNIPC = "nnipc";
  public static final String IPC2 = "IPC2";

  private ResourceManagerRegistryService registry;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryOperations.class);
  private RegistryOperations operations;
  private final RecordOperations.ServiceRecordMarshal recordMarshal =
      new RecordOperations.ServiceRecordMarshal();


  @Before
  public void setupClient() throws IOException {
    registry = new ResourceManagerRegistryService("yarnRegistry");
    registry.init(createRegistryConfiguration());
    registry.start();
    registry.createRegistryPaths();
    operations = registry;
    operations.delete(ENTRY_PATH, true);
  }

  @After
  public void teardownClient() {
    ServiceOperations.stop(registry);
  }

  /**
   * Add some endpoints
   * @param entry entry
   */
  protected void addSampleEndpoints(ServiceRecord entry, String hostname) throws
      URISyntaxException {
    entry.addExternalEndpoint(webEndpoint("web",
        new URI("http", hostname + ":80", "/")));
    entry.addExternalEndpoint(
        restEndpoint(API_WEBHDFS,
            new URI("http", hostname + ":8020", "/")));

    Endpoint endpoint = ipcEndpoint(API_HDFS,
        true, null);
    endpoint.addresses.add(tuple(hostname, "8030"));
    entry.addInternalEndpoint(endpoint);
    InetSocketAddress localhost = new InetSocketAddress("localhost", 8050);
    entry.addInternalEndpoint(
        inetAddrEndpoint(NNIPC, ProtocolTypes.PROTOCOL_THRIFT, "localhost",
            8050));
    entry.addInternalEndpoint(
        RegistryTypeUtils.ipcEndpoint(
            IPC2,
            true,
            RegistryTypeUtils.marshall(localhost)));
    
  }

  /**
   * General code to validate bits of a component/service entry built iwth
   * {@link #addSampleEndpoints(ServiceRecord, String)}
   * @param record instance to check
   */
  protected void validateEntry(ServiceRecord record) {
    assertNotNull("null service record", record);
    List<Endpoint> endpoints = record.external;
    assertEquals(2, endpoints.size());

    Endpoint webhdfs = findEndpoint(record, API_WEBHDFS, true, 1, 1);
    assertEquals(API_WEBHDFS, webhdfs.api);
    assertEquals(AddressTypes.ADDRESS_URI, webhdfs.addressType);
    assertEquals(ProtocolTypes.PROTOCOL_REST, webhdfs.protocolType);
    List<List<String>> addressList = webhdfs.addresses;
    List<String> url = addressList.get(0);
    String addr = url.get(0);
    assertTrue(addr.contains("http"));
    assertTrue(addr.contains(":8020"));

    Endpoint nnipc = findEndpoint(record, NNIPC, false, 1,2);
    assertEquals("wrong protocol in " + nnipc, ProtocolTypes.PROTOCOL_THRIFT,
        nnipc.protocolType);

    Endpoint ipc2 = findEndpoint(record, IPC2, false, 1,2);

    Endpoint web = findEndpoint(record, "web", true, 1, 1);
    assertEquals(1, web.addresses.size());
    assertEquals(1, web.addresses.get(0).size());
    
  }


  /**
   * Create a service entry with the sample endpoints, and put it
   * at the destination
   * @param path path
   * @param createFlags flags
   * @return the record
   * @throws IOException on a failure
   */
  protected ServiceRecord putExampleServiceEntry(String path, int createFlags) throws
      IOException,
      URISyntaxException {
    return putExampleServiceEntry(path, createFlags, PersistencePolicies.MANUAL);
  }
  
  /**
   * Create a service entry with the sample endpoints, and put it
   * at the destination
   * @param path path
   * @param createFlags flags
   * @return the record
   * @throws IOException on a failure
   */
  protected ServiceRecord putExampleServiceEntry(String path,
      int createFlags,
      int persistence)
      throws IOException, URISyntaxException {
    ServiceRecord record = buildExampleServiceEntry(persistence);

    registry.mkdir(RegistryPathUtils.parentOf(path), true);
    operations.create(path, record, createFlags);
    return record;
  }

  /**
   * Create a service entry with the sample endpoints
   * @param persistence persistence policy
   * @return the record
   * @throws IOException on a failure
   */
  private ServiceRecord buildExampleServiceEntry(int persistence) throws
      IOException,
      URISyntaxException {
    List<ACL> acls = registry.parseACLs("world:anyone:rwcda");
    ServiceRecord record = new ServiceRecord();
    record.id = "example-0001";
    record.persistence = persistence;
    record.description = methodName.getMethodName();
    record.registrationTime = System.currentTimeMillis();
    addSampleEndpoints(record, "namenode");
    return record;
  }


  public void assertMatches(Endpoint endpoint,
      String addressType,
      String protocolType,
      String api) {
    assertNotNull(endpoint);
    assertEquals(addressType, endpoint.addressType);
    assertEquals(protocolType, endpoint.protocolType);
    assertEquals(api, endpoint.api);
  }

  public void assertMatches(ServiceRecord written, ServiceRecord resolved) {
    assertEquals(written.id, resolved.id);
    assertEquals(written.registrationTime, resolved.registrationTime);
    assertEquals(written.description, resolved.description);
    assertEquals(written.persistence, resolved.persistence);
  }


  /**
   * Assert a path exists
   * @param path
   * @throws IOException
   */
  public void assertPathExists(String path) throws IOException {
    operations.stat(path);
  }
  
  public void assertPathNotFound(String path) throws IOException {
    try {
      operations.stat(path);
      fail("Path unexpectedly found: " + path);
    } catch (PathNotFoundException e) {

    }
  }

  public void assertResolves(String path) throws IOException {
    operations.resolve(path);
  }
  
  
  public Endpoint findEndpoint(ServiceRecord record,
      String api, boolean external, int addressElements, int elementSize) {
    Endpoint epr = external ? record.getExternalEndpoint(api)
                            : record.getInternalEndpoint(api);
    if (epr != null) {
      assertEquals("wrong # of addresses",
          addressElements, epr.addresses.size());
      assertEquals("wrong # of elements in an address",
          elementSize, epr.addresses.get(0).size());
      return epr;
    }
    List<Endpoint> endpoints = external ? record.external : record.internal;
    StringBuilder builder = new StringBuilder();
    for (Endpoint endpoint : endpoints) {
      builder.append("\"").append(endpoint).append("\" ");
    }
    fail("Did not find " + api + " in endpoints " + builder);
    return null;
  }


  public void log(String name, ServiceRecord record) throws
      IOException {
    LOG.info(" {} = \n{}\n", name, recordMarshal.toJson(record));
  }

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
        PersistencePolicies.APPLICATION);
    webapp.addExternalEndpoint(restEndpoint("www",
        new URI("http","//loadbalancer/", null)));

    ServiceRecord comp1 = new ServiceRecord(cid1, null,
        PersistencePolicies.EPHEMERAL);
    comp1.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//rack4server3:43572", null)));
    comp1.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack4server3", 43573));
    
    // Component 2 has a container lifespan
    ServiceRecord comp2 = new ServiceRecord(cid2, null,
        PersistencePolicies.CONTAINER);
    comp2.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//rack1server28:35881",null)));
    comp2.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack1server28", 35882));

    operations.mkdir(USERPATH, false);
    operations.create(appPath, webapp, CreateFlags.OVERWRITE);
    String components = appPath + RegistryConstants.SUBPATH_COMPONENTS + "/";
    operations.mkdir(components, false);
    String dns1 = yarnIdToDnsId(cid1);
    String dns1path = components + dns1;
    operations.create(dns1path, comp1, CreateFlags.EPHEMERAL);
    String dns2 = yarnIdToDnsId(cid2);
    String dns2path = components + dns2;
    operations.create(dns2path, comp2, CreateFlags.CREATE );

    ZKPathDumper pathDumper = registry.dumpPath();
    LOG.info(pathDumper.toString());

    log("tomcat", webapp);
    log(dns1, comp1);
    log(dns2, comp2);

    ServiceRecord dns1resolved = operations.resolve(dns1path);
    assertEquals("Persistence policies on resolved entry",
        PersistencePolicies.EPHEMERAL, dns1resolved.persistence);


    RegistryPathStatus[] componentStats = operations.listDir(components);
    assertEquals(2, componentStats.length);
    Map<String, ServiceRecord> records =
        RecordOperations.extractServiceRecords(operations, componentStats);
    assertEquals(2, records.size());
    ServiceRecord retrieved1 = records.get(dns1path);
    log(retrieved1.id, retrieved1);
    assertMatches(dns1resolved, retrieved1);
    assertEquals(PersistencePolicies.EPHEMERAL, retrieved1.persistence);

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
    assertEquals(0, registry.purgeRecordsWithID("/", cid2,
        PersistencePolicies.APPLICATION,
        ResourceManagerRegistryService.PurgePolicy.FailOnChildren,
        null));
    // succeed for container
    assertEquals(1, registry.purgeRecordsWithID("/", cid2,
        PersistencePolicies.CONTAINER,
        ResourceManagerRegistryService.PurgePolicy.FailOnChildren,
        null));
    assertPathNotFound(dns2path);
    assertPathExists(dns1path);
    
    // attempt to delete root with policy of fail on children
    try {
      registry.purgeRecordsWithID("/",
          appId,
          PersistencePolicies.APPLICATION,
          ResourceManagerRegistryService.PurgePolicy.FailOnChildren, null);
      fail("expected a failure");
    } catch (PathIsNotEmptyDirectoryException expected) {
     // expected
    }
    assertPathExists(appPath);
    assertPathExists(dns1path);

    // downgrade to a skip on children
    assertEquals(0,
        registry.purgeRecordsWithID("/", appId,
            PersistencePolicies.APPLICATION,
            ResourceManagerRegistryService.PurgePolicy.SkipOnChildren,
            null));
    assertPathExists(appPath);
    assertPathExists(dns1path);

    // now trigger recursive delete
    assertEquals(1,
        registry.purgeRecordsWithID("/",
            appId,
            PersistencePolicies.APPLICATION,
            ResourceManagerRegistryService.PurgePolicy.PurgeAll,
            null));
    assertPathNotFound(appPath);
    assertPathNotFound(dns1path);

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
    CuratorEventCatcher events = new CuratorEventCatcher();

    LOG.info("Initial state {}", dump);

    // container query
    int opcount = registry.purgeRecordsWithID("/",
        written.id,
        PersistencePolicies.CONTAINER,
        ResourceManagerRegistryService.PurgePolicy.PurgeAll,
        events);
    assertPathExists(path);
    assertEquals(0, opcount);
    assertEquals("Event counter", 0, events.getCount());


    // now the application attempt
    opcount = registry.purgeRecordsWithID("/",
        written.id,
        -1,
//        PersistencePolicies.APPLICATION_ATTEMPT,
        ResourceManagerRegistryService.PurgePolicy.PurgeAll,
        events);

    LOG.info("Final state {}", dump);

    assertPathNotFound(path);
    assertEquals("wrong no of delete operations in " + dump, 1, opcount);
    // and validate the callback event
    assertEquals("Event counter", 1, events.getCount());

  }
  

  @Test
  public void testPutGetEphemeralServiceEntry() throws Throwable {

    String path = ENTRY_PATH;
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.EPHEMERAL);

    operations.mkdir(RegistryPathUtils.parentOf(path), true);
    operations.create(path, written, CreateFlags.EPHEMERAL);
    ServiceRecord resolved = operations.resolve(path);
    validateEntry(resolved);
    assertMatches(written, resolved);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testPutGetEphemeralServiceEntryWrongPolicy() throws Throwable {
    String path = ENTRY_PATH;
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.APPLICATION_ATTEMPT);

    operations.mkdir(RegistryPathUtils.parentOf(path), true);
    operations.create(path, written, CreateFlags.EPHEMERAL);
  }
  
  @Test
  public void testEphemeralNoChildren() throws Throwable {
    ServiceRecord webapp = new ServiceRecord("1",
        "tomcat-based web application", PersistencePolicies.EPHEMERAL);
    operations.mkdir(USERPATH, false);
    String appPath = USERPATH + "tomcat2";

    operations.create(appPath, webapp, CreateFlags.EPHEMERAL);
    String components = appPath + RegistryConstants.SUBPATH_COMPONENTS + "/";
    try {
      operations.mkdir(components, false);
      fail("expected an error");
    } catch (NoChildrenForEphemeralsException expected) {
      // expected
    }
    try {
      operations.create(appPath + "/subdir", webapp, CreateFlags.EPHEMERAL);
      fail("expected an error");
    } catch (NoChildrenForEphemeralsException expected) {
      // expected
    }

  }

  @Test(expected = IllegalArgumentException.class)
  public void testPolicyConflict() throws Throwable {
    ServiceRecord rec = buildExampleServiceEntry(PersistencePolicies.EPHEMERAL);
    operations.mkdir(USERPATH, false);
    String appPath = USERPATH + "ex";
    operations.create(appPath, rec, 0);
  }
  
}
