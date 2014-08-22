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

import org.apache.hadoop.yarn.registry.client.binding.JsonMarshal;
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.binding.ZKPathDumper;
import org.apache.hadoop.yarn.registry.client.types.AddressTypes;
import org.apache.hadoop.yarn.registry.client.types.CreateFlags;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.server.ResourceManagerRegistryService;
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

  private ResourceManagerRegistryService yarnRegistry;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryOperations.class);
  private RegistryOperations operations;
  private final JsonMarshal.ServiceRecordMarshal recordMarshal =
      new JsonMarshal.ServiceRecordMarshal();


  @Before
  public void setupClient() throws IOException {
    yarnRegistry = new ResourceManagerRegistryService("yarnRegistry");
    yarnRegistry.init(createRegistryConfiguration());
    yarnRegistry.start();
    yarnRegistry.createRegistryPaths();
    operations = yarnRegistry;
    operations.delete(ENTRY_PATH, true);

  }

  @After
  public void teardownClient() {
    ServiceOperations.stop(yarnRegistry);
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
  protected ServiceRecord putExampleServiceEntry(String path, int createFlags)
      throws IOException, URISyntaxException {
    List<ACL> acls = yarnRegistry.parseACLs("world:anyone:rwcda");
    ServiceRecord record = new ServiceRecord();
    record.id = "example-0001";
    record.description = "example service entry";
    record.description = methodName.getMethodName();
    record.registrationTime = System.currentTimeMillis();
    addSampleEndpoints(record, "namenode");
    yarnRegistry.zkMkPathRecursive(path, acls, false);
    operations.create(path, record, createFlags);
    return record;
  }


  public void assertMatches(Endpoint endpoint,
      String addressType,
      String protocolType, String api) {
    assertNotNull(endpoint);
    assertEquals(addressType, endpoint.addressType);
    assertEquals(protocolType, endpoint.protocolType);
    assertEquals(api, endpoint.api);
  }

  public void assertMatches(ServiceRecord written, ServiceRecord resolved) {
    assertEquals(written.id, resolved.id);
    assertEquals(written.registrationTime, resolved.registrationTime);
    assertEquals(written.description, resolved.description);
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


  @Test
  public void testPutGetServiceEntry() throws Throwable {

    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
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
    assertTrue(stat.hasRecord);
    assertTrue(stat.time > 0);
    assertEquals(ENTRY_PATH, stat.path);
  }

  @Test
  public void testTestLsParent() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    RegistryPathStatus stat = operations.stat(ENTRY_PATH);

    RegistryPathStatus[] statuses =
        operations.listDir(PARENT_PATH);
    assertEquals(1, statuses.length);
    assertEquals(stat, statuses[0]);

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
    } catch (PathNotFoundException e) {

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

  //  @Test(expected = PathNotFoundException.class)
  @Test
  public void testPutNoParent() throws Throwable {
    ServiceRecord record = new ServiceRecord();
    record.id = "testPutNoParent";
    String path = "/path/without/parent";
    try {
      operations.create(path, record, 0);
      // didn't get a failure
    } catch (PathNotFoundException e) {
      return;
    }

    // trouble
    RegistryPathStatus stat = operations.stat(path);
    fail("Got a status " + stat);

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
    assertEquals(0, stat.size);
  }


  @Test
  public void testResolvePathThatHasNoEntry() throws Throwable {
    String empty = "/empty2";
    operations.mkdir(empty, false);
    ServiceRecord record = operations.resolve(empty);
  }


  @Test
  public void testOverwrite() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    ServiceRecord resolved1 = operations.resolve(ENTRY_PATH);
    resolved1.description = "resolved1";
    try {
      operations.create(ENTRY_PATH, resolved1, 0);
      fail("overwrite succeeded when it should have failed");
    } catch (FileAlreadyExistsException e) {

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
        "tomcat-based web application");
    webapp.addExternalEndpoint(restEndpoint("www",
        new URI("http","//loadbalancer/", null)));

    ServiceRecord comp1 = new ServiceRecord(cid1, null);
    comp1.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//rack4server3:43572", null)));
    comp1.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack4server3", 43573));
    ServiceRecord comp2 = new ServiceRecord(cid2, null);
    comp2.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//rack1server28:35881",null)));
    comp2.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack1server28", 35882));

    operations.mkdir(USERPATH, false);
    operations.create(appPath, webapp, CreateFlags.OVERWRITE);
    String components = appPath + RegistryConstants.SUBPATH_COMPONENTS + "/";
    operations.mkdir(components, false);
    String dns1 = yarnIdToDnsId(cid1);
    operations.create(components + dns1, comp1, CreateFlags.EPHEMERAL);
    String dns2 = yarnIdToDnsId(cid2);
    operations.create(components + dns2, comp2, CreateFlags.EPHEMERAL );

    ZKPathDumper pathDumper = yarnRegistry.dumpPath();
    LOG.info(pathDumper.toString());

    log("tomcat", webapp);
    log(dns1, comp1);
    log(dns2, comp2);
  }

  public void log(String name, ServiceRecord record) throws
      IOException {
    LOG.info(" {} = \n{}\n", name, recordMarshal.toJson(record));
  }
}
