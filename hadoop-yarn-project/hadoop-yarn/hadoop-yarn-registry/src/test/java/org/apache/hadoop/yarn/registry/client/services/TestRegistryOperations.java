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
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.types.AddressTypes;
import org.apache.hadoop.yarn.registry.client.types.CreateFlags;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class TestRegistryOperations extends AbstractZKRegistryTest {
  public static final String SC_HADOOP = "org-apache-hadoop";
  public static final String WEBHDFS = "webhdfs";
  public static final String USER = "drwho/";
  public static final String NAME = "hdfs";
  public static final String DATANODE = "datanode";
  public static final String API_WEBHDFS = "org_apache_hadoop_namenode_webhdfs";
  public static final String API_HDFS = "org_apache_hadoop_namenode_dfs";

  public static final String USERPATH =
      "/" + RegistryConstants.PATH_USERS + USER;
  public static final String PARENT_PATH = USERPATH + SC_HADOOP + "/";
  public static final String ENTRY_PATH = PARENT_PATH + NAME;
  public static final String NNIPC = "nnipc";

  private RegistryOperationsService yarnRegistry;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryOperations.class);
  private RegistryOperations operations;


  @Before
  public void setupClient() throws IOException {
    yarnRegistry = new RegistryOperationsService("yarnRegistry");
    yarnRegistry.init(createRegistryConfiguration());
    yarnRegistry.start();
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
    entry.putExternalEndpoint("web",
        RegistryTypeUtils.webEndpoint("UI", "web UI",
            new URI("http", hostname + ":80", "/")));
    entry.putExternalEndpoint(WEBHDFS,
        RegistryTypeUtils.restEndpoint(API_WEBHDFS,
            WEBHDFS,
            new URI("http", hostname + ":8020", "/")));

    Endpoint endpoint = RegistryTypeUtils.ipcEndpoint(API_HDFS,
        "hdfs", true);
    endpoint.addresses.add(RegistryTypeUtils.tuple(hostname, "8030"));
    entry.putInternalEndpoint(NNIPC,
        endpoint);
  }

  /**
   * General code to validate bits of a component/service entry built iwth
   * {@link #addSampleEndpoints(ServiceRecord, String)}
   * @param resolved instance to check
   */
  protected void validateEntry(ServiceRecord written, ServiceRecord resolved) {

    assertMatches(written, resolved);
    Map<String, Endpoint> externalEndpointMap = resolved.external;
    assertEquals(2, externalEndpointMap.size());
    Endpoint webhdfs = externalEndpointMap.get(WEBHDFS);
    assertMatches(webhdfs,
        AddressTypes.ADDRESS_URI,
        ProtocolTypes.PROTOCOL_REST,
        API_WEBHDFS);

    assertMatches(resolved.getInternalEndpoint(NNIPC),
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        ProtocolTypes.PROTOCOL_HADOOP_IPC_PROTOBUF,
        API_HDFS);

    List<List<String>> addresses = webhdfs.addresses;
    assertEquals(1, addresses.size());
    String addr = addresses.get(0).get(0);
    assertTrue(addr.contains("http"));
    assertTrue(addr.contains(":8020"));
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

  @Test
  public void testPutGetServiceEntry() throws Throwable {

    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    ServiceRecord resolved = operations.resolve(ENTRY_PATH);
    validateEntry(written, resolved);
    assertMatches(written, resolved);

    Endpoint endpoint = resolved.getExternalEndpoint(WEBHDFS);
    assertMatches(endpoint, AddressTypes.ADDRESS_URI,
        ProtocolTypes.PROTOCOL_REST,
        API_WEBHDFS);
    assertMatches(resolved.getInternalEndpoint(NNIPC),
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        ProtocolTypes.PROTOCOL_HADOOP_IPC_PROTOBUF,
        API_HDFS);
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
    operations.mkdir(ENTRY_PATH);
    RegistryPathStatus stat = operations.stat(ENTRY_PATH);
    fail("Got a status " + stat);
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
  public void testResolvePathThatHasNoEntry() throws Throwable {
    String empty = "/empty";
    operations.mkdir(empty);
    RegistryPathStatus stat = operations.stat(empty);
    assertEquals(0, stat.size);
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
}
