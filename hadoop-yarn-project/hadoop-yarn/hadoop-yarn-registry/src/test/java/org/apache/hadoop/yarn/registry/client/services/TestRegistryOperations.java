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

import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.registry.AbstractZKRegistryTest;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.types.AddressTypes;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  public static final String HADOOP_SERVICES = USERPATH + SC_HADOOP + "/";
  public static final String ENTRY_PATH = HADOOP_SERVICES + NAME;
  public static final String NNIPC = "nnipc";

  private RegistryOperationsService yarnRegistry;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryOperations.class);
  private RegistryOperations operations;


  @Before
  public void setupClient() {
    yarnRegistry = new RegistryOperationsService("yarnRegistry");
    yarnRegistry.init(createRegistryConfiguration());
    yarnRegistry.start();
    operations = yarnRegistry;
  }

  @After
  public void teardownClient() {
    ServiceOperations.stop(yarnRegistry);
  }

  /**
   * Add some endpoints
   * @param entry entry
   */
  protected void addSampleEndpoints(ServiceRecord entry, String hostname) {
    entry.putExternalEndpoint("web",
        RegistryTypeUtils.webEndpoint("UI", "web UI",
            "http://" + hostname + ":80"));
    entry.putExternalEndpoint(WEBHDFS,
        RegistryTypeUtils.restEndpoint(API_WEBHDFS,
            WEBHDFS, "http://" + hostname + ":8020"));

    entry.putInternalEndpoint(NNIPC,
        RegistryTypeUtils.ipcEndpoint(API_HDFS,
            "hdfs", true, hostname + "/8030"));
  }

  /**
   * General code to validate bits of a component/service entry built iwth
   * {@link #addSampleEndpoints(ServiceRecord, String)}
   * @param instance instance to check
   */
  protected void validateEntry(ServiceRecord instance) {
    Map<String, Endpoint> externalEndpointMap = instance.external;
    assertEquals(2, externalEndpointMap.size());

    Endpoint webhdfs = externalEndpointMap.get(WEBHDFS);
    assertNotNull(webhdfs);
    assertEquals(API_WEBHDFS, webhdfs.api);
    assertEquals(AddressTypes.ADDRESS_URI, webhdfs.addressType);
    assertEquals(ProtocolTypes.PROTOCOL_REST, webhdfs.protocolType);
    List<String> addresses = webhdfs.addresses;
    assertEquals(1, addresses.size());
    String addr = addresses.get(0);
    assertTrue(addr.contains("http"));
    assertTrue(addr.contains(":8020"));

    Endpoint nnipc = instance.getInternalEndpoint(NNIPC);
    assertNotNull(nnipc);
    assertEquals(ProtocolTypes.PROTOCOL_HADOOP_IPC_PROTOBUF,
        nnipc.protocolType);
    Endpoint web = instance.getExternalEndpoint("web");
    assertNotNull(web);
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
      throws IOException {
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
  
  
  @Test
  public void testPutGetServiceEntry() throws Throwable {

    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    ServiceRecord resolved = operations.resolve(ENTRY_PATH);
    assertEquals(written.id, resolved.id);
    assertMatches(written, resolved);

    Endpoint endpoint = resolved.getExternalEndpoint(WEBHDFS);
    assertMatches(endpoint, AddressTypes.ADDRESS_URI,
        ProtocolTypes.PROTOCOL_REST,
        API_WEBHDFS);
    assertMatches(resolved.getInternalEndpoint(NNIPC),
        AddressTypes.ADDRESS_HOSTNAME_AND_PORT,
        ProtocolTypes.PROTOCOL_HADOOP_IPC_PROTOBUF
        , API_HDFS);
  }

  public void assertMatches(Endpoint endpoint,
      String addressType,
      String protocolType, String api) {
    assertEquals(addressType, endpoint.addressType );
    assertEquals(protocolType, endpoint.protocolType );
    assertEquals(api, endpoint.api );
  }

  public void assertMatches(ServiceRecord written, ServiceRecord resolved) {
    assertEquals(written.registrationTime, resolved.registrationTime);
    assertEquals(written.description, resolved.description);
  }

  @Test
  public void testDeleteServiceEntry() throws Throwable {
    putExampleServiceEntry(ENTRY_PATH, 0);
    operations.delete(ENTRY_PATH, false);
  }

  

}
