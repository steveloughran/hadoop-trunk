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

package org.apache.hadoop.yarn.registry.client.binding.zk;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.registry.AbstractZKRegistryTest;
import org.apache.hadoop.yarn.registry.client.types.AddressTypes;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.registry.client.types.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.server.services.YarnRegistryService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Tests for the YARN registry
 */
public class TestYARNRegistryService extends AbstractZKRegistryTest {

  public static final String SC_HADOOP = "org-apache-hadoop";
  public static final String WEBHDFS = "webhdfs";
  public static final String USER = "yarn";
  public static final String CLUSTERNAME = "namenode1";
  public static final String DATANODE = "datanode";
  public static final String API_WEBHDFS = "org_apache_hadoop_namenode_webhdfs";
  public static final String API_HDFS = "org_apache_hadoop_namenode_dfs";
  private static final Logger LOG =
      LoggerFactory.getLogger(TestYARNRegistryService.class);
  
  private YarnRegistryService yarnRegistry;

  @Before
  public void setupClient() {
    yarnRegistry = new YarnRegistryService("yarnRegistry");
    yarnRegistry.init(createRegistryConfiguration());
    yarnRegistry.start();
  }

  @After
  public void teardownClient() {
    ServiceOperations.stop(yarnRegistry);
  }


  @Test
  public void testPutServiceEntry() throws Throwable {

    putExampleServiceEntry();

    List<String> serviceClasses = yarnRegistry.listServiceClasses(USER);
    assertEquals(1, serviceClasses.size());
    assertEquals(SC_HADOOP, serviceClasses.get(0));
    assertTrue(yarnRegistry.serviceClassExists(USER, SC_HADOOP));
    List<String> hadoopServices = yarnRegistry.listServices(USER, SC_HADOOP);
    assertEquals(1, hadoopServices.size());

    assertEquals(CLUSTERNAME, hadoopServices.get(0));

    assertTrue(yarnRegistry.serviceClassExists(USER, SC_HADOOP));
    assertTrue(yarnRegistry.serviceExists(USER, SC_HADOOP, CLUSTERNAME));
  }

  protected ServiceRecord putExampleServiceEntry() throws IOException {
    ServiceRecord se = new ServiceRecord();
    se.description = methodName.getMethodName();
    addSampleEndpoints(se, "namenode");

    yarnRegistry.putServiceEntry(USER, SC_HADOOP,
        CLUSTERNAME,
        se);
    return se;
  }


  @Test
  public void testDeleteServiceEntry() throws Throwable {
    putExampleServiceEntry();
    deleteExampleServiceEntry();
    List<String> hadoopServices = yarnRegistry.listServices(USER, SC_HADOOP);
    assertEquals(0, hadoopServices.size());
    assertTrue(yarnRegistry.serviceClassExists(USER, SC_HADOOP));
    assertFalse(yarnRegistry.serviceExists(USER, SC_HADOOP, CLUSTERNAME));
  }

  protected void deleteExampleServiceEntry() throws IOException {
    yarnRegistry.deleteServiceEntry(USER, SC_HADOOP, CLUSTERNAME);
  }

  @Test
  public void testPutComponentEntry() throws Throwable {
    putExampleServiceEntry();

    ServiceRecord component = new ServiceRecord();
    addSampleEndpoints(component, DATANODE);

    yarnRegistry.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        component,
        true);

    List<String> serviceClasses = yarnRegistry.listServiceClasses(USER);
    assertEquals(1, serviceClasses.size());
    assertEquals(SC_HADOOP, serviceClasses.get(0));
    assertTrue(yarnRegistry.serviceClassExists(USER, SC_HADOOP));
    List<String> components = yarnRegistry.listComponents(USER, SC_HADOOP,
        CLUSTERNAME);
    for (String name : components) {
      LOG.info(name);
    }
    assertEquals(1, components.size());

    assertEquals(DATANODE, components.get(0));
    assertTrue(yarnRegistry.componentExists(USER, SC_HADOOP, CLUSTERNAME, DATANODE));

    yarnRegistry.deleteComponent(USER, SC_HADOOP, CLUSTERNAME, DATANODE);
    assertEquals(0, yarnRegistry.listComponents(USER, SC_HADOOP,
        CLUSTERNAME).size());
    yarnRegistry.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        component,
        false);
    assertEquals(1, yarnRegistry.listComponents(USER, SC_HADOOP,
        CLUSTERNAME).size());
    deleteExampleServiceEntry();

    // verify that when the service is deleted, so go the components
    assertEquals(0, yarnRegistry.listComponents(USER, SC_HADOOP,
        CLUSTERNAME).size());
    assertFalse(yarnRegistry.componentExists(USER, SC_HADOOP, CLUSTERNAME, DATANODE));

  }

  @Test
  public void testLookforUndefinedServiceClass() throws Throwable {
    assertFalse(yarnRegistry.serviceClassExists("user2", "hadoop0"));
  }

  @Test
  public void testLookforUndefinedComponent() throws Throwable {
    assertFalse(
        yarnRegistry.componentExists("user2", "hadoop0", "cluster-3",
            "dn-0"));
  }

  @Test
  public void testLookforUndefinedService() throws Throwable {
    assertFalse(
        yarnRegistry.serviceExists("user2", "hadoop0", "cluster-3"));
  }


  @Test(expected = FileNotFoundException.class)
  public void testGetUndefinedComponent() throws Throwable {
    yarnRegistry.getComponent("user2", "hadoop0", "cluster-3", "dn-0");
  }

  @Test(expected = FileNotFoundException.class)
  public void testGetUndefinedService() throws Throwable {
    yarnRegistry.getServiceInstance("user2", "hadoop0", "cluster-3");
  }


  @Test(expected = FileNotFoundException.class)
  public void testPutComponentNoService() throws Throwable {
    deleteExampleServiceEntry();

    yarnRegistry.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        new ServiceRecord(),
        true);
  }

  @Test
  public void testOverwriteComponentEntry() throws Throwable {
    putExampleServiceEntry();
    ServiceRecord entry1 = new ServiceRecord();
    entry1.description = "entry1";
    addSampleEndpoints(entry1, "entry1");

    yarnRegistry.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        entry1,
        true);
    ServiceRecord entry2 = new ServiceRecord();
    entry2.description = "entry2";

    yarnRegistry.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        entry2,
        true);

    ServiceRecord entry3 = yarnRegistry.getComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE);
    assertEquals(entry2.description, entry3.description);
    assertEquals(0, entry3.internal.size());
    assertEquals(0, entry3.external.size());
  }

  @Test
  public void testOverwriteServiceEntry() throws Throwable {
    testPutServiceEntry();
    testPutServiceEntry();
  }

  @Test
  public void testDeleteMissingService() throws Throwable {
    yarnRegistry.deleteServiceEntry("user2", "hadoop0", "cluster-3");
  }

  @Test
  public void testDeleteUndefinedComponent() throws Throwable {
    yarnRegistry.deleteComponent("user2", "hadoop0", "cluster-3", "dn-0");
  }

  @Test
  public void testReadServiceEntry() throws Throwable {
    putExampleServiceEntry();
    ServiceRecord instance = yarnRegistry.getServiceInstance(USER,
        SC_HADOOP,
        CLUSTERNAME);
    assertEquals(
        methodName.getMethodName(),
        instance.description);

    validateEntry(instance);
  }

  @Test
  public void testReadComponent() throws Throwable {
    putExampleServiceEntry();
    ServiceRecord entry = new ServiceRecord();
    entry.description = methodName.getMethodName();
    addSampleEndpoints(entry, "datanode");
    yarnRegistry.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        entry,
        true);
    ServiceRecord instance = yarnRegistry.getComponent(USER,
        SC_HADOOP,
        CLUSTERNAME, DATANODE);
    assertEquals(
        methodName.getMethodName(),
        instance.description);

    validateEntry(instance);
  }

  @Test
  public void testServiceLiveness() throws Throwable {
    putExampleServiceEntry();
    putServiceLiveness(true, true);

    assertTrue("service is not live", isServiceLive());

    deleteServiceLiveness();
    assertFalse("service is live", isServiceLive());
    
    putServiceLiveness(false, true);
    assertTrue("service is not live", isServiceLive());

  }

  protected boolean isServiceLive() throws IOException {
    return yarnRegistry.isServiceLive(USER, SC_HADOOP, CLUSTERNAME);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testPutServiceLivenessOverwrite() throws Throwable {
    putExampleServiceEntry();
    putServiceLiveness(true, true);
    putServiceLiveness(true, false);
  }

  @Test()
  public void testPutServiceLivenessForcedOverwrite() throws Throwable {
    putExampleServiceEntry();
    putServiceLiveness(true, true);
    putServiceLiveness(true, true);
    putServiceLiveness(true, true);
    putServiceLiveness(true, true);
  }

  @Test
  public void testPutServiceLivenessNoService() throws Throwable {
    deleteExampleServiceEntry();

    try {
      putServiceLiveness(true, true);
      // if this is reached then parent directories are being created
      // when they should not
      fail("expected a failure, but the service liveness is " + isServiceLive());
    } catch (FileNotFoundException e) {

    }

  }

  @Test
  public void testServiceLivenessMissingService() throws Throwable {
    deleteExampleServiceEntry();
    assertFalse("service is live", isServiceLive());
  }

  @Test
  public void testDeleteServiceLivenessOfMissingServiceHarmless() throws
      Throwable {
    deleteExampleServiceEntry();
    deleteServiceLiveness();
    deleteServiceLiveness();
  }
  
  protected void putServiceLiveness(boolean ephemeral, boolean forceDelete) throws
      IOException {
    yarnRegistry.putServiceLiveness(USER,
        SC_HADOOP,
        CLUSTERNAME,
        ephemeral,
        forceDelete);
  }
  
  protected void deleteServiceLiveness() throws
      IOException {
    yarnRegistry.deleteServiceLiveness(USER,
        SC_HADOOP,
        CLUSTERNAME);
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

    entry.putInternalEndpoint("nnipc",
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

    Endpoint nnipc = instance.getInternalEndpoint("nnipc");
    assertNotNull(nnipc);
    assertEquals(ProtocolTypes.PROTOCOL_HADOOP_IPC_PROTOBUF,
        nnipc.protocolType);
    Endpoint web = instance.getExternalEndpoint("web");
    assertNotNull(web);
  }


}
