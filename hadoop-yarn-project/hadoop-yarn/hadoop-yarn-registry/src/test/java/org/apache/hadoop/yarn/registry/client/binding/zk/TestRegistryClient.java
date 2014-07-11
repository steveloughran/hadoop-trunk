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

import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.registry.AbstractZKRegistryTest;
import org.apache.hadoop.yarn.registry.client.types.ComponentEntry;
import org.apache.hadoop.yarn.registry.client.types.ServiceEntry;
import org.apache.hadoop.yarn.registry.client.types.TypeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.List;

public class TestRegistryClient extends AbstractZKRegistryTest {

  public static final String SC_HADOOP = "org-apache-hadoop";
  public static final String WEBHDFS = "webhdfs";
  public static final String USER = "yarn";
  public static final String CLUSTERNAME = "namenode1";
  public static final String DATANODE = "datanode";
  private ZookeeperRegistryClient client;

  @Before
  public void setupClient() {
    client = new ZookeeperRegistryClient("registryClient");
    client.init(createRegistryConfiguration());
    client.start();
  }

  @After
  public void teardownClient() {
    ServiceOperations.stop(client);
  }

  @Test
  public void testPutServiceEntry() throws Throwable {

    ServiceEntry se = new ServiceEntry();
    addSampleEndpoints(se, "namenode");

    client.putServiceEntry(USER, SC_HADOOP,
        CLUSTERNAME,
        se);

    List<String> serviceClasses = client.listServiceClasses(USER);
    assertEquals(1, serviceClasses.size());
    assertEquals(SC_HADOOP, serviceClasses.get(0));
    assertTrue(client.serviceClassExists(USER, SC_HADOOP));
    List<String> hadoopServices = client.listServices(USER, SC_HADOOP);
    assertEquals(1, hadoopServices.size());

    assertEquals(CLUSTERNAME, hadoopServices.get(0));
    
    assertTrue(client.serviceClassExists(USER, SC_HADOOP));
    assertTrue(client.serviceExists(USER, SC_HADOOP, CLUSTERNAME));
  }


  @Test
  public void testDeleteServiceEntry() throws Throwable {
    testPutServiceEntry();
    client.deleteServiceEntry(USER, SC_HADOOP, CLUSTERNAME);
    List<String> hadoopServices = client.listServices(USER, SC_HADOOP);
    assertEquals(0, hadoopServices.size());
    assertTrue(client.serviceClassExists(USER, SC_HADOOP));
    assertFalse(client.serviceExists(USER, SC_HADOOP, CLUSTERNAME));
  }

  @Test
  public void testPutComponentEntry() throws Throwable {
    testPutServiceEntry();

    ComponentEntry component = new ComponentEntry();
    addSampleEndpoints(component, DATANODE);

    client.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        component,
        true);

    List<String> serviceClasses = client.listServiceClasses(USER);
    assertEquals(1, serviceClasses.size());
    assertEquals(SC_HADOOP, serviceClasses.get(0));
    assertTrue(client.serviceClassExists(USER, SC_HADOOP));
    List<String> components = client.listComponents(USER, SC_HADOOP,
        CLUSTERNAME);
    assertEquals(1, components.size());

    assertEquals(DATANODE, components.get(0));
    assertTrue(client.componentExists(USER, SC_HADOOP, CLUSTERNAME, DATANODE));

    client.deleteComponent(USER, SC_HADOOP, CLUSTERNAME, DATANODE);
    assertEquals(0, client.listComponents(USER, SC_HADOOP,
        CLUSTERNAME).size());
    client.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        component,
        false);
    assertEquals(1, client.listComponents(USER, SC_HADOOP,
        CLUSTERNAME).size());
    client.deleteServiceEntry(USER, SC_HADOOP, CLUSTERNAME);
    
    // verify that when the service is deleted, so go the components
    assertEquals(0, client.listComponents(USER, SC_HADOOP,
        CLUSTERNAME).size());
    assertFalse(client.componentExists(USER, SC_HADOOP, CLUSTERNAME, DATANODE));

  }

  @Test
  public void testLookforUndefinedComponent() throws Throwable {
    assertFalse(client.componentExists("user2", "hadoop0", "cluster-3", "dn-0"));
  }
  
  @Test
  public void testLookforUndefinedService() throws Throwable {
    assertFalse(client.serviceExists("user2", "hadoop0", "cluster-3"));
  }
  
  @Test
  public void testLookforUndefinedServiceClass() throws Throwable {
    assertFalse(client.serviceClassExists("user2", "hadoop0"));
  }
  
  
  
  /**
   * Add some endpoints
   * @param entry entry
   */
  protected void addSampleEndpoints(ComponentEntry entry, String hostname) {
    entry.putExternal("web",
        TypeUtils.webEndpoint("UI", "web UI", "http://" + hostname + ":80"));
    entry.putExternal(WEBHDFS,
        TypeUtils.restEndpoint(("org_apache_hadoop_namenode_webhdfs"),
            WEBHDFS, "http://" + hostname + ":8020"));
    entry.putInternal("nnipc",
        TypeUtils.ipcEndpoint("org_apache_hadoop_namenode_dfs",
            "hdfs", true, hostname + "/8030"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testPutComponentNoService() throws Throwable {
    client.deleteServiceEntry(USER, SC_HADOOP, CLUSTERNAME);

    client.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        new ComponentEntry(),
        true);
  }

  @Test
  public void testOverwriteComponentEntry() throws Throwable {
    testPutServiceEntry();
    client.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        new ComponentEntry(),
        true);
    client.putComponent(USER,
        SC_HADOOP,
        CLUSTERNAME,
        DATANODE,
        new ComponentEntry(),
        true);
    
  }
  
  @Test
  public void testOverwriteServiceEntry() throws Throwable {
    testPutServiceEntry();
    testPutServiceEntry();
  }

  @Test
  public void testDeleteMissingService() throws Throwable {
    client.deleteServiceEntry("user2", "hadoop0", "cluster-3");
  }

  @Test
  public void testDeleteUndefinedComponent() throws Throwable {
        client.deleteComponent("user2", "hadoop0", "cluster-3", "dn-0");
  }


}
