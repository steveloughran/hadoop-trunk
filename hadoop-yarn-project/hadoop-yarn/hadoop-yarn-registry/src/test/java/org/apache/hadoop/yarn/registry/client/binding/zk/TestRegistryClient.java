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
import org.apache.hadoop.yarn.registry.client.types.ServiceEntry;
import org.apache.hadoop.yarn.registry.client.types.TypeUtils;
import org.apache.hadoop.yarn.registry.server.services.RegistryZKService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class TestRegistryClient extends AbstractZKRegistryTest {

  public static final String SC_HADOOP = "org-apache-hadoop";
  public static final String WEBHDFS = "webhdfs";
  public static final String USER = "yarn";
  public static final String CLUSTERNAME = "namenode1";
  private RegistryZKService registry;
  private ZookeeperRegistryClient registryClient;

  @Before
  public void setupClient() {
    registryClient = new ZookeeperRegistryClient("registryClient");
    registryClient.init(createRegistryConfiguration());
    registryClient.start();
  }  
  
  @After
  public void  teardownClient() {
    ServiceOperations.stop(registryClient);
  }
  
  @Test
  public void testPutSE() throws Throwable {

    ServiceEntry se = new ServiceEntry();
    se.putExternal("web",
        TypeUtils.webEndpoint("UI","web UI", "http://localhost:80"));
    se.putExternal(WEBHDFS,
        TypeUtils.restEndpoint(("org_apache_hadoop_namenode_webhdfs"),
            WEBHDFS, "http://namenode:8020"));

    registryClient.putServiceEntry(USER, SC_HADOOP,
        CLUSTERNAME,
        se);

    List<String> serviceClasses = registryClient.listServiceClasses(USER);
    assertEquals(1, serviceClasses.size());
    assertEquals(SC_HADOOP, serviceClasses.get(0));
    assertTrue(registryClient.serviceClassExists(USER, SC_HADOOP));
    List<String> hadoopServices = registryClient.listServices(USER, SC_HADOOP);
    assertEquals(1, hadoopServices.size());

    assertEquals(CLUSTERNAME, hadoopServices.get(0));
  }
  
  
}
