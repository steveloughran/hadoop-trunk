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

package org.apache.hadoop.yarn.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.services.RegistryOperationsService;
import org.apache.hadoop.yarn.registry.server.services.MicroZookeeperService;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.registry.RMRegistryService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Test registry support in the cluster
 */
public class TestMiniYARNClusterRegistry extends Assert {


  MiniYARNCluster cluster;

  @Rule
  public final Timeout testTimeout = new Timeout(10000);

  @Rule
  public TestName methodName = new TestName();
  private Configuration conf;

  @Before
  public void setup() throws IOException, InterruptedException {
    conf = new YarnConfiguration();

    cluster = new MiniYARNCluster(methodName.getMethodName(),
        1, 1, 1, 1, false, true);
    cluster.init(conf);
    cluster.start();
  }

  @Test
  public void testZKInstance() throws Exception {
    assertNotNull("zookeeper", cluster.getZookeeper());
  }

  @Test
  public void testZKConnectionAddress() throws Exception {
    MicroZookeeperService zookeeper = cluster.getZookeeper();
    InetSocketAddress address = zookeeper.getConnectionAddress();
    assertTrue("Unconfigured address", address.getPort() != 0);
  }

  @Test
  public void testZKConfigPatchPropagaton() throws Exception {
    MicroZookeeperService zookeeper = cluster.getZookeeper();
    String connectionString = zookeeper.getConnectionString();
    String confConnection = conf.get(RegistryConstants.KEY_REGISTRY_ZK_QUORUM);
    assertNotNull(confConnection);
    assertEquals(connectionString, confConnection);
  }

  @Test
  public void testRegistryCreated() throws Exception {
    assertTrue("registry not enabled",
        cluster.getConfig().getBoolean(RegistryConstants.KEY_REGISTRY_ENABLED,
            false));
    MicroZookeeperService zookeeper = cluster.getZookeeper();
    String connectionString = zookeeper.getConnectionString();
    String confConnection = conf.get(RegistryConstants.KEY_REGISTRY_ZK_QUORUM);
    ResourceManager rm = cluster.getResourceManager(0);
    RMRegistryService registry = rm.getRMContext().getRegistry();
    assertNotNull("null registry", registry);
  }

  @Test
  public void testPathsExist() throws Throwable {
    MicroZookeeperService zookeeper = cluster.getZookeeper();
    // service to directly hook in to the ZK server
    RegistryOperationsService operations =
        new RegistryOperationsService("operations", zookeeper);
    operations.init(new YarnConfiguration());
    operations.start();

    operations.stat("/");
    //verifies that the RM startup has created the system services path
    operations.stat(RegistryConstants.PATH_SYSTEM_SERVICES);

  }
}
