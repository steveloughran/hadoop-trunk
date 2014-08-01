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

package org.apache.hadoop.yarn.registry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.server.services.CuratorService;
import org.apache.hadoop.yarn.registry.server.services.InMemoryLocalhostZKService;
import org.apache.zookeeper.common.PathUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;

public class AbstractZKRegistryTest extends Assert {

  protected static InMemoryLocalhostZKService zookeeper;
  @Rule
  public final Timeout testTimeout = new Timeout(10000);
  @Rule
  public TestName methodName = new TestName();
  protected CuratorService registry;

  @BeforeClass
  public static void createZKServer() throws Exception {
    File zkDir = new File("target/zookeeper");
    FileUtils.deleteDirectory(zkDir);
    assertTrue(zkDir.mkdirs());
    zookeeper = new InMemoryLocalhostZKService("InMemoryZKService");
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(InMemoryLocalhostZKService.KEY_DATADIR, zkDir.getAbsolutePath());
    zookeeper.init(conf);
    zookeeper.start();
  }

  @AfterClass
  public static void destroyZKServer() throws IOException {
    zookeeper.close();
  }

  public static void assertValidZKPath(String path) {
    try {
      PathUtils.validatePath(path);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Path " + path + ": " + e, e);
    }
  }

  /**
   * give our thread a name
   */
  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Returns the connection string to use
   *
   * @return connection string
   */
  public String getConnectString() {
    return zookeeper.getConnectionString();
  }

  protected YarnConfiguration createRegistryConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(RegistryConstants.ZK_CONNECTION_TIMEOUT, 1000);
    conf.setInt(RegistryConstants.ZK_RETRY_INTERVAL, 500);
    conf.setInt(RegistryConstants.ZK_RETRY_TIMES, 10);
    conf.setInt(RegistryConstants.ZK_RETRY_CEILING, 10);
    conf.set(RegistryConstants.ZK_HOSTS, zookeeper.getConnectionString());
    return conf;
  }

  @Before
  public void startRegistry() {
    createRegistry();
  }

  @After
  public void stopRegistry() {
    ServiceOperations.stop(registry);
  }

  /**
   * Create an instance
   */
  protected void createRegistry() {
    registry = new CuratorService("registry");
    registry.init(createRegistryConfiguration());
    registry.start();
  }
}
